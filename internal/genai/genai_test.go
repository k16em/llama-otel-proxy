package genai

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func post(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	return r
}

func Test_PeekRequestでモデル名とstream指定が取得されている(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantOK         bool
		wantModel      string
		wantModelKnown bool
		wantStream     bool
	}{
		{name: "model and stream", body: `{"model":"m1","stream":true}`, wantOK: true, wantModel: "m1", wantModelKnown: true, wantStream: true},
		{name: "stream absent defaults false", body: `{"model":"m1"}`, wantOK: true, wantModel: "m1", wantModelKnown: true},
		{name: "model absent", body: `{"stream":true}`, wantOK: true, wantStream: true},
		{name: "invalid json", body: `{oops`, wantOK: false},
		{name: "empty", body: ``, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := post(tt.body)
			got, ok := PeekRequest(r, DefaultRequestLimit)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got.Model != tt.wantModel || got.Stream != tt.wantStream ||
				got.ModelKnown != tt.wantModelKnown {
				t.Errorf("got %+v", got)
			}
			if tt.wantOK && !got.StreamKnown {
				t.Error("a parsed body always knows the stream flag")
			}
			if string(got.Body) != tt.body {
				t.Errorf("captured body = %q, want %q", got.Body, tt.body)
			}

			forwarded, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read forwarded body: %v", err)
			}
			if string(forwarded) != tt.body {
				t.Errorf("forwarded body = %q, want %q", forwarded, tt.body)
			}
			if tt.wantOK && r.ContentLength != int64(len(tt.body)) {
				t.Errorf("ContentLength = %d, want %d", r.ContentLength, len(tt.body))
			}
		})
	}
}

func Test_PeekRequestで解析できない本文から値が推測されていない(t *testing.T) {
	r := post(`{oops`)
	got, ok := PeekRequest(r, DefaultRequestLimit)
	if ok {
		t.Fatal("want not ok")
	}
	if got.ModelKnown || got.StreamKnown {
		t.Errorf("got %+v, want nothing known", got)
	}
}

func Test_PeekRequestで上限を超えるContentLengthの本文も先頭が取得されている(t *testing.T) {
	body := `{"model":"m1","stream":true,"padding":"` + strings.Repeat("x", 5000) + `"}`
	r := post(body)
	got, ok := PeekRequest(r, 4096)
	if ok {
		t.Fatal("want the peek to be skipped")
	}
	if !got.BodyTruncated || len(got.Body) != 4096 {
		t.Errorf("captured body = %d bytes, truncated = %v", len(got.Body), got.BodyTruncated)
	}
	forwarded, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(forwarded) != body {
		t.Errorf("body must be untouched, got %q", forwarded)
	}
}

func Test_PeekRequestで上限を超えた本文からもモデル名とstream指定が取得されている(t *testing.T) {
	body := `{"model":"m1","stream":true,"messages":[{"role":"user","content":"` + strings.Repeat("x", 5000) + `"}]}`
	got, _ := PeekRequest(post(body), 4096)
	if !got.BodyTruncated {
		t.Fatal("want the body to be marked truncated")
	}
	if !got.ModelKnown || got.Model != "m1" {
		t.Errorf("model = %q (known = %v), want m1", got.Model, got.ModelKnown)
	}
	if !got.StreamKnown || !got.Stream {
		t.Errorf("stream = %v (known = %v), want true", got.Stream, got.StreamKnown)
	}
}

func Test_PeekRequestで上限より後ろにあるモデル名が推測されていない(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"` + strings.Repeat("x", 5000) + `"}],"model":"m1"}`
	got, _ := PeekRequest(post(body), 4096)
	if got.ModelKnown {
		t.Errorf("model = %q, want it to stay unknown past the limit", got.Model)
	}
	if got.StreamKnown {
		t.Error("stream must stay unknown when the body is cut before it")
	}
}

func Test_PeekRequestで上限を超えた本文が解析済みになっていない(t *testing.T) {
	body := `{"model":"m1","stream":true,"padding":"` + strings.Repeat("x", 200) + `"}`
	r := post(body)
	r.TransferEncoding = []string{"chunked"}
	r.ContentLength = -1
	got, ok := PeekRequest(r, 64)
	if ok {
		t.Fatal("want parsing to be abandoned over the limit")
	}
	if !got.BodyTruncated || len(got.Body) != 64 {
		t.Errorf("captured body = %d bytes, truncated = %v", len(got.Body), got.BodyTruncated)
	}
	forwarded, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read forwarded body: %v", err)
	}
	if string(forwarded) != body {
		t.Errorf("body was truncated: got %d bytes, want %d", len(forwarded), len(body))
	}
	if r.TransferEncoding == nil {
		t.Error("TransferEncoding must be left alone when the body is not buffered")
	}
}

func Test_PeekRequest後のリクエストからTransferEncodingが除去されている(t *testing.T) {
	r := post(`{"model":"m1"}`)
	r.TransferEncoding = []string{"chunked"}
	if _, ok := PeekRequest(r, DefaultRequestLimit); !ok {
		t.Fatal("want ok")
	}
	if r.TransferEncoding != nil {
		t.Errorf("TransferEncoding = %v, want nil", r.TransferEncoding)
	}
}

type errBody struct{ read bool }

func (b *errBody) Read(p []byte) (int, error) {
	if !b.read {
		b.read = true
		copy(p, "{\"mod")
		return 5, nil
	}
	return 0, errors.New("boom")
}
func (b *errBody) Close() error { return nil }

func Test_PeekRequestで本文の読み込みエラーが返されている(t *testing.T) {
	r := post("")
	r.Body = &errBody{}
	if _, ok := PeekRequest(r, DefaultRequestLimit); ok {
		t.Fatal("want failure")
	}

	got, err := io.ReadAll(r.Body)
	if err == nil {
		t.Error("want the original error to resurface")
	}
	if !bytes.Equal(got, []byte(`{"mod`)) {
		t.Errorf("got %q", got)
	}
}

func Test_PeekRequestで本文がないとき未解析になっている(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	if _, ok := PeekRequest(r, DefaultRequestLimit); ok {
		t.Error("want not ok for an empty body")
	}
}

func Test_ParseResponseで非ストリーミング応答のモデルと使用量が取得されている(t *testing.T) {
	body := `{"model":"m1","usage":{"prompt_tokens":12,"completion_tokens":34},
	          "timings":{"prompt_ms":100.5,"predicted_ms":2000.25,"predicted_per_second":17.5,"cache_n":8}}`
	got, ok := ParseResponse([]byte(body))
	if !ok {
		t.Fatal("want ok")
	}
	if got.Model != "m1" || got.InputTokens != 12 || got.OutputTokens != 34 ||
		!got.HasInputTokens || !got.HasOutputTokens {
		t.Fatalf("got %+v", got)
	}
	if got.Timings == nil {
		t.Fatal("want timings")
	}
	if got.Timings.PromptMS != 100.5 || got.Timings.PredictedMS != 2000.25 ||
		got.Timings.PredictedPerSecond != 17.5 || got.Timings.CacheN != 8 {
		t.Errorf("timings = %+v", got.Timings)
	}
}

func Test_ParseResponseでusage内のtimingsが取得されている(t *testing.T) {
	got, ok := ParseResponse([]byte(`{"usage":{"prompt_tokens":1,"timings":{"prompt_ms":5}}}`))
	if !ok || got.Timings == nil || got.Timings.PromptMS != 5 {
		t.Fatalf("got %+v", got)
	}
}

func Test_ParseResponseでtimingsがなくても使用量が取得されている(t *testing.T) {
	got, ok := ParseResponse([]byte(`{"model":"m1","usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	if !ok {
		t.Fatal("want ok")
	}
	if got.Timings != nil {
		t.Errorf("want no timings, got %+v", got.Timings)
	}
}

func Test_ParseResponseでusageがないときトークン数が設定されていない(t *testing.T) {
	got, ok := ParseResponse([]byte(`{"model":"m1","choices":[]}`))
	if !ok {
		t.Fatal("want ok")
	}
	if got.HasInputTokens || got.HasOutputTokens {
		t.Error("want no token counts")
	}
}

func Test_ParseResponseに不正なJSONを渡したとき未解析になっている(t *testing.T) {
	if _, ok := ParseResponse([]byte("not json")); ok {
		t.Error("want not ok")
	}
}

func Test_ParseResponseでResponsesAPIの非ストリーミング応答が取得されている(t *testing.T) {

	body := `{"created_at":1787309419,"id":"resp_x","model":"/models/qwen.gguf","object":"response",
	          "status":"completed","usage":{"input_tokens":41,"output_tokens":8,"total_tokens":49,
	          "input_tokens_details":{"cached_tokens":0}}}`
	got, ok := ParseResponse([]byte(body))
	if !ok {
		t.Fatal("want ok")
	}
	if got.Model != "/models/qwen.gguf" {
		t.Errorf("model = %q", got.Model)
	}
	if !got.HasInputTokens || got.InputTokens != 41 {
		t.Errorf("input_tokens = %d (has=%v)", got.InputTokens, got.HasInputTokens)
	}
	if !got.HasOutputTokens || got.OutputTokens != 8 {
		t.Errorf("output_tokens = %d (has=%v)", got.OutputTokens, got.HasOutputTokens)
	}
}

func Test_ParseResponseでResponsesAPIのストリーミング応答が取得されている(t *testing.T) {
	body := `{"type":"response.completed","response":{"id":"resp_y","object":"response",
	          "status":"completed","model":"/models/qwen.gguf",
	          "usage":{"input_tokens":41,"output_tokens":8,"total_tokens":49}},
	          "timings":{"cache_n":37,"prompt_ms":205.725,"predicted_ms":900.5}}`
	got, ok := ParseResponse([]byte(body))
	if !ok {
		t.Fatal("want ok")
	}
	if got.Model != "/models/qwen.gguf" {
		t.Errorf("model = %q", got.Model)
	}
	if got.InputTokens != 41 || got.OutputTokens != 8 {
		t.Errorf("usage = %d/%d", got.InputTokens, got.OutputTokens)
	}
	if got.Timings == nil || got.Timings.PromptMS != 205.725 || got.Timings.CacheN != 37 {
		t.Errorf("timings = %+v", got.Timings)
	}
}

func Test_ParseResponseでResponsesAPIの失敗と未完了が終端として取得されている(t *testing.T) {
	failed, ok := ParseResponse([]byte(`{"type":"response.failed","response":{"error":{"code":"server_error","message":"boom"}}}`))
	if !ok || !failed.Terminal || !failed.Failed || failed.Message != "boom" {
		t.Errorf("failed = %+v (ok=%v)", failed, ok)
	}
	incomplete, ok := ParseResponse([]byte(`{"type":"response.incomplete","response":{"status":"incomplete"}}`))
	if !ok || !incomplete.Terminal || !incomplete.Incomplete || incomplete.Failed {
		t.Errorf("incomplete = %+v (ok=%v)", incomplete, ok)
	}
	streamError, ok := ParseResponse([]byte(`{"type":"error","code":"server_error","message":"stream broke"}`))
	if !ok || !streamError.Terminal || !streamError.Failed || streamError.Message != "stream broke" {
		t.Errorf("stream error = %+v (ok=%v)", streamError, ok)
	}
	nestedError, ok := ParseResponse([]byte(`{"type":"error","error":{"message":"quota exceeded"}}`))
	if !ok || nestedError.Message != "quota exceeded" {
		t.Errorf("nested stream error = %+v (ok=%v)", nestedError, ok)
	}
}

func Test_ParseResponseで一部だけのusageが欠損を補完せず取得されている(t *testing.T) {
	got, ok := ParseResponse([]byte(`{"model":"e5","usage":{"prompt_tokens":19}}`))
	if !ok {
		t.Fatal("want ok")
	}
	if !got.HasInputTokens || got.InputTokens != 19 {
		t.Errorf("input_tokens = %d (has=%v)", got.InputTokens, got.HasInputTokens)
	}
	if got.HasOutputTokens {
		t.Error("output_tokens must not be fabricated when the response omits it")
	}
}

func Test_ParseResponseで本文内のエラーが失敗として取得されている(t *testing.T) {
	tests := map[string]string{
		"object": `{"error":{"code":500,"message":"context shift is disabled","type":"server_error"}}`,
		"string": `{"error":"something went wrong"}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := ParseResponse([]byte(body))
			if !ok {
				t.Fatal("want ok")
			}
			if !got.Failed {
				t.Error("want Failed")
			}
			if got.Message == "" {
				t.Error("want a message")
			}
			if !got.Terminal {
				t.Error("an error payload ends the stream")
			}
		})
	}
}

func Test_ParseResponseでnullのerrorが失敗として扱われていない(t *testing.T) {
	got, _ := ParseResponse([]byte(`{"model":"m","error":null}`))
	if got.Failed {
		t.Error("a null error field is not an error")
	}
}

func Test_ParseResponseでエラーメッセージの長さが制限されている(t *testing.T) {
	huge := strings.Repeat("x", 5000)
	got, _ := ParseResponse([]byte(`{"error":{"message":"` + huge + `"}}`))
	if len(got.Message) > 300 {
		t.Errorf("message is %d chars; upstream text must be bounded before it becomes a span status", len(got.Message))
	}
}

func Test_ParseResponseでnative形式のstopが終端として取得されている(t *testing.T) {
	got, ok := ParseResponse([]byte(`{"content":"","stop":true,"model":"/models/q.gguf"}`))
	if !ok || !got.Terminal {
		t.Errorf("got %+v, want Terminal", got)
	}
	mid, _ := ParseResponse([]byte(`{"content":"2","stop":false}`))
	if mid.Terminal {
		t.Error("stop:false is not terminal")
	}
}

func Test_FirstTokenInで生成トークンを含むpayloadだけが検出されている(t *testing.T) {
	tokens := map[string]string{
		"chat delta":            `{"choices":[{"delta":{"content":"hi"}}]}`,
		"chat reasoning delta":  `{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`,
		"chat reasoning alias":  `{"choices":[{"delta":{"reasoning":"thinking"}}]}`,
		"legacy completion":     `{"choices":[{"text":"hi"}]}`,
		"native completion":     `{"content":"hi","stop":false}`,
		"native reasoning":      `{"reasoning":"thinking","stop":false}`,
		"responses text delta":  `{"type":"response.output_text.delta","delta":"hi"}`,
		"responses custom tool": `{"type":"response.output_item.added","item":{"type":"custom_tool_call","name":"shell"}}`,
		"responses built-in":    `{"type":"response.output_item.added","item":{"type":"web_search_call"}}`,
		"responses done-only":   `{"type":"response.output_item.done","item":{"type":"function_call","name":"get_weather"}}`,
	}
	for name, payload := range tokens {
		t.Run(name, func(t *testing.T) {
			if !FirstTokenIn([]byte(payload)) {
				t.Error("want a token")
			}
		})
	}

	notTokens := map[string]string{
		"role only chunk":                      `{"choices":[{"delta":{"role":"assistant","content":""}}]}`,
		"empty delta":                          `{"choices":[{"delta":{}}]}`,
		"responses created":                    `{"type":"response.created","response":{"status":"in_progress"}}`,
		"responses tool lifecycle":             `{"type":"response.web_search_call.completed","item_id":"ws_1","output_index":0}`,
		"responses message item":               `{"type":"response.output_item.added","item":{"type":"message","name":"assistant"}}`,
		"responses function call without name": `{"type":"response.output_item.added","item":{"type":"function_call","name":""}}`,
		"responses function call output":       `{"type":"response.output_item.added","item":{"type":"function_call_output","call_id":"call_1","output":"done"}}`,
		"usage only chunk":                     `{"choices":[],"usage":{"prompt_tokens":6}}`,
		"native empty":                         `{"content":"","stop":false}`,
		"not json":                             `keep-alive`,
	}
	for name, payload := range notTokens {
		t.Run(name, func(t *testing.T) {
			if FirstTokenIn([]byte(payload)) {
				t.Errorf("%s must not count as the first token", name)
			}
		})
	}
}

func Test_FirstTokenInでResponsesAPIのfunction_callが検出されている(t *testing.T) {
	payload, err := os.ReadFile("testdata/responses_output_item_added_function_call.json")
	if err != nil {
		t.Fatal(err)
	}
	if !FirstTokenIn(payload) {
		t.Error("want a token")
	}
}

// The message becomes a span attribute and a status description, both of which
// are protobuf strings. Cutting a multi-byte rune in half produces invalid
// UTF-8, which makes the whole OTLP batch — up to 512 spans — fail to marshal.
func Test_Truncate後の文字列が有効なUTF8になっている(t *testing.T) {
	inputs := []string{
		strings.Repeat("a", 255) + "€",
		strings.Repeat("あ", 400),
		strings.Repeat("a", 254) + "🎉" + strings.Repeat("b", 100),
		"short",
		string([]byte{0xff, 0xfe, 'a'}),
	}
	for _, in := range inputs {
		got := Truncate(in)
		if !utf8.ValidString(got) {
			t.Errorf("Truncate(%d bytes) produced invalid UTF-8: %q", len(in), got)
		}
		if len(got) > 256+len("…") {
			t.Errorf("Truncate returned %d bytes, want at most %d", len(got), 256+len("…"))
		}
	}
}

func Test_errorMessageの結果が有効なUTF8になっている(t *testing.T) {
	body := `{"error":{"message":"` + strings.Repeat("a", 255) + `€"}}`
	got, ok := ParseResponse([]byte(body))
	if !ok || !got.Failed {
		t.Fatalf("got %+v", got)
	}
	if !utf8.ValidString(got.Message) {
		t.Errorf("message is not valid UTF-8: %q", got.Message)
	}
}

// A single field of an unexpected shape must not make the whole payload
// unreadable: the object-valued top-level delta used by other chat APIs left
// choices[] unexamined and hid a real first token.
func Test_FirstTokenInでobject形式のdeltaがあっても他のトークンが検出されている(t *testing.T) {
	payload := `{"delta":{"type":"text_delta","text":"hi"},` +
		`"choices":[{"delta":{"content":"hi"}}]}`
	if !FirstTokenIn([]byte(payload)) {
		t.Error("want a token from choices even when delta is an object")
	}
	if FirstTokenIn([]byte(`{"delta":{"type":"text_delta","text":"hi"}}`)) {
		t.Error("an object-valued delta on its own is not a recognized token")
	}
	if FirstTokenIn([]byte(`{"content":{"parts":["hi"]}}`)) {
		t.Error("an object-valued content is not a recognized token")
	}
}

func Test_FirstTokenInでtool_callの内容だけがトークンとして検出されている(t *testing.T) {
	tokens := map[string]string{
		"tool call name":      `{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"get_weather","arguments":""}}]}}]}`,
		"tool call arguments": `{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":1}"}}]}}]}`,
		"legacy function":     `{"choices":[{"delta":{"function_call":{"name":"f"}}}]}`,
		"refusal":             `{"choices":[{"delta":{"refusal":"I cannot"}}]}`,
	}
	for name, payload := range tokens {
		t.Run(name, func(t *testing.T) {
			if !FirstTokenIn([]byte(payload)) {
				t.Error("want a token")
			}
		})
	}
	if FirstTokenIn([]byte(`{"choices":[{"delta":{"tool_calls":[]}}]}`)) {
		t.Error("an empty tool_calls array is not a token")
	}
}

func Test_StreamAccumulatorでChatの複数choiceとtool_callが完成している(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	started := time.Unix(10, 0)
	finished := started.Add(time.Second)
	terminal := finished.Add(time.Second)

	first := `{"choices":[` +
		`{"index":0,"delta":{"reasoning_content":"考え","content":"返","tool_calls":[{"index":1,"id":"call_0","type":"function","function":{"name":"look","arguments":"{\"city\":"}}]}},` +
		`{"index":1,"delta":{"reasoning_content":"別","content":"候補"}}]}`
	if got := accumulator.Observe([]byte(first), false, started); len(got) != 0 {
		t.Fatalf("first outputs = %+v, want none", got)
	}

	second := `{"choices":[{"index":0,"delta":{"reasoning_content":"中","content":"答","tool_calls":[{"index":1,"function":{"name":"up","arguments":"\"東京\"}"}}]},"finish_reason":"tool_calls"}]}`
	choiceZero := accumulator.Observe([]byte(second), false, finished)
	if len(choiceZero) != 3 {
		t.Fatalf("choice zero outputs = %+v, want 3", choiceZero)
	}

	reasoning := matchingStreamOutput(t, choiceZero, func(output StreamOutput) bool {
		return output.Kind == StreamOutputReasoning && output.HasChoiceIndex && output.ChoiceIndex == 0
	})
	if reasoning.Reasoning != "考え中" || reasoning.FinishReason != "tool_calls" || !reasoning.Complete {
		t.Errorf("reasoning = %+v", reasoning)
	}
	if !reasoning.Start.Equal(started) || !reasoning.End.Equal(finished) {
		t.Errorf("reasoning time = %v..%v", reasoning.Start, reasoning.End)
	}

	response := matchingStreamOutput(t, choiceZero, func(output StreamOutput) bool {
		return output.Kind == StreamOutputResponse && output.HasChoiceIndex && output.ChoiceIndex == 0
	})
	if response.Content != "返答" || response.FinishReason != "tool_calls" || !response.Complete {
		t.Errorf("response = %+v", response)
	}

	tool := matchingStreamOutput(t, choiceZero, func(output StreamOutput) bool {
		return output.Kind == StreamOutputToolCall && output.HasChoiceIndex && output.ChoiceIndex == 0
	})
	if !tool.HasToolCallIndex || tool.ToolCallIndex != 1 || tool.ToolCallID != "call_0" ||
		tool.ToolType != "function" || tool.Name != "lookup" || tool.Arguments != `{"city":"東京"}` ||
		tool.FinishReason != "tool_calls" || !tool.Complete {
		t.Errorf("tool = %+v", tool)
	}

	choiceOne := accumulator.Observe(nil, true, terminal)
	if len(choiceOne) != 2 {
		t.Fatalf("choice one outputs = %+v, want 2", choiceOne)
	}
	secondChoiceReasoning := matchingStreamOutput(t, choiceOne, func(output StreamOutput) bool {
		return output.Kind == StreamOutputReasoning && output.HasChoiceIndex && output.ChoiceIndex == 1
	})
	secondChoiceResponse := matchingStreamOutput(t, choiceOne, func(output StreamOutput) bool {
		return output.Kind == StreamOutputResponse && output.HasChoiceIndex && output.ChoiceIndex == 1
	})
	if secondChoiceReasoning.Reasoning != "別" || !secondChoiceReasoning.Complete || !secondChoiceReasoning.End.Equal(terminal) {
		t.Errorf("second choice reasoning = %+v", secondChoiceReasoning)
	}
	if secondChoiceResponse.Content != "候補" || !secondChoiceResponse.Complete || !secondChoiceResponse.End.Equal(terminal) {
		t.Errorf("second choice response = %+v", secondChoiceResponse)
	}
}

func Test_StreamAccumulatorで同じChatの複数tool_callがindexごとに完成している(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	started := time.Unix(15, 0)
	for _, payload := range []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_0","type":"function","function":{"name":"first_","arguments":"{\"a\":"}},{"index":1,"id":"call_1","type":"function","function":{"name":"second_","arguments":"{\"b\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"name":"tool","arguments":"2}"}},{"index":0,"function":{"name":"tool","arguments":"1}"}}]}}]}`,
	} {
		if outputs := accumulator.Observe([]byte(payload), false, started); len(outputs) != 0 {
			t.Fatalf("outputs before finish = %+v", outputs)
		}
		started = started.Add(time.Second)
	}

	outputs := accumulator.Observe([]byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`), false, started)
	if len(outputs) != 2 {
		t.Fatalf("tool outputs = %+v, want 2", outputs)
	}
	first := matchingStreamOutput(t, outputs, func(output StreamOutput) bool {
		return output.Kind == StreamOutputToolCall && output.HasToolCallIndex && output.ToolCallIndex == 0
	})
	second := matchingStreamOutput(t, outputs, func(output StreamOutput) bool {
		return output.Kind == StreamOutputToolCall && output.HasToolCallIndex && output.ToolCallIndex == 1
	})
	if first.ToolCallID != "call_0" || first.Name != "first_tool" || first.Arguments != `{"a":1}` || !first.Complete {
		t.Errorf("first tool = %+v", first)
	}
	if second.ToolCallID != "call_1" || second.Name != "second_tool" || second.Arguments != `{"b":2}` || !second.Complete {
		t.Errorf("second tool = %+v", second)
	}
}

func Test_StreamAccumulatorでResponsesAPIの各出力がdoneで完成している(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	at := time.Unix(20, 0)

	assertNoOutput := func(payload string) {
		t.Helper()
		if got := accumulator.Observe([]byte(payload), false, at); len(got) != 0 {
			t.Fatalf("outputs = %+v, want none", got)
		}
		at = at.Add(time.Second)
	}

	assertNoOutput(`{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":1,"delta":"考え"}`)
	assertNoOutput(`{"type":"response.reasoning_summary_text.done","item_id":"rs_1","output_index":0,"summary_index":1,"text":"考えた"}`)
	reasoningOutputs := accumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":""},{"type":"summary_text","text":"考えた"}]}}`), false, at)
	at = at.Add(time.Second)
	if len(reasoningOutputs) != 1 {
		t.Fatalf("reasoning outputs = %+v, want 1", reasoningOutputs)
	}
	reasoning := reasoningOutputs[0]
	if reasoning.Kind != StreamOutputReasoning || reasoning.Reasoning != "考えた" ||
		!reasoning.HasOutputIndex || reasoning.OutputIndex != 0 || !reasoning.HasContentIndex ||
		reasoning.ContentIndex != 1 || reasoning.ItemID != "rs_1" || !reasoning.Complete {
		t.Errorf("reasoning = %+v", reasoning)
	}

	assertNoOutput(`{"type":"response.output_text.delta","item_id":"msg_1","output_index":1,"content_index":2,"delta":"Hello "}`)
	assertNoOutput(`{"type":"response.output_text.done","item_id":"msg_1","output_index":1,"content_index":2,"text":"Hello world"}`)
	responseOutputs := accumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","status":"completed","content":[{"type":"output_text","text":""},{"type":"output_text","text":""},{"type":"output_text","text":"Hello world"}]}}`), false, at)
	at = at.Add(time.Second)
	if len(responseOutputs) != 1 {
		t.Fatalf("response outputs = %+v, want 1", responseOutputs)
	}
	response := responseOutputs[0]
	if response.Kind != StreamOutputResponse || response.Content != "Hello world" ||
		!response.HasOutputIndex || response.OutputIndex != 1 || !response.HasContentIndex ||
		response.ContentIndex != 2 || response.ItemID != "msg_1" || !response.Complete {
		t.Errorf("response = %+v", response)
	}

	assertNoOutput(`{"type":"response.output_item.added","output_index":2,"item":{"id":"fc_1","call_id":"call_1","type":"function_call","name":"lookup","arguments":""}}`)
	assertNoOutput(`{"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"{\"city\":"}`)
	assertNoOutput(`{"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"\"東京\"}"}`)
	assertNoOutput(`{"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":2,"arguments":"{\"city\":\"東京\"}"}`)
	toolOutputs := accumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":2,"item":{"id":"fc_1","call_id":"call_1","type":"function_call","status":"completed","name":"lookup","arguments":"{\"city\":\"東京\"}"}}`), false, at)
	if len(toolOutputs) != 1 {
		t.Fatalf("tool outputs = %+v, want 1", toolOutputs)
	}
	tool := toolOutputs[0]
	if tool.Kind != StreamOutputToolCall || tool.Arguments != `{"city":"東京"}` ||
		tool.ToolType != "function_call" || tool.Name != "lookup" || tool.ToolCallID != "call_1" ||
		tool.ItemID != "fc_1" || !tool.HasOutputIndex || tool.OutputIndex != 2 || !tool.Complete {
		t.Errorf("tool = %+v", tool)
	}

	assertNoOutput(`{"type":"response.output_item.added","output_index":3,"item":{"id":"ws_1","type":"web_search_call"}}`)
	assertNoOutput(`{"type":"response.web_search_call.completed","item_id":"ws_1","output_index":3}`)
	builtInOutputs := accumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":3,"item":{"id":"ws_1","type":"web_search_call","action":{"type":"search","query":"weather"}}}`), false, at)
	if len(builtInOutputs) != 1 {
		t.Fatalf("built-in outputs = %+v, want 1", builtInOutputs)
	}
	builtIn := builtInOutputs[0]
	if builtIn.Kind != StreamOutputToolCall || builtIn.ToolType != "web_search_call" ||
		builtIn.ItemID != "ws_1" || builtIn.Arguments != `{"type":"search","query":"weather"}` || !builtIn.Complete {
		t.Errorf("built-in = %+v", builtIn)
	}

	lifecycleOnlyAccumulator := NewStreamAccumulator(1024)
	lifecycleOnlyAccumulator.Observe([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"ws_2","type":"web_search_call","status":"in_progress"}}`), false, at)
	lifecycleOnlyAccumulator.Observe([]byte(`{"type":"response.web_search_call.completed","item_id":"ws_2","output_index":0}`), false, at.Add(time.Second))
	lifecycleOnly := lifecycleOnlyAccumulator.Finish(at.Add(2 * time.Second))
	if len(lifecycleOnly) != 1 || lifecycleOnly[0].Kind != StreamOutputToolCall || lifecycleOnly[0].Complete || lifecycleOnly[0].Failed {
		t.Errorf("lifecycle-only output = %+v", lifecycleOnly)
	}
}

func Test_StreamAccumulatorでResponsesAPIの複数partがoutput単位に集約されている(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	at := time.Unix(25, 0)
	for _, payload := range []string{
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"A"}`,
		`{"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":0,"text":"A"}`,
		`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":1,"delta":"B"}`,
		`{"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":1,"text":"B"}`,
	} {
		if outputs := accumulator.Observe([]byte(payload), false, at); len(outputs) != 0 {
			t.Fatalf("outputs before item done = %+v", outputs)
		}
		at = at.Add(time.Second)
	}
	outputs := accumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","status":"completed","content":[{"type":"output_text","text":"A"},{"type":"output_text","text":"B"}]}}`), false, at)
	if len(outputs) != 1 {
		t.Fatalf("outputs = %+v, want 1", outputs)
	}
	if output := outputs[0]; output.Kind != StreamOutputResponse || output.Content != "AB" ||
		output.HasContentIndex || !output.Complete || output.ItemID != "msg_1" {
		t.Errorf("output = %+v", output)
	}
}

func Test_StreamAccumulatorでResponsesAPIのobject引数とpatch操作がtoolに保持されている(t *testing.T) {
	at := time.Unix(25, 0)
	toolSearchAccumulator := NewStreamAccumulator(1024)
	toolSearch := toolSearchAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"search_1","type":"tool_search_call","status":"completed","arguments":{"query":"weather","limit":2}}}`), false, at)
	if len(toolSearch) != 1 || toolSearch[0].Kind != StreamOutputToolCall ||
		toolSearch[0].Arguments != `{"query":"weather","limit":2}` || !toolSearch[0].Complete || toolSearchAccumulator.Limited() {
		t.Errorf("tool search outputs = %+v, limited=%v", toolSearch, toolSearchAccumulator.Limited())
	}

	applyPatchAccumulator := NewStreamAccumulator(1024)
	applyPatch := applyPatchAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":1,"item":{"id":"patch_1","type":"apply_patch_call","status":"completed","operation":{"type":"create_file","path":"new.txt","diff":"+hello"}}}`), false, at)
	if len(applyPatch) != 1 || applyPatch[0].Kind != StreamOutputToolCall ||
		applyPatch[0].Arguments != `{"type":"create_file","path":"new.txt","diff":"+hello"}` || !applyPatch[0].Complete || applyPatchAccumulator.Limited() {
		t.Errorf("apply patch outputs = %+v, limited=%v", applyPatch, applyPatchAccumulator.Limited())
	}

	partialAccumulator := NewStreamAccumulator(1024)
	partialAccumulator.Observe([]byte(`{"type":"response.function_call_arguments.delta","item_id":"partial_1","output_index":2,"delta":"{\"q\":"}`), false, at)
	partial := partialAccumulator.Finish(at.Add(time.Second))
	if len(partial) != 1 || partial[0].ItemID != "partial_1" || partial[0].ToolCallID != "partial_1" || partial[0].Complete {
		t.Errorf("partial outputs = %+v", partial)
	}

	computerAccumulator := NewStreamAccumulator(1024)
	computer := computerAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":2,"item":{"id":"computer_1","call_id":"call_2","type":"computer_call","status":"completed","actions":[{"type":"click","x":1,"y":2},{"type":"type","text":"hi"}]}}`), false, at)
	if len(computer) != 1 || computer[0].Arguments != `[{"type":"click","x":1,"y":2},{"type":"type","text":"hi"}]` ||
		computer[0].ToolCallID != "call_2" || !computer[0].Complete {
		t.Errorf("computer outputs = %+v", computer)
	}

	programAccumulator := NewStreamAccumulator(1024)
	program := programAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":3,"item":{"id":"program_1","call_id":"call_3","type":"program","code":"await tools.lookup()","fingerprint":"fp"}}`), false, at)
	if len(program) != 1 || program[0].ToolType != "program" || program[0].Arguments != "await tools.lookup()" ||
		program[0].ToolCallID != "call_3" || !program[0].Complete {
		t.Errorf("program outputs = %+v", program)
	}

	shellAccumulator := NewStreamAccumulator(1024)
	for _, payload := range []string{
		`{"type":"response.shell_call_command.added","output_index":4,"command_index":0,"command":"ec"}`,
		`{"type":"response.shell_call_command.added","output_index":4,"command_index":1,"command":"pw"}`,
		`{"type":"response.shell_call_command.delta","output_index":4,"command_index":1,"delta":"d"}`,
		`{"type":"response.shell_call_command.delta","output_index":4,"command_index":0,"delta":"ho hi"}`,
		`{"type":"response.shell_call_command.done","output_index":4,"command_index":1,"command":"pwd"}`,
		`{"type":"response.shell_call_command.done","output_index":4,"command_index":0,"command":"echo hi"}`,
	} {
		if outputs := shellAccumulator.Observe([]byte(payload), false, at); len(outputs) != 0 {
			t.Fatalf("shell outputs before finish = %+v", outputs)
		}
	}
	shell := shellAccumulator.Finish(at.Add(time.Second))
	if len(shell) != 1 || shell[0].ToolType != "shell_call" || shell[0].Arguments != `{"commands":["echo hi","pwd"]}` || shell[0].Complete {
		t.Errorf("shell outputs = %+v", shell)
	}
}

func Test_StreamAccumulatorでResponsesAPIの終端snapshotとerrorが部分出力を保持している(t *testing.T) {
	at := time.Unix(26, 0)
	failedAccumulator := NewStreamAccumulator(1024)
	failed := failedAccumulator.Observe([]byte(`{"type":"response.failed","response":{"error":{"code":"server_error","message":"boom"},"output":[{"id":"msg_1","type":"message","status":"incomplete","content":[{"type":"output_text","text":"partial"}]}]}}`), false, at)
	if len(failed) != 1 || failed[0].Kind != StreamOutputResponse || failed[0].Content != "partial" ||
		!failed[0].Failed || failed[0].Complete || failed[0].ErrorMessage != "boom" {
		t.Errorf("failed outputs = %+v", failed)
	}

	incompleteAccumulator := NewStreamAccumulator(1024)
	incomplete := incompleteAccumulator.Observe([]byte(`{"type":"response.incomplete","response":{"output":[{"id":"fc_1","call_id":"call_1","type":"function_call","status":"incomplete","name":"lookup","arguments":"{\"q\":"}]}}`), false, at)
	if len(incomplete) != 1 || incomplete[0].Kind != StreamOutputToolCall || incomplete[0].Arguments != `{"q":` ||
		incomplete[0].Complete || incomplete[0].Failed || incomplete[0].ToolCallID != "call_1" {
		t.Errorf("incomplete outputs = %+v", incomplete)
	}

	errorAccumulator := NewStreamAccumulator(1024)
	errorAccumulator.Observe([]byte(`{"choices":[{"delta":{"content":"partial"}}]}`), false, at)
	streamError := errorAccumulator.Observe([]byte(`{"type":"error","code":"server_error","message":"stream broke"}`), false, at.Add(time.Second))
	if len(streamError) != 1 || !streamError[0].Failed || streamError[0].Content != "partial" || streamError[0].ErrorMessage != "stream broke" {
		t.Errorf("stream error outputs = %+v", streamError)
	}

	progressAccumulator := NewStreamAccumulator(1024)
	progress := progressAccumulator.Observe([]byte(`{"type":"response.in_progress","response":{"output":[{"id":"msg_1","type":"message","status":"in_progress","content":[{"type":"output_text","text":"old"}]}]}}`), false, at)
	if len(progress) != 0 {
		t.Fatalf("in-progress outputs = %+v", progress)
	}
	completed := progressAccumulator.Observe([]byte(`{"type":"response.completed","response":{"output":[{"id":"msg_1","type":"message","status":"completed","content":[{"type":"output_text","text":"final"}]}]}}`), false, at.Add(time.Second))
	if len(completed) != 1 || completed[0].Content != "final" || !completed[0].Complete || completed[0].ProtocolIncomplete {
		t.Errorf("completed outputs = %+v", completed)
	}

	mixedIncompleteAccumulator := NewStreamAccumulator(1024)
	mixedIncomplete := mixedIncompleteAccumulator.Observe([]byte(`{"type":"response.incomplete","response":{"output":[{"id":"rs_1","type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":"done"}]},{"id":"msg_1","type":"message","status":"incomplete","content":[{"type":"output_text","text":"partial"}]}]}}`), false, at)
	if len(mixedIncomplete) != 2 {
		t.Fatalf("mixed incomplete outputs = %+v, want 2", mixedIncomplete)
	}
	completedReasoning := matchingStreamOutput(t, mixedIncomplete, func(output StreamOutput) bool {
		return output.Kind == StreamOutputReasoning
	})
	incompleteResponse := matchingStreamOutput(t, mixedIncomplete, func(output StreamOutput) bool {
		return output.Kind == StreamOutputResponse
	})
	if !completedReasoning.Complete || completedReasoning.Failed || completedReasoning.ProtocolIncomplete {
		t.Errorf("completed reasoning = %+v", completedReasoning)
	}
	if incompleteResponse.Complete || incompleteResponse.Failed || !incompleteResponse.ProtocolIncomplete {
		t.Errorf("incomplete response = %+v", incompleteResponse)
	}

	mixedFailedAccumulator := NewStreamAccumulator(1024)
	mixedFailed := mixedFailedAccumulator.Observe([]byte(`{"type":"response.failed","response":{"error":{"message":"boom"},"output":[{"id":"rs_1","type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":"done"}]},{"id":"program_1","call_id":"call_1","type":"program","code":"await tools.lookup()"},{"id":"fs_1","type":"file_search_call","status":"failed","queries":["weather"]}]}}`), false, at)
	if len(mixedFailed) != 3 {
		t.Fatalf("mixed failed outputs = %+v, want 3", mixedFailed)
	}
	completedBeforeFailure := matchingStreamOutput(t, mixedFailed, func(output StreamOutput) bool {
		return output.Kind == StreamOutputReasoning
	})
	failedTool := matchingStreamOutput(t, mixedFailed, func(output StreamOutput) bool {
		return output.Kind == StreamOutputToolCall && output.ToolType == "file_search_call"
	})
	completedProgram := matchingStreamOutput(t, mixedFailed, func(output StreamOutput) bool {
		return output.Kind == StreamOutputToolCall && output.ToolType == "program"
	})
	if !completedBeforeFailure.Complete || completedBeforeFailure.Failed {
		t.Errorf("completed before failure = %+v", completedBeforeFailure)
	}
	if !failedTool.Failed || failedTool.Complete || failedTool.ErrorMessage != "boom" || failedTool.Arguments != `["weather"]` {
		t.Errorf("failed tool = %+v", failedTool)
	}
	if !completedProgram.Complete || completedProgram.Failed || completedProgram.Arguments != "await tools.lookup()" {
		t.Errorf("completed program = %+v", completedProgram)
	}
}

func Test_StreamAccumulatorでtoolとreasoning固有の失敗と未完了が保持されている(t *testing.T) {
	at := time.Unix(27, 0)
	toolAccumulator := NewStreamAccumulator(1024)
	toolAccumulator.Observe([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"mcp_1","type":"mcp_call","name":"lookup"}}`), false, at)
	toolAccumulator.Observe([]byte(`{"type":"response.mcp_call_arguments.delta","item_id":"mcp_1","output_index":0,"delta":"{\"q\":1}"}`), false, at)
	if outputs := toolAccumulator.Observe([]byte(`{"type":"response.mcp_call.failed","item_id":"mcp_1","output_index":0,"message":"tool failed"}`), false, at.Add(time.Second)); len(outputs) != 0 {
		t.Fatalf("tool lifecycle outputs = %+v", outputs)
	}
	tools := toolAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"mcp_1","type":"mcp_call","status":"failed","name":"lookup","arguments":"{\"q\":1}"}}`), false, at.Add(2*time.Second))
	if len(tools) != 1 || tools[0].Kind != StreamOutputToolCall || tools[0].Arguments != `{"q":1}` ||
		!tools[0].Failed || tools[0].Complete || tools[0].ErrorMessage != "tool failed" {
		t.Errorf("tools = %+v", tools)
	}

	reasoningAccumulator := NewStreamAccumulator(1024)
	reasoningAccumulator.Observe([]byte(`{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":"partial"}`), false, at)
	reasoningAccumulator.Observe([]byte(`{"type":"response.reasoning_summary_part.done","item_id":"rs_1","output_index":0,"summary_index":0,"status":"incomplete","part":{"type":"summary_text","text":"partial"}}`), false, at)
	reasoning := reasoningAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","status":"completed","summary":[]}}`), false, at.Add(time.Second))
	if len(reasoning) != 1 || reasoning[0].Kind != StreamOutputReasoning || reasoning[0].Reasoning != "partial" ||
		reasoning[0].Complete || reasoning[0].Failed {
		t.Errorf("reasoning = %+v", reasoning)
	}

	partCompletedAccumulator := NewStreamAccumulator(1024)
	partCompletedAccumulator.Observe([]byte(`{"type":"response.reasoning_summary_part.done","item_id":"rs_1","output_index":0,"summary_index":0,"status":"completed","part":{"type":"summary_text","text":"part done"}}`), false, at)
	partCompleted := partCompletedAccumulator.Finish(at.Add(time.Second))
	if len(partCompleted) != 1 || partCompleted[0].Complete || partCompleted[0].Reasoning != "part done" {
		t.Errorf("part-completed reasoning = %+v", partCompleted)
	}

	messageAccumulator := NewStreamAccumulator(1024)
	messageAccumulator.MarkLimited()
	messageAccumulator.Observe([]byte(`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"partial"}`), false, at)
	messages := messageAccumulator.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","status":"incomplete","content":[{"type":"output_text","text":"partial"}]}}`), false, at.Add(time.Second))
	if len(messages) != 1 || !messages[0].ProtocolIncomplete || messages[0].Complete || !messages[0].ObservationLimited {
		t.Errorf("messages = %+v", messages)
	}
}

func Test_StreamAccumulatorで観測欠落とpart上限が区別されている(t *testing.T) {
	at := time.Unix(28, 0)
	malformed := NewStreamAccumulator(1024)
	malformed.Observe([]byte(`{"choices":[{"delta":{"content":"A"}}]}`), false, at)
	malformed.Observe([]byte(`{"choices":`), false, at)
	completed := malformed.Observe(nil, true, at.Add(time.Second))
	if len(completed) != 1 || !completed[0].ObservationLimited || !malformed.Limited() {
		t.Errorf("completed after malformed event = %+v, limited=%v", completed, malformed.Limited())
	}

	truncatedTerminal := NewStreamAccumulator(1024)
	truncatedTerminal.Observe([]byte(`{"choices":[{"delta":{"content":"A"}}]}`), false, at)
	truncatedTerminal.Observe([]byte(`{"stop":`), false, at)
	incomplete := truncatedTerminal.Finish(at.Add(time.Second))
	if len(incomplete) != 1 || incomplete[0].Complete || incomplete[0].ObservationLimited || truncatedTerminal.Limited() {
		t.Errorf("incomplete terminal = %+v, limited=%v", incomplete, truncatedTerminal.Limited())
	}

	parts := NewStreamAccumulator(1024)
	for index := 0; index <= maxStreamOutputParts; index++ {
		payload := []byte(`{"type":"response.output_text.delta","output_index":0,"content_index":` + strconv.Itoa(index) + `,"delta":"x"}`)
		parts.Observe(payload, false, at)
	}
	partOutputs := parts.Observe(nil, true, at.Add(time.Second))
	if !parts.Limited() || len(partOutputs) != 1 || !partOutputs[0].ObservationLimited || len(partOutputs[0].Content) != maxStreamOutputParts {
		t.Errorf("part-limited outputs = %+v, limited=%v", partOutputs, parts.Limited())
	}

	choices := NewStreamAccumulator(1024)
	for index := 0; index <= maxStreamOutputStates; index++ {
		payload := []byte(`{"choices":[{"index":` + strconv.Itoa(index) + `,"delta":{},"finish_reason":"stop"}]}`)
		choices.Observe(payload, false, at)
	}
	if !choices.Limited() || len(choices.finishedChoices) != maxStreamOutputStates {
		t.Errorf("finished choices = %d, limited=%v", len(choices.finishedChoices), choices.Limited())
	}

	native := NewStreamAccumulator(1024)
	native.Observe([]byte(`{"content":`), false, at)
	nativeOutputs := native.Observe([]byte(`{"content":"ok","stop":true}`), false, at.Add(time.Second))
	if !native.Limited() || len(nativeOutputs) != 1 || !nativeOutputs[0].ObservationLimited {
		t.Errorf("native outputs = %+v, limited=%v", nativeOutputs, native.Limited())
	}

	terminalShape := NewStreamAccumulator(1024)
	terminalShape.Observe([]byte(`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"partial"}`), false, at)
	terminalShapeOutputs := terminalShape.Observe([]byte(`{"type":"response.completed","response":{"output":[{"id":"msg_1","type":"message","content":[{"type":"output_text","text":{"unexpected":true}}]}]}}`), false, at.Add(time.Second))
	if !terminalShape.Limited() || len(terminalShapeOutputs) != 1 || !terminalShapeOutputs[0].ObservationLimited ||
		!terminalShapeOutputs[0].Complete || terminalShapeOutputs[0].Content != "partial" {
		t.Errorf("terminal-shape outputs = %+v, limited=%v", terminalShapeOutputs, terminalShape.Limited())
	}

	typedFailure := NewStreamAccumulator(1024)
	typedFailure.Observe([]byte(`{"type":"response.mcp_call.failed","output_index":"invalid","error":{"message":"tool failed"}}`), false, at)
	if typedFailure.closed || !typedFailure.Limited() {
		t.Errorf("typed tool failure closed=%v limited=%v", typedFailure.closed, typedFailure.Limited())
	}
	typedFailure.Observe([]byte(`{"type":"response.completed","response":{"output":[]}}`), false, at.Add(time.Second))
	if !typedFailure.closed || !typedFailure.Limited() {
		t.Errorf("typed failure recovery closed=%v limited=%v", typedFailure.closed, typedFailure.Limited())
	}
}

func Test_StreamAccumulatorでnative形式はreasoningとresponseがstopで完成している(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	started := time.Unix(30, 0)
	finished := started.Add(time.Second)

	if got := accumulator.Observe([]byte(`{"reasoning":"考","content":"こん","stop":false}`), false, started); len(got) != 0 {
		t.Fatalf("first outputs = %+v, want none", got)
	}
	outputs := accumulator.Observe([]byte(`{"reasoning_content":"察","content":"にちは","stop":true}`), false, finished)
	if len(outputs) != 2 {
		t.Fatalf("outputs = %+v, want 2", outputs)
	}
	reasoning := matchingStreamOutput(t, outputs, func(output StreamOutput) bool {
		return output.Kind == StreamOutputReasoning
	})
	response := matchingStreamOutput(t, outputs, func(output StreamOutput) bool {
		return output.Kind == StreamOutputResponse
	})
	if reasoning.Reasoning != "考察" || reasoning.FinishReason != "stop" || !reasoning.Complete {
		t.Errorf("reasoning = %+v", reasoning)
	}
	if response.Content != "こんにちは" || response.FinishReason != "stop" || !response.Complete {
		t.Errorf("response = %+v", response)
	}
}

func Test_StreamAccumulatorで同一イベントのerrorがchoice完了より優先されている(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		failed string
	}{
		{
			name:   "Chat",
			first:  `{"choices":[{"index":0,"delta":{"content":"A"}}]}`,
			failed: `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"error":{"message":"boom"}}`,
		},
		{
			name:   "native",
			first:  `{"content":"A","stop":false}`,
			failed: `{"content":"","stop":true,"error":{"message":"boom"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accumulator := NewStreamAccumulator(1024)
			at := time.Unix(35, 0)
			if outputs := accumulator.Observe([]byte(tt.first), false, at); len(outputs) != 0 {
				t.Fatalf("first outputs = %+v", outputs)
			}
			outputs := accumulator.Observe([]byte(tt.failed), false, at.Add(time.Second))
			if len(outputs) != 1 || outputs[0].Content != "A" || outputs[0].Complete || !outputs[0].Failed ||
				outputs[0].ErrorMessage != "boom" || outputs[0].FinishReason != "stop" || !accumulator.TerminalObserved() {
				t.Errorf("outputs = %+v, terminal=%v", outputs, accumulator.TerminalObserved())
			}
		})
	}
}

func Test_StreamAccumulatorでFinish時の未完成出力が返されている(t *testing.T) {
	accumulator := NewStreamAccumulator(1024)
	started := time.Unix(40, 0)
	finished := started.Add(time.Second)
	accumulator.Observe([]byte(`{"choices":[{"index":3,"delta":{"content":"partial"}}]}`), false, started)

	outputs := accumulator.Finish(finished)
	if len(outputs) != 1 {
		t.Fatalf("outputs = %+v, want 1", outputs)
	}
	output := outputs[0]
	if output.Kind != StreamOutputResponse || output.Content != "partial" || !output.HasChoiceIndex ||
		output.ChoiceIndex != 3 || output.Complete || !output.Start.Equal(started) || !output.End.Equal(finished) {
		t.Errorf("output = %+v", output)
	}
	if got := accumulator.Finish(finished.Add(time.Second)); len(got) != 0 {
		t.Errorf("second Finish outputs = %+v, want none", got)
	}
}

func Test_StreamAccumulatorのUTF8出力が上限内で切り詰められている(t *testing.T) {
	accumulator := NewStreamAccumulator(5)
	outputs := accumulator.Observe([]byte(`{"choices":[{"delta":{"content":"あい"},"finish_reason":"stop"}]}`), false, time.Unix(50, 0))
	if len(outputs) != 1 {
		t.Fatalf("outputs = %+v, want 1", outputs)
	}
	output := outputs[0]
	if output.Content != "あ" || !output.Truncated || !utf8.ValidString(output.Content) {
		t.Errorf("output = %+v", output)
	}
	if accumulator.Limited() {
		t.Error("trace field truncation must not make the stream observation-limited")
	}

	tiny := NewStreamAccumulator(1)
	tinyOutputs := tiny.Observe([]byte(`{"choices":[{"delta":{"content":"あ"},"finish_reason":"stop"}]}`), false, time.Unix(51, 0))
	if len(tinyOutputs) != 1 || tinyOutputs[0].Content != "" || !tinyOutputs[0].Truncated || !tinyOutputs[0].ContainsToken() {
		t.Errorf("tiny outputs = %+v", tinyOutputs)
	}
}

func matchingStreamOutput(t *testing.T, outputs []StreamOutput, match func(StreamOutput) bool) StreamOutput {
	t.Helper()
	for _, output := range outputs {
		if match(output) {
			return output
		}
	}
	t.Fatalf("matching output not found in %+v", outputs)
	return StreamOutput{}
}

func Test_PeekRequestで生成パラメータが取得されている(t *testing.T) {
	body := `{"model":"m1","max_completion_tokens":64,"temperature":0.25,"top_p":0.8,"top_k":20,` +
		`"frequency_penalty":0.5,"presence_penalty":0.6,"seed":11,"n":3,"stop":"</s>",` +
		`"response_format":{"type":"json_schema"}}`
	got, ok := PeekRequest(post(body), DefaultRequestLimit)
	if !ok {
		t.Fatal("want the body to parse")
	}
	p := got.Parameters
	if p.MaxTokens == nil || *p.MaxTokens != 64 {
		t.Errorf("max_tokens = %v", p.MaxTokens)
	}
	if p.Temperature == nil || *p.Temperature != 0.25 {
		t.Errorf("temperature = %v", p.Temperature)
	}
	if p.TopP == nil || *p.TopP != 0.8 || p.TopK == nil || *p.TopK != 20 {
		t.Errorf("top_p/top_k = %v/%v", p.TopP, p.TopK)
	}
	if p.FrequencyPenalty == nil || *p.FrequencyPenalty != 0.5 || p.PresencePenalty == nil || *p.PresencePenalty != 0.6 {
		t.Errorf("penalties = %v/%v", p.FrequencyPenalty, p.PresencePenalty)
	}
	if p.Seed == nil || *p.Seed != 11 || p.ChoiceCount == nil || *p.ChoiceCount != 3 {
		t.Errorf("seed/n = %v/%v", p.Seed, p.ChoiceCount)
	}
	if len(p.StopSequences) != 1 || p.StopSequences[0] != "</s>" {
		t.Errorf("stop = %v", p.StopSequences)
	}
	if p.OutputType != "json" {
		t.Errorf("output type = %q", p.OutputType)
	}
}

func Test_PeekRequestでstopが上限件数まで取得されている(t *testing.T) {
	stops := make([]string, 0, maxStopSequences+3)
	for i := range maxStopSequences + 3 {
		stops = append(stops, fmt.Sprintf("s%d", i))
	}
	raw, err := json.Marshal(map[string]any{"model": "m1", "stop": stops, "text": map[string]any{"format": map[string]any{"type": "text"}}})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := PeekRequest(post(string(raw)), DefaultRequestLimit)
	if !ok {
		t.Fatal("want the body to parse")
	}
	if len(got.Parameters.StopSequences) != maxStopSequences {
		t.Errorf("stop sequences = %d, want %d", len(got.Parameters.StopSequences), maxStopSequences)
	}
	if got.Parameters.OutputType != "text" {
		t.Errorf("output type = %q, want text", got.Parameters.OutputType)
	}
}

func Test_ParseResponseで応答IDとfinish_reasonが取得されている(t *testing.T) {
	res, ok := ParseResponse([]byte(`{"id":"cmpl-1","choices":[{"finish_reason":"stop"},{"finish_reason":"length"}]}`))
	if !ok {
		t.Fatal("want the body to parse")
	}
	if res.ID != "cmpl-1" {
		t.Errorf("id = %q", res.ID)
	}
	if len(res.FinishReasons) != 2 || res.FinishReasons[0] != "stop" || res.FinishReasons[1] != "length" {
		t.Errorf("finish reasons = %v", res.FinishReasons)
	}

	res, ok = ParseResponse([]byte(`{"type":"response.completed","response":{"id":"resp-1","status":"completed"}}`))
	if !ok {
		t.Fatal("want the Responses body to parse")
	}
	if res.ID != "resp-1" {
		t.Errorf("id = %q", res.ID)
	}
	if len(res.FinishReasons) != 1 || res.FinishReasons[0] != "completed" {
		t.Errorf("finish reasons = %v", res.FinishReasons)
	}
}
