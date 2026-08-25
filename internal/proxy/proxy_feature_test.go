package proxy

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/serversentevents"
	"github.com/k16em/llama-otel-proxy/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

type harness struct {
	proxy    *httptest.Server
	upstream *httptest.Server
	spans    *tracetest.SpanRecorder
	tp       *sdktrace.TracerProvider
	handler  *Handler
}

func newHarness(t *testing.T, upstream http.Handler, sampler sdktrace.Sampler) *harness {
	t.Helper()
	if sampler == nil {
		sampler = sdktrace.AlwaysSample()
	}
	rec := tracetest.NewSpanRecorder()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sampler),
		sdktrace.WithIDGenerator(tracing.NewIDGenerator()),

		sdktrace.WithRawSpanLimits(tracing.SpanLimits()),
	)
	up := httptest.NewServer(upstream)
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := New(Options{
		Upstream:            u,
		TracerProvider:      tp,
		Propagator:          propagation.TraceContext{},
		ModelInSpanName:     false,
		TrustTraceContext:   true,
		SessionTraceIDRoots: true,
	})
	px := httptest.NewServer(h)
	t.Cleanup(func() {
		px.Close()
		up.Close()
		h.CloseSessions()
	})
	return &harness{proxy: px, upstream: up, spans: rec, tp: tp, handler: h}
}

func (h *harness) ended(t *testing.T) map[string]sdktrace.ReadOnlySpan {
	t.Helper()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range h.spans.Ended() {
		byName[s.Name()] = s
	}
	return byName
}

func (h *harness) awaitServer(t *testing.T) map[string]sdktrace.ReadOnlySpan {
	t.Helper()
	waitFor(t, func() bool {
		for _, s := range h.spans.Ended() {
			if s.SpanKind() == trace.SpanKindServer {
				return true
			}
		}
		return false
	})
	return h.ended(t)
}

func (h *harness) endedByName(name string) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, span := range h.spans.Ended() {
		if span.Name() == name {
			spans = append(spans, span)
		}
	}
	return spans
}

func attrOf(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func mustAttr(t *testing.T, s sdktrace.ReadOnlySpan, key attribute.Key) attribute.Value {
	t.Helper()
	v, ok := attrOf(s, key)
	if !ok {
		t.Fatalf("span %q: missing attribute %s (have %v)", s.Name(), key, s.Attributes())
	}
	return v
}

const nonStreamBody = `{"model":"served-model","choices":[{"index":0}],
 "usage":{"prompt_tokens":6,"completion_tokens":8},
 "timings":{"cache_n":0,"prompt_ms":125.71,"predicted_ms":313.225,"predicted_per_second":22.34}}`

func Test_非ストリーミング応答でSERVERとCLIENTのspanに属性が記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, nonStreamBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	spans := h.awaitServer(t)
	if len(spans) != 3 {
		t.Fatalf("got %d spans %v, want 3", len(spans), spanNames(h))
	}
	server, ok := spans["chat"]
	if !ok {
		t.Fatalf("no SERVER span named %q: %v", "chat", spanNames(h))
	}
	if server.SpanKind() != trace.SpanKindServer {
		t.Errorf("kind = %v", server.SpanKind())
	}
	client, ok := spans["chat m1"]
	if !ok {
		t.Fatalf("no CLIENT span: %v", spanNames(h))
	}
	if client.SpanKind() != trace.SpanKindClient {
		t.Errorf("client kind = %v", client.SpanKind())
	}
	timeToFirstTokenSpan, ok := spans[SpanTimeToFirstToken]
	if !ok {
		t.Fatalf("no TimeToFirstToken span: %v", spanNames(h))
	}
	if _, ok := spans[SpanGeneration]; ok {
		t.Error("non-streaming responses must not get a generation span")
	}

	tid := server.SpanContext().TraceID()
	for _, s := range []sdktrace.ReadOnlySpan{client, timeToFirstTokenSpan} {
		if s.SpanContext().TraceID() != tid {
			t.Errorf("%s: different trace", s.Name())
		}
		if s.Parent().SpanID() != server.SpanContext().SpanID() {
			t.Errorf("%s: parent = %v, want SERVER", s.Name(), s.Parent().SpanID())
		}
	}

	if !timeToFirstTokenSpan.StartTime().Equal(server.StartTime()) {
		t.Errorf("TimeToFirstToken start %v != server start %v", timeToFirstTokenSpan.StartTime(), server.StartTime())
	}
	if timeToFirstTokenSpan.EndTime().After(server.EndTime()) {
		t.Error("TimeToFirstToken ends after the server span")
	}
	if !client.EndTime().After(client.StartTime()) {
		t.Error("client span has no duration")
	}

	checks := []struct {
		key  attribute.Key
		want any
	}{
		{AttrProviderName, System},
		{AttrOperationName, "chat"},
		{AttrRequestModel, "m1"},
		{AttrRequestStream, false},
		{AttrHTTPMethod, http.MethodPost},
		{AttrURLPath, "/v1/chat/completions"},
		{AttrStatusCode, int64(200)},
		{AttrResponseModel, "served-model"},
		{AttrInputTokens, int64(6)},
		{AttrOutputTokens, int64(8)},
		{AttrTimingsPromptMS, 125.71},
		{AttrTimingsPredictedMS, 313.225},
		{AttrTimingsPredictedPerSecond, 22.34},
		{AttrTimingsCacheN, int64(0)},
	}
	for _, c := range checks {
		v := mustAttr(t, server, c.key)
		if fmt.Sprint(v.AsInterface()) != fmt.Sprint(c.want) {
			t.Errorf("%s = %v, want %v", c.key, v.AsInterface(), c.want)
		}
	}
	if _, ok := attrOf(server, AttrClientDisconnected); ok {
		t.Error("client_disconnected must not be set on a clean request")
	}
	for _, s := range []sdktrace.ReadOnlySpan{server, client, timeToFirstTokenSpan} {
		if _, ok := attrOf(s, attribute.Key("gen_ai.system")); ok {
			t.Errorf("span %q carries the deprecated gen_ai.system", s.Name())
		}
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v", server.Status())
	}
}

func Test_GenAI規約のrequest属性がSERVERとCLIENTのspanに記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-42","model":"served-model","choices":[{"index":0,"finish_reason":"stop"}]}`)
	}), nil)

	body := `{"model":"m1","max_tokens":128,"temperature":0.4,"top_p":0.9,"top_k":40,` +
		`"frequency_penalty":0.1,"presence_penalty":0.2,"seed":7,"n":2,"stop":["</s>","STOP"],` +
		`"response_format":{"type":"json_object"},"messages":[]}`
	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("X-Session-Id", "session-genai")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	spans := h.awaitServer(t)
	server, ok := spans["chat"]
	if !ok {
		t.Fatalf("no SERVER span: %v", spanNames(h))
	}
	client, ok := spans["chat m1"]
	if !ok {
		t.Fatalf("no CLIENT span: %v", spanNames(h))
	}

	for _, span := range []sdktrace.ReadOnlySpan{server, client} {
		for _, c := range []struct {
			key  attribute.Key
			want any
		}{
			{AttrRequestMaxTokens, int64(128)},
			{AttrRequestTemperature, 0.4},
			{AttrRequestTopP, 0.9},
			{AttrRequestTopK, int64(40)},
			{AttrRequestFrequencyPenalty, 0.1},
			{AttrRequestPresencePenalty, 0.2},
			{AttrRequestSeed, int64(7)},
			{AttrRequestChoiceCount, int64(2)},
			{AttrOutputType, "json"},
			{AttrConversationID, "session-genai"},
		} {
			got := mustAttr(t, span, c.key)
			if fmt.Sprint(got.AsInterface()) != fmt.Sprint(c.want) {
				t.Errorf("span %q: %s = %v, want %v", span.Name(), c.key, got.AsInterface(), c.want)
			}
		}
		if got := mustAttr(t, span, AttrRequestStopSequences).AsStringSlice(); len(got) != 2 || got[0] != "</s>" {
			t.Errorf("span %q: stop_sequences = %v", span.Name(), got)
		}
		if got := mustAttr(t, span, semconv.ServerAddressKey).AsString(); got != "127.0.0.1" {
			t.Errorf("span %q: server.address = %q", span.Name(), got)
		}
		if got := mustAttr(t, span, semconv.ServerPortKey).AsInt64(); got <= 0 {
			t.Errorf("span %q: server.port = %d", span.Name(), got)
		}
	}

	if got := mustAttr(t, server, AttrResponseID).AsString(); got != "chatcmpl-42" {
		t.Errorf("gen_ai.response.id = %q", got)
	}
	if got := mustAttr(t, server, AttrResponseFinishReasons).AsStringSlice(); len(got) != 1 || got[0] != "stop" {
		t.Errorf("gen_ai.response.finish_reasons = %v", got)
	}
}

func Test_ストリーミングのfinish_reasonがSERVER_spanにまとめて記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"choices":[{"index":0,"delta":{"content":"hi"}}]}`+"\n\n")
		io.WriteString(w, `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`+"\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server, ok := h.awaitServer(t)["chat"]
	if !ok {
		t.Fatalf("no SERVER span: %v", spanNames(h))
	}
	if got := mustAttr(t, server, AttrResponseFinishReasons).AsStringSlice(); len(got) != 1 || got[0] != "length" {
		t.Errorf("gen_ai.response.finish_reasons = %v", got)
	}
}

func Test_応答モデルがファイルパスのときgen_ai_response_modelがモデル識別子になっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"model":"/var/lib/models/Qwen3.8-27B-UD-Q8_K_XL.gguf","choices":[{"index":0}]}`)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server, ok := h.awaitServer(t)["chat"]
	if !ok {
		t.Fatalf("no SERVER span: %v", spanNames(h))
	}
	if got := mustAttr(t, server, AttrResponseModel).AsString(); got != "Qwen3.8-27B-UD-Q8_K_XL" {
		t.Errorf("gen_ai.response.model = %q, want %q", got, "Qwen3.8-27B-UD-Q8_K_XL")
	}
}

func Test_REQUESTとRESPONSEのheaderとJSON_bodyがSERVER_spanにfield別で記録されている(t *testing.T) {
	requestBody := `{"model":"m1","max_context":32000,"messages":[{"role":"user","content":"hello"}],"stream":false,"temperature":0.5,"metadata":null,"stream_options":{"include_usage":true}}`
	responseBody := `{"model":"served-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"}}]}`
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Response-Value", "first")
		w.Header().Add("X-Response-Value", "second")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, responseBody)
	}), nil)

	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("X-Request-Value", "first")
	req.Header.Add("X-Request-Value", "second")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	gotBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(gotBody) != responseBody {
		t.Fatalf("response body = %q", gotBody)
	}

	server := h.awaitServer(t)["chat"]
	if _, ok := attrOf(server, AttrRequestBody); ok {
		t.Error("split request body must not retain the parent attribute")
	}
	if _, ok := attrOf(server, attribute.Key("llamaproxy.request.body.messages")); ok {
		t.Error("expanded messages must not retain the array attribute")
	}
	if _, ok := attrOf(server, AttrResponseBody); ok {
		t.Error("split response body must not retain the parent attribute")
	}
	bodyChecks := []struct {
		key      attribute.Key
		wantType attribute.Type
		want     string
	}{
		{attribute.Key("llamaproxy.request.body.max_context"), attribute.INT64, "32000"},
		{attribute.Key("llamaproxy.request.body.message.user.content"), attribute.STRINGSLICE, `["hello"]`},
		{attribute.Key("llamaproxy.request.body.metadata"), attribute.STRING, "null"},
		{attribute.Key("llamaproxy.request.body.model"), attribute.STRING, "m1"},
		{attribute.Key("llamaproxy.request.body.stream"), attribute.BOOL, "false"},
		{attribute.Key("llamaproxy.request.body.stream_options.include_usage"), attribute.STRINGSLICE, `["true"]`},
		{attribute.Key("llamaproxy.request.body.temperature"), attribute.FLOAT64, "0.5"},
		{attribute.Key("llamaproxy.response.body.choices.index"), attribute.STRINGSLICE, `["0"]`},
		{attribute.Key("llamaproxy.response.body.choices.message.role"), attribute.STRINGSLICE, `["assistant"]`},
		{attribute.Key("llamaproxy.response.body.choices.message.content"), attribute.STRINGSLICE, `["hello"]`},
		{attribute.Key("llamaproxy.response.body.model"), attribute.STRING, "served-model"},
	}
	for _, check := range bodyChecks {
		got := mustAttr(t, server, check.key)
		if got.Type() != check.wantType || got.String() != check.want {
			t.Errorf("%s = %s (%v), want %s (%v)", check.key, got.String(), got.Type(), check.want, check.wantType)
		}
	}
	if got := fmt.Sprint(mustAttr(t, server, attribute.Key("http.request.header.x-request-value")).AsStringSlice()); got != "[first second]" {
		t.Errorf("request header = %s", got)
	}
	if got := fmt.Sprint(mustAttr(t, server, attribute.Key("http.response.header.x-response-value")).AsStringSlice()); got != "[first second]" {
		t.Errorf("response header = %s", got)
	}
	if got := mustAttr(t, server, attribute.Key("http.request.header.content-type")).AsStringSlice(); len(got) != 1 || got[0] != "application/json" {
		t.Errorf("content-type = %v", got)
	}
}

func Test_JSONでないREQUESTとRESPONSEのbodyが文字列として記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "plain response")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/embeddings", "text/plain", strings.NewReader("plain request"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["embeddings"]
	if got := mustAttr(t, server, AttrRequestBody); got.Type() != attribute.STRING || got.AsString() != "plain request" {
		t.Errorf("request body = %v (%v)", got.AsInterface(), got.Type())
	}
	if got := mustAttr(t, server, AttrResponseBody); got.Type() != attribute.STRING || got.AsString() != "plain response" {
		t.Errorf("response body = %v (%v)", got.AsInterface(), got.Type())
	}
}

func Test_JSON_bodyのfieldがprimitive属性とネストの属性パスへ分割されている(t *testing.T) {
	body := `{"array": ["value", 1, true, null], "boolean":true,"encoded":"{\"nested\":true}","field.with.dot":"dot","float":0.7,"integer":9223372036854775807,"large_integer":9223372036854775808,"lossy_float":1.0000000000000001,"null":null,"object": {"nested": 1},"string":"value"}`
	attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(body), false)
	got := make(map[attribute.Key]attribute.Value, len(attrs))
	for i, attr := range attrs {
		if i > 0 && attrs[i-1].Key >= attr.Key {
			t.Errorf("attribute order = %s before %s", attrs[i-1].Key, attr.Key)
		}
		got[attr.Key] = attr.Value
	}
	if _, ok := got[AttrRequestBody]; ok {
		t.Error("split body must not retain the parent attribute")
	}
	wants := []struct {
		key      attribute.Key
		wantType attribute.Type
		want     string
	}{
		{attribute.Key("llamaproxy.request.body.array"), attribute.STRINGSLICE, `["value","1","true","null"]`},
		{attribute.Key("llamaproxy.request.body.boolean"), attribute.BOOL, "true"},
		{attribute.Key("llamaproxy.request.body.encoded"), attribute.STRING, `{"nested":true}`},
		{attribute.Key("llamaproxy.request.body.field.with.dot"), attribute.STRING, "dot"},
		{attribute.Key("llamaproxy.request.body.float"), attribute.FLOAT64, "0.7"},
		{attribute.Key("llamaproxy.request.body.integer"), attribute.INT64, "9223372036854775807"},
		{attribute.Key("llamaproxy.request.body.large_integer"), attribute.STRING, "9223372036854775808"},
		{attribute.Key("llamaproxy.request.body.lossy_float"), attribute.STRING, "1.0000000000000001"},
		{attribute.Key("llamaproxy.request.body.null"), attribute.STRING, "null"},
		{attribute.Key("llamaproxy.request.body.object.nested"), attribute.STRINGSLICE, `["1"]`},
		{attribute.Key("llamaproxy.request.body.string"), attribute.STRING, "value"},
	}
	if len(got) != len(wants) {
		t.Fatalf("attributes = %v, want %d", attrs, len(wants))
	}
	for _, want := range wants {
		value, ok := got[want.key]
		if !ok {
			t.Errorf("missing %s", want.key)
			continue
		}
		if value.Type() != want.wantType || value.String() != want.want {
			t.Errorf("%s = %s (%v), want %s (%v)", want.key, value.String(), value.Type(), want.want, want.wantType)
		}
	}
}

func Test_JSON_objectとして分割できないbodyが親属性へ記録されている(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		truncated bool
	}{
		{name: "array", body: `[{"value":1}]`},
		{name: "string", body: `"value"`},
		{name: "boolean", body: `true`},
		{name: "null", body: `null`},
		{name: "empty object", body: `{}`},
		{name: "duplicate field", body: `{"value":1,"value":2}`},
		{name: "empty field", body: `{"":1}`},
		{name: "field with space", body: `{"bad field":1}`},
		{name: "unicode field", body: `{"日本語":1}`},
		{name: "long field", body: `{"` + strings.Repeat("a", maxSplitBodyFieldNameLength+1) + `":1}`},
		{name: "multiple values", body: `{} {}`},
		{name: "empty", body: ``},
		{name: "truncated valid object", body: `{"value":1}`, truncated: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(tt.body), tt.truncated)
			if len(attrs) != 1 || attrs[0].Key != AttrRequestBody || attrs[0].Value.Type() != attribute.STRING || attrs[0].Value.AsString() != tt.body {
				t.Errorf("attributes = %v, want parent string %q", attrs, tt.body)
			}
		})
	}
}

func Test_JSON_bodyのfield数上限を超えたとき親属性へ戻っている(t *testing.T) {
	object := func(fields int) string {
		values := make([]string, fields)
		for i := range fields {
			values[i] = fmt.Sprintf(`"field_%02d":%d`, i, i)
		}
		return "{" + strings.Join(values, ",") + "}"
	}
	atLimit := object(maxSplitBodyFields)
	if attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(atLimit), false); len(attrs) != maxSplitBodyFields {
		t.Fatalf("at limit attributes = %d, want %d", len(attrs), maxSplitBodyFields)
	}
	overLimit := object(maxSplitBodyFields + 1)
	attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(overLimit), false)
	if len(attrs) != 1 || attrs[0].Key != AttrRequestBody || attrs[0].Value.AsString() != overLimit {
		t.Errorf("over limit attributes = %v", attrs)
	}
	longestField := strings.Repeat("a", maxSplitBodyFieldNameLength)
	attrs = traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(`{"`+longestField+`":1}`), false)
	if len(attrs) != 1 || attrs[0].Key != attribute.Key(string(AttrRequestBody)+"."+longestField) {
		t.Errorf("longest field attributes = %v", attrs)
	}
}

func splitBodyAttributes(t *testing.T, body string) map[attribute.Key]attribute.Value {
	t.Helper()
	attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, []byte(body), false)
	got := make(map[attribute.Key]attribute.Value, len(attrs))
	for i, attr := range attrs {
		if i > 0 && attrs[i-1].Key >= attr.Key {
			t.Errorf("attribute order = %s before %s", attrs[i-1].Key, attr.Key)
		}
		got[attr.Key] = attr.Value
	}
	return got
}

func wantSliceAttributes(t *testing.T, got map[attribute.Key]attribute.Value, wants map[attribute.Key][]string) {
	t.Helper()
	for key, want := range wants {
		value, ok := got[key]
		if !ok {
			t.Errorf("missing %s (have %v)", key, got)
			continue
		}
		if value.Type() != attribute.STRINGSLICE || !slices.Equal(value.AsStringSlice(), want) {
			t.Errorf("%s = %v (%v), want %v", key, value.AsInterface(), value.Type(), want)
		}
	}
}

func Test_messagesがroleごとのspan属性へ展開されている(t *testing.T) {
	body := `{"model":"m1","messages":[{"role":"system","content":"sys"},{"role":"user","content":"hi"},{"role":"assistant","content":"yo"}]}`
	got := splitBodyAttributes(t, body)
	if _, ok := got[attribute.Key("llamaproxy.request.body.messages")]; ok {
		t.Error("expanded messages must not retain the array attribute")
	}
	if _, ok := got[attribute.Key("llamaproxy.request.body.message.user.role")]; ok {
		t.Error("role belongs to the key, not to an attribute")
	}
	if _, ok := got[AttrTraceRequestBodySplitLimited]; ok {
		t.Error("unlimited body must not be marked limited")
	}
	if value := got[attribute.Key("llamaproxy.request.body.model")]; value.Type() != attribute.STRING || value.AsString() != "m1" {
		t.Errorf("model = %v (%v)", value.AsInterface(), value.Type())
	}
	wantSliceAttributes(t, got, map[attribute.Key][]string{
		"llamaproxy.request.body.message.system.content":    {"sys"},
		"llamaproxy.request.body.message.user.content":      {"hi"},
		"llamaproxy.request.body.message.assistant.content": {"yo"},
	})
}

func Test_roleが解決できないmessageがdefaultへ記録されている(t *testing.T) {
	tests := []struct {
		name string
		body string
		want map[attribute.Key][]string
	}{
		{
			name: "missing role",
			body: `{"messages":[{"content":"a"}]}`,
			want: map[attribute.Key][]string{"llamaproxy.request.body.message.default.content": {"a"}},
		},
		{
			name: "non string role",
			body: `{"messages":[{"role":1,"content":"a"}]}`,
			want: map[attribute.Key][]string{"llamaproxy.request.body.message.default.content": {"a"}},
		},
		{
			name: "unicode role",
			body: `{"messages":[{"role":"ユーザー","content":"a"}]}`,
			want: map[attribute.Key][]string{"llamaproxy.request.body.message.default.content": {"a"}},
		},
		{
			name: "empty role",
			body: `{"messages":[{"role":"","content":"a"}]}`,
			want: map[attribute.Key][]string{"llamaproxy.request.body.message.default.content": {"a"}},
		},
		{
			name: "scalar message",
			body: `{"messages":["a"]}`,
			want: map[attribute.Key][]string{"llamaproxy.request.body.message.default": {"a"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantSliceAttributes(t, splitBodyAttributes(t, tt.body), tt.want)
		})
	}
}

func Test_同じroleのmessageが出現順のStringSliceへまとまっている(t *testing.T) {
	body := `{"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"reply"},{"role":"user","content":"second"}]}`
	wantSliceAttributes(t, splitBodyAttributes(t, body), map[attribute.Key][]string{
		"llamaproxy.request.body.message.user.content":      {"first", "second"},
		"llamaproxy.request.body.message.assistant.content": {"reply"},
	})
}

func Test_multimodalのcontentがキーごとの属性へ展開されている(t *testing.T) {
	body := `{"messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"https://example.test/y.png"}}]}]}`
	wantSliceAttributes(t, splitBodyAttributes(t, body), map[attribute.Key][]string{
		"llamaproxy.request.body.message.user.content.type":          {"text", "image_url"},
		"llamaproxy.request.body.message.user.content.text":          {"hi"},
		"llamaproxy.request.body.message.user.content.image_url.url": {"https://example.test/y.png"},
	})
}

func Test_tool_callのmessageがtool_callsとtool_call_idへ展開されている(t *testing.T) {
	body := `{"messages":[` +
		`{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"read","arguments":"{\"path\":\"a.go\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"{\"result\":42}"}]}`
	wantSliceAttributes(t, splitBodyAttributes(t, body), map[attribute.Key][]string{
		"llamaproxy.request.body.message.assistant.content":                       {"null"},
		"llamaproxy.request.body.message.assistant.tool_calls.id":                 {"call_1"},
		"llamaproxy.request.body.message.assistant.tool_calls.type":               {"function"},
		"llamaproxy.request.body.message.assistant.tool_calls.function.name":      {"read"},
		"llamaproxy.request.body.message.assistant.tool_calls.function.arguments": {`{"path":"a.go"}`},
		"llamaproxy.request.body.message.tool.tool_call_id":                       {"call_1"},
		"llamaproxy.request.body.message.tool.content":                            {`{"result":42}`},
	})
}

func Test_ネストの深さ上限を超えた部分木がJSON文字列として記録されている(t *testing.T) {
	deepest := maxSplitBodyDepth + 2
	value := "1"
	for level := deepest; level >= 1; level-- {
		value = fmt.Sprintf(`{"l%d":%s}`, level, value)
	}
	path := AttrRequestBody
	for level := 1; level <= maxSplitBodyDepth+1; level++ {
		path = attribute.Key(fmt.Sprintf("%s.l%d", path, level))
	}
	got := splitBodyAttributes(t, value)
	wantSliceAttributes(t, got, map[attribute.Key][]string{
		path: {fmt.Sprintf(`{"l%d":1}`, deepest)},
	})
	if limited, ok := got[AttrTraceRequestBodySplitLimited]; !ok || !limited.AsBool() {
		t.Error("depth limited body must be marked limited")
	}
}

func Test_ネストのfield名が不正な部分木がJSON文字列として記録されている(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{name: "unicode field", body: `{"model":"m1","outer":{"日本語":1}}`, want: []string{`{"日本語":1}`}},
		{name: "field with space", body: `{"model":"m1","outer":{"bad field":1}}`, want: []string{`{"bad field":1}`}},
		{name: "duplicate field", body: `{"model":"m1","outer":{"a":1,"a":2}}`, want: []string{`{"a":1,"a":2}`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitBodyAttributes(t, tt.body)
			wantSliceAttributes(t, got, map[attribute.Key][]string{"llamaproxy.request.body.outer": tt.want})
			if value := got[attribute.Key("llamaproxy.request.body.model")]; value.Type() != attribute.STRING || value.AsString() != "m1" {
				t.Errorf("sibling field must stay expanded, model = %v (%v)", value.AsInterface(), value.Type())
			}
			if limited, ok := got[AttrTraceRequestBodySplitLimited]; !ok || !limited.AsBool() {
				t.Error("degraded subtree must be marked limited")
			}
		})
	}
}

func Test_展開属性数とスライス長の上限超過がllamaproxy属性で示されている(t *testing.T) {
	fields := make([]string, maxSplitBodyAttributes+8)
	for i := range fields {
		fields[i] = fmt.Sprintf(`"k%04d":%d`, i, i)
	}
	wide := splitBodyAttributes(t, `{"wide":{`+strings.Join(fields, ",")+`}}`)
	if limited, ok := wide[AttrTraceRequestBodySplitLimited]; !ok || !limited.AsBool() {
		t.Error("attribute count overflow must be marked limited")
	}
	if len(wide) != maxSplitBodyAttributes+1 {
		t.Errorf("attributes = %d, want %d", len(wide), maxSplitBodyAttributes+1)
	}

	values := make([]string, maxSplitBodyValues+8)
	for i := range values {
		values[i] = strconv.Itoa(i)
	}
	long := splitBodyAttributes(t, `{"list":[`+strings.Join(values, ",")+`]}`)
	if limited, ok := long[AttrTraceRequestBodySplitLimited]; !ok || !limited.AsBool() {
		t.Error("value count overflow must be marked limited")
	}
	list, ok := long[attribute.Key("llamaproxy.request.body.list")]
	if !ok {
		t.Fatalf("missing list attribute (have %v)", long)
	}
	if got := list.AsStringSlice(); len(got) != maxSplitBodyValues || got[0] != "0" || got[maxSplitBodyValues-1] != strconv.Itoa(maxSplitBodyValues-1) {
		t.Errorf("list = %d values, want the first %d", len(got), maxSplitBodyValues)
	}
}

func Test_同じX_SESSION_IDの応答ごとに別SERVER_spanが同じtraceへ記録されている(t *testing.T) {
	type forwardedTraceContext struct {
		traceparent string
		tracestate  string
	}
	traceContexts := make(chan forwardedTraceContext, 2)
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceContexts <- forwardedTraceContext{
			traceparent: r.Header.Get("traceparent"),
			tracestate:  r.Header.Get("tracestate"),
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, nonStreamBody)
	}), nil)

	const sessionID = "ses_66a71b6f4ffeq796jvvOpJQ04m"
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m1"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Session-Id", sessionID)
		if i == 0 {
			req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
			req.Header.Set("tracestate", "vendor=old-trace")
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}

	waitFor(t, func() bool {
		count := 0
		for _, span := range h.spans.Ended() {
			if span.SpanKind() == trace.SpanKindServer {
				count++
			}
		}
		return count == 2
	})
	var servers []sdktrace.ReadOnlySpan
	for _, span := range h.spans.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			servers = append(servers, span)
		}
	}
	for _, server := range servers {
		if got := server.SpanContext().TraceID().String(); got != "f5a7a908ebca02bae6f6084dfa1c5e7a" {
			t.Errorf("trace id = %s", got)
		}
		if !server.Parent().IsValid() {
			t.Error("parent = none, want the session span")
		}
		if got := mustAttr(t, server, semconv.SessionIDKey); got.AsString() != sessionID {
			t.Errorf("session.id = %q", got.AsString())
		}
	}
	if servers[0].Parent().SpanID() != servers[1].Parent().SpanID() {
		t.Error("responses did not share one session span")
	}
	if servers[0].SpanContext().SpanID() == servers[1].SpanContext().SpanID() {
		t.Error("responses reused a span id")
	}
	if len(servers[0].Links()) != 1 || servers[0].Links()[0].SpanContext.TraceID().String() != "11111111111111111111111111111111" {
		t.Errorf("incoming trace context was not retained as a link: %v", servers[0].Links())
	}
	for i := 0; i < 2; i++ {
		got := <-traceContexts
		if !strings.HasPrefix(got.traceparent, "00-f5a7a908ebca02bae6f6084dfa1c5e7a-") {
			t.Errorf("upstream traceparent = %q", got.traceparent)
		}
		if got.tracestate != "" {
			t.Errorf("upstream tracestate = %q", got.tracestate)
		}
	}
}

func Test_trace_contextを信頼しないときX_SESSION_IDがtrace_IDに採用されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), nil)
	h.handler.opts.TrustTraceContext = false

	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Session-Id", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if got := server.SpanContext().TraceID().String(); got == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Error("untrusted session id selected the trace id")
	}
}

func Test_trace_contextを信頼しないとき外側のspanも親にしない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), nil)
	h.handler.opts.TrustTraceContext = false

	outerCtx, outer := h.tp.Tracer("middleware").Start(context.Background(), "outer")
	defer outer.End()
	req := httptest.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", nil).WithContext(outerCtx)
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")

	ctx, incoming, newRoot := h.handler.traceContext(req)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("outer span context remained in the request context")
	}
	if !newRoot {
		t.Error("server span was not forced to a new root")
	}
	if got := incoming.TraceID().String(); got != "11111111111111111111111111111111" {
		t.Errorf("linked trace id = %s", got)
	}
}

func Test_ServerSentEventsの断片がchoiceとtool_callごとのspanへ組み立てられている(t *testing.T) {
	responseBody := `data: {"choices":[{"index":0,"delta":{"reasoning_content":"think "}},{"index":1,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"weather","arguments":"{\"city\":"}}]}}]}` + "\n\n" +
		`data: {"choices":[{"index":0,"delta":{"reasoning_content":"step","content":"answer"}},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Tokyo\"}"}}]}}]}` + "\n\n" +
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n" +
		"data: [DONE]\n\n"
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, responseBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	forwarded, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if string(forwarded) != responseBody {
		t.Errorf("forwarded response body = %q, want %q", forwarded, responseBody)
	}

	server := h.awaitServer(t)["chat"]
	if _, ok := attrOf(server, AttrResponseBody); ok {
		t.Error("SERVER span must not retain the framed Server-Sent Events response body")
	}
	if got := mustAttr(t, server, AttrServerSentEventsChunkCount).AsInt64(); got != 3 {
		t.Errorf("Server-Sent Events chunk count = %d, want 3", got)
	}
	reasoning := h.endedByName(SpanReasoning)
	responses := h.endedByName(SpanResponse)
	tools := h.endedByName(SpanToolCall)
	if len(reasoning) != 1 || len(responses) != 1 || len(tools) != 1 {
		t.Fatalf("reasoning=%d response=%d tool_call=%d: %v", len(reasoning), len(responses), len(tools), spanNames(h))
	}
	if got := len(h.spans.Ended()); got != 7 {
		t.Errorf("spans = %d, want 7: %v", got, spanNames(h))
	}
	for _, output := range []sdktrace.ReadOnlySpan{reasoning[0], responses[0], tools[0]} {
		if output.SpanKind() != trace.SpanKindInternal {
			t.Errorf("%s kind = %v, want INTERNAL", output.Name(), output.SpanKind())
		}
		if output.SpanContext().TraceID() != server.SpanContext().TraceID() || output.Parent().SpanID() != server.SpanContext().SpanID() {
			t.Errorf("%s is not a direct child of SERVER", output.Name())
		}
		if output.StartTime().Before(server.StartTime()) || output.EndTime().After(server.EndTime()) || output.EndTime().Before(output.StartTime()) {
			t.Errorf("%s time %v..%v is outside SERVER %v..%v", output.Name(), output.StartTime(), output.EndTime(), server.StartTime(), server.EndTime())
		}
	}
	if got := mustAttr(t, reasoning[0], AttrChoiceIndex).AsInt64(); got != 0 {
		t.Errorf("reasoning choice = %d", got)
	}
	if got := mustAttr(t, reasoning[0], AttrResponseBodyReasoningContent).AsString(); got != "think step" {
		t.Errorf("reasoning = %q", got)
	}
	if got := mustAttr(t, responses[0], AttrResponseBodyContent).AsString(); got != "answer" {
		t.Errorf("response = %q", got)
	}
	if got := mustAttr(t, tools[0], AttrChoiceIndex).AsInt64(); got != 1 {
		t.Errorf("tool choice = %d", got)
	}
	if got := mustAttr(t, tools[0], AttrToolCallIndex).AsInt64(); got != 0 {
		t.Errorf("tool index = %d", got)
	}
	if got := mustAttr(t, tools[0], AttrToolCallID).AsString(); got != "call_1" {
		t.Errorf("tool id = %q", got)
	}
	if got := mustAttr(t, tools[0], AttrResponseBodyName).AsString(); got != "weather" {
		t.Errorf("tool name = %q", got)
	}
	if got := mustAttr(t, tools[0], AttrResponseBodyArguments).AsString(); got != `{"city":"Tokyo"}` {
		t.Errorf("tool arguments = %q", got)
	}
}

func Test_組み立て中のresponseはspanにならずchoice完了時に送信されている(t *testing.T) {
	finish := make(chan struct{})
	release := make(chan struct{})
	var finishOnce sync.Once
	var releaseOnce sync.Once
	finishChoice := func() {
		finishOnce.Do(func() { close(finish) })
	}
	releaseStream := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer finishChoice()
	defer releaseStream()

	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		fl.Flush()
		<-finish
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
		fl.Flush()
		<-release
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(h.endedByName(SpanResponse)); got != 0 {
		t.Fatalf("response spans before finish = %d", got)
	}
	finishChoice()
	waitFor(t, func() bool { return len(h.endedByName(SpanResponse)) == 1 })
	for _, span := range h.spans.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			t.Fatal("SERVER span ended before the stream completed")
		}
	}

	releaseStream()
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)
}

func Test_上限を超えるServerSentEvents応答の組み立て結果がspan内で切り詰められている(t *testing.T) {
	payload := `{"choices":[{"delta":{"content":"` + strings.Repeat("x", maxTracedBodySize+1) + `"}}]}`
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: "+payload+"\n\ndata: [DONE]\n\n")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	response := responses[0]
	if got := len(mustAttr(t, response, AttrResponseBodyContent).AsString()); got != maxTracedBodySize {
		t.Errorf("response body length = %d", got)
	}
	if !mustAttr(t, response, AttrTraceResponseBodyTruncated).AsBool() {
		t.Error("response body was not marked truncated")
	}
	if got := mustAttr(t, response, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("response outcome = %q", got)
	}
}

func Test_trace用bodyの上限を超えてもREQUESTとRESPONSEが変更されていない(t *testing.T) {
	requestBody := `{"model":"m1","input":"` + strings.Repeat("r", maxTracedBodySize) + `"}`
	responseBody := `{"model":"served-model","output":"` + strings.Repeat("s", maxTracedBodySize) + `"}`
	forwarded := make(chan string, 1)
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		forwarded <- string(body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, responseBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	gotResponse, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if got := <-forwarded; got != requestBody {
		t.Errorf("forwarded request = %d bytes, want %d", len(got), len(requestBody))
	}
	if string(gotResponse) != responseBody {
		t.Errorf("forwarded response = %d bytes, want %d", len(gotResponse), len(responseBody))
	}

	server := h.awaitServer(t)["chat"]
	if got := len(mustAttr(t, server, AttrRequestBody).AsString()); got != maxTracedBodySize {
		t.Errorf("traced request = %d bytes", got)
	}
	if got := len(mustAttr(t, server, AttrResponseBody).AsString()); got != maxTracedBodySize {
		t.Errorf("traced response = %d bytes", got)
	}
	if !mustAttr(t, server, AttrTraceRequestBodyTruncated).AsBool() {
		t.Error("request trace body was not marked truncated")
	}
	if !mustAttr(t, server, AttrTraceResponseBodyTruncated).AsBool() {
		t.Error("response trace body was not marked truncated")
	}
}

func spanNames(h *harness) []string {
	var out []string
	for _, s := range h.spans.Ended() {
		out = append(out, s.Name())
	}
	return out
}

func serverSentEventsUpstream(chunks int, delay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		for i := 0; i < chunks; i++ {

			fmt.Fprintf(w, "data: {\"model\":\"served-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t%d\"}}]}\n\n", i)
			fl.Flush()
			time.Sleep(delay)
		}
		io.WriteString(w, `data: {"model":"served-model","usage":{"prompt_tokens":6,"completion_tokens":8},"timings":{"prompt_ms":10.5,"predicted_ms":20.5}}`+"\n\n")
		fl.Flush()
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
}

func Test_ストリーミング応答でphase_spanが適切な時刻に記録されている(t *testing.T) {
	const delay = 30 * time.Millisecond
	h := newHarness(t, serverSentEventsUpstream(3, delay), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	start := time.Now()
	br := bufio.NewReader(res.Body)
	firstLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	firstAt := time.Since(start)
	if !strings.HasPrefix(firstLine, "data:") {
		t.Fatalf("first line = %q", firstLine)
	}
	if firstAt > 2*delay {
		t.Errorf("first chunk took %v: the response is being buffered", firstAt)
	}
	io.Copy(io.Discard, br)
	res.Body.Close()

	h.awaitServer(t)

	spans := h.ended(t)
	for _, name := range []string{"chat", "chat m1", SpanTimeToFirstToken, SpanGeneration} {
		if _, ok := spans[name]; !ok {
			t.Fatalf("missing span %q: %v", name, spanNames(h))
		}
	}
	server := spans["chat"]
	gen := spans[SpanGeneration]
	timeToFirstTokenSpan := spans[SpanTimeToFirstToken]

	if v := mustAttr(t, server, AttrRequestStream); v.AsBool() != true {
		t.Error("gen_ai.request.stream should be true")
	}

	if v := mustAttr(t, server, AttrServerSentEventsChunkCount); v.AsInt64() != 4 {
		t.Errorf("Server-Sent Events chunk count = %d, want 4", v.AsInt64())
	}

	if v := mustAttr(t, server, AttrInputTokens); v.AsInt64() != 6 {
		t.Errorf("input_tokens = %d", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrOutputTokens); v.AsInt64() != 8 {
		t.Errorf("output_tokens = %d", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrTimingsPromptMS); v.AsFloat64() != 10.5 {
		t.Errorf("prompt_ms = %v", v.AsFloat64())
	}

	if !gen.StartTime().Equal(timeToFirstTokenSpan.EndTime()) {
		t.Errorf("generation starts at %v, TimeToFirstToken ends at %v", gen.StartTime(), timeToFirstTokenSpan.EndTime())
	}
	if gen.EndTime().Sub(gen.StartTime()) < 2*delay {
		t.Errorf("generation span too short: %v", gen.EndTime().Sub(gen.StartTime()))
	}
	if gen.Parent().SpanID() != server.SpanContext().SpanID() {
		t.Error("generation span is not a child of SERVER")
	}
}

func Test_計装対象外のpathでspanが生成されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/logs/stream" {

			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			for {
				select {
				case <-r.Context().Done():
					return
				default:
				}
				io.WriteString(w, "data: log line\n\n")
				fl.Flush()
				time.Sleep(5 * time.Millisecond)
			}
		}
		io.WriteString(w, "ok")
	}), nil)

	for _, path := range []string{"/v1/models", "/v1/messages", "/ui/index.html", "/running"} {
		res, err := http.Get(h.proxy.URL + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.proxy.URL+"/logs/stream", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := res.Body.Read(buf); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	res.Body.Close()

	if got := len(h.spans.Ended()); got != 0 {
		t.Errorf("got %d spans, want none: %v", got, spanNames(h))
	}
	if got := len(h.spans.Started()); got != 0 {
		t.Errorf("%d spans were started (and would never end): %v", got, spanNames(h))
	}
}

func Test_clientが切断したときspanがerrorになっていない(t *testing.T) {
	upstreamDone := make(chan struct{})
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 0; i < 200; i++ {
			if _, err := fmt.Fprintf(w, "data: {\"n\":%d}\n\n", i); err != nil {
				return
			}
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := res.Body.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	cancel()
	res.Body.Close()
	<-upstreamDone

	waitFor(t, func() bool {
		for _, s := range h.spans.Ended() {
			if s.SpanKind() == trace.SpanKindServer {
				return true
			}
		}
		return false
	})

	var server sdktrace.ReadOnlySpan
	for _, s := range h.spans.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			server = s
		}
	}
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if server.Status().Code == codes.Error {
		t.Errorf("a client disconnect must not be an error, got %v", server.Status())
	}
	v, ok := attrOf(server, AttrClientDisconnected)
	if !ok || !v.AsBool() {
		t.Errorf("want llamaproxy.client_disconnected=true, attrs: %v", server.Attributes())
	}
}

func Test_Upstreamへ接続できないとき502が返されspanにerrorが記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), nil)
	h.upstream.Close()

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	if res.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", res.StatusCode)
	}
	spans := h.awaitServer(t)
	server, ok := spans["chat"]
	if !ok {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if server.Status().Code != codes.Error {
		t.Errorf("status = %v, want error", server.Status())
	}
	if len(server.Events()) == 0 {
		t.Error("want the error recorded on the span")
	}
	if v := mustAttr(t, server, AttrStatusCode); v.AsInt64() != 502 {
		t.Errorf("status_code = %d", v.AsInt64())
	}
}

func Test_Upstreamが5xxを返したときSERVERSpanにエラーとステータスコードが記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}), nil)

	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if server.Status().Code != codes.Error {
		t.Errorf("status = %v, want error", server.Status())
	}
	if v := mustAttr(t, server, AttrStatusCode); v.AsInt64() != 500 {
		t.Errorf("status_code = %d", v.AsInt64())
	}
}

func Test_NeverSampleを指定したときspanが記録されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), sdktrace.NeverSample())

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()

	if len(h.spans.Ended()) != 0 {
		t.Errorf("got %d spans with NeverSample", len(h.spans.Ended()))
	}

	if string(body) != nonStreamBody {
		t.Errorf("body was altered: %q", body)
	}
}

func Test_受信したtrace_contextがUpstreamへ伝播されている(t *testing.T) {
	gotHeader := make(chan string, 1)
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader <- r.Header.Get("traceparent")
		io.WriteString(w, nonStreamBody)
	}), nil)

	const incoming = "00-11111111111111111111111111111111-2222222222222222-01"
	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`))
	req.Header.Set("traceparent", incoming)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	upstreamHeader := <-gotHeader
	if upstreamHeader == "" {
		t.Fatal("no traceparent was injected upstream")
	}
	if !strings.HasPrefix(upstreamHeader, "00-11111111111111111111111111111111-") {
		t.Errorf("upstream traceparent = %q, want the incoming trace id", upstreamHeader)
	}
	server := h.awaitServer(t)["chat"]
	if server == nil {
		t.Fatal("no server span")
	}
	if got := server.SpanContext().TraceID().String(); got != "11111111111111111111111111111111" {
		t.Errorf("trace id = %s", got)
	}
	if got := server.Parent().SpanID().String(); got != "2222222222222222" {
		t.Errorf("parent span id = %s", got)
	}

	client := h.awaitServer(t)["chat m1"]
	if !strings.Contains(upstreamHeader, client.SpanContext().SpanID().String()) {
		t.Errorf("upstream traceparent %q does not carry the client span id %s",
			upstreamHeader, client.SpanContext().SpanID())
	}
}

func Test_モデルが不明なときSERVERとCLIENTのspan名が操作名だけになっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, nonStreamBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	h.awaitServer(t)
	var named int
	for _, s := range h.spans.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindServer, trace.SpanKindClient:
			named++
			if s.Name() != "chat" {
				t.Errorf("%v span name = %q, want %q", s.SpanKind(), s.Name(), "chat")
			}
		}
	}
	if named != 2 {
		t.Fatalf("got %v, want one SERVER and one CLIENT span", spanNames(h))
	}
}

func Test_span名にモデル名を含める設定が反映されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	px := httptest.NewServer(New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: true, TrustTraceContext: true,
	}))
	defer px.Close()

	res, _ := http.Post(px.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var done bool
		for _, s := range rec.Ended() {
			done = done || s.SpanKind() == trace.SpanKindServer
		}
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	var found bool
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			found = true
			if s.Name() != "chat m1" {
				t.Errorf("span name = %q, want %q", s.Name(), "chat m1")
			}
			if v, ok := attrOf(s, AttrRequestModel); !ok || v.AsString() != "m1" {
				t.Error("the model must still be an attribute")
			}
		}
	}
	if !found {
		t.Fatal("no server span")
	}
}

func Test_解析したrequest_bodyが変更されずUpstreamへ転送されている(t *testing.T) {
	body := `{"model":"m1","messages":[{"role":"user","content":"hello"}]}`
	got := make(chan string, 1)
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- string(b)
		io.WriteString(w, nonStreamBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	if forwarded := <-got; forwarded != body {
		t.Errorf("upstream saw %q, want %q", forwarded, body)
	}
}

func Test_APIのpathに対応するoperation名がspanに記録されている(t *testing.T) {
	cases := map[string]string{
		"/v1/chat/completions": "chat",
		"/v1/completions":      "text_completion",
		"/v1/responses":        "chat",
		"/v1/embeddings":       "embeddings",
		"/completion":          "text_completion",
		"/infill":              "text_completion",
		"/v1/rerank":           "rerank",
		"/v1/reranking":        "rerank",
		"/rerank":              "rerank",
	}
	for path, op := range cases {
		t.Run(path, func(t *testing.T) {
			h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, nonStreamBody)
			}), nil)
			res, _ := http.Post(h.proxy.URL+path, "application/json", strings.NewReader(`{"model":"m1"}`))
			io.Copy(io.Discard, res.Body)
			res.Body.Close()

			server := h.awaitServer(t)[op+" m1"]
			if server == nil {
				t.Fatalf("want a span named %q, got %v", op+" m1", spanNames(h))
			}
			if v := mustAttr(t, server, AttrOperationName); v.AsString() != op {
				t.Errorf("operation = %s, want %s", v.AsString(), op)
			}
		})
	}
}

func Test_request_bodyを解析できないとき既知の属性だけがspanに記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/embeddings", "text/plain", strings.NewReader("not json"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["embeddings"]
	if server == nil {
		t.Fatalf("want a span named %q, got %v", "embeddings", spanNames(h))
	}
	if _, ok := attrOf(server, AttrRequestModel); ok {
		t.Error("no model should be recorded")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 10s")
}

var _ = net.Dial

func Test_すべての生成AI_spanに共通dimensionが記録されている(t *testing.T) {
	h := newHarness(t, serverSentEventsUpstream(2, time.Millisecond), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	dims := []attribute.Key{AttrRequestModel, AttrOperationName, AttrRequestStream}
	for _, s := range h.spans.Ended() {
		for _, key := range dims {
			if _, ok := attrOf(s, key); !ok {
				t.Errorf("span %q is missing dimension %s", s.Name(), key)
			}
		}
	}
}

type errAfterFirstByte struct{ header http.Header }

func (t *errAfterFirstByte) RoundTrip(r *http.Request) (*http.Response, error) {
	h := t.header
	if h == nil {
		h = http.Header{"Content-Type": []string{"application/json"}}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     h,
		Body:       io.NopCloser(&brokenReader{}),
		Request:    r,
	}, nil
}

type brokenReader struct{ sent bool }

func (b *brokenReader) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		p[0] = '{'
		return 1, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func Test_Upstreamのbody読み込みエラーがspanに記録されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	px := httptest.NewServer(New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: &errAfterFirstByte{},
	}))
	defer px.Close()

	res, err := http.Post(px.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	waitForSpan(t, rec, trace.SpanKindServer)
	server := serverSpan(rec)
	if server == nil {
		t.Fatal("no server span")
	}
	if server.Status().Code != codes.Error {
		t.Errorf("status = %v, want error: a truncated upstream response is not a success",
			server.Status())
	}
	if len(server.Events()) == 0 {
		t.Error("want the read error recorded on the span")
	}
	if v := mustAttr(t, server, AttrOutcome); v.AsString() != string(OutcomeUpstreamError) {
		t.Errorf("outcome = %q, want %q", v.AsString(), OutcomeUpstreamError)
	}
}

func Test_streamリクエストへのエラー応答でgeneration_spanが生成されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":{"message":"boom"}}`)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	spans := h.awaitServer(t)
	if _, ok := spans[SpanGeneration]; ok {
		t.Error("an error body must not produce a generation span")
	}
	server := spans["chat"]
	if server.Status().Code != codes.Error {
		t.Errorf("status = %v, want error", server.Status())
	}
}

func Test_phase_spanにHTTP_status_codeが記録されている(t *testing.T) {
	h := newHarness(t, serverSentEventsUpstream(2, time.Millisecond), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	spans := h.ended(t)
	phaseSpans := []sdktrace.ReadOnlySpan{spans[SpanTimeToFirstToken], spans[SpanGeneration]}
	phaseSpans = append(phaseSpans, h.endedByName(SpanReasoning)...)
	phaseSpans = append(phaseSpans, h.endedByName(SpanResponse)...)
	phaseSpans = append(phaseSpans, h.endedByName(SpanToolCall)...)
	for _, s := range phaseSpans {
		if s == nil {
			t.Fatal("missing phase span")
		}
		v, ok := attrOf(s, AttrStatusCode)
		if !ok || v.AsInt64() != 200 {
			t.Errorf("%s: http.response.status_code = %v (present=%v), want 200", s.Name(), v.AsInt64(), ok)
		}
	}
}

func Test_終端のないstreamがincompleteとして記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"n\":1}\n\n")
		w.(http.Flusher).Flush()

	}), nil)

	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if v, ok := attrOf(server, AttrResponseIncomplete); !ok || !v.AsBool() {
		t.Error("want llamaproxy.response_incomplete=true for a stream without [DONE]")
	}
}

func Test_request_bodyを解析できなくてもContentTypeからstreamが検出されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	up := httptest.NewServer(serverSentEventsUpstream(2, time.Millisecond))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	px := httptest.NewServer(New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false,

		RequestBodyLimit: 16,
	}))
	defer px.Close()

	body := `{"model":"m1","stream":true,"messages":[{"role":"user","content":"` +
		strings.Repeat("x", 200) + `"}]}`
	res, err := http.Post(px.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	waitForSpan(t, rec, trace.SpanKindServer)

	var gen, server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch {
		case s.Name() == SpanGeneration:
			gen = s
		case s.SpanKind() == trace.SpanKindServer:
			server = s
		}
	}
	if server == nil {
		t.Fatal("no server span")
	}
	if _, ok := attrOf(server, AttrRequestStream); ok {
		t.Error("gen_ai.request.stream must be omitted when the body could not be parsed")
	}
	if v, ok := attrOf(server, AttrRequestModel); !ok || v.AsString() != "m1" {
		t.Error("gen_ai.request.model must survive a body cut after it")
	}

	if gen == nil {
		t.Fatal("want a generation span: the response was an event stream")
	}
	if v := mustAttr(t, server, AttrServerSentEventsChunkCount); v.AsInt64() != 3 {
		t.Errorf("Server-Sent Events chunk count = %d, want 3", v.AsInt64())
	}
}

func Test_最大構成の本文とheaderでもspan属性が上限で捨てられていない(t *testing.T) {
	messages := make([]any, 0, maxSplitBodyFields)
	for i := range 120 {
		messages = append(messages, map[string]any{
			"role": fmt.Sprintf("role%d", i), "content": "c", "name": "n", "tool_call_id": "t",
		})
	}
	request := map[string]any{"model": "m1", "messages": messages}
	for i := range maxSplitBodyFields - 2 {
		request[fmt.Sprintf("field%d", i)] = i
	}
	requestBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response := map[string]any{"model": "served-model"}
	for i := range maxSplitBodyFields - 1 {
		response[fmt.Sprintf("field%d", i)] = map[string]any{"a": i, "b": i, "c": i, "d": i, "e": i}
	}
	responseBody, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}

	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for i := range 200 {
			w.Header().Set(fmt.Sprintf("X-Response-%d", i), "v")
		}
		w.Write(responseBody)
	}), nil)

	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", bytes.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	for i := range 200 {
		req.Header.Set(fmt.Sprintf("X-Request-%d", i), "v")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server, ok := h.awaitServer(t)["chat"]
	if !ok {
		t.Fatalf("no SERVER span: %v", spanNames(h))
	}
	if got := server.DroppedAttributes(); got != 0 {
		t.Errorf("%d attributes were dropped by the span limit (kept %d)", got, len(server.Attributes()))
	}
	if !mustAttr(t, server, AttrTraceRequestHeaderLimited).AsBool() {
		t.Error("want the request headers marked as limited")
	}
	if !mustAttr(t, server, AttrTraceResponseHeaderLimited).AsBool() {
		t.Error("want the response headers marked as limited")
	}
	headers := 0
	for _, kv := range server.Attributes() {
		if strings.HasPrefix(string(kv.Key), "http.request.header.") || strings.HasPrefix(string(kv.Key), "http.response.header.") {
			headers++
		}
	}
	if headers > 2*maxHeaderAttributes {
		t.Errorf("header attributes = %d, want at most %d", headers, 2*maxHeaderAttributes)
	}
}

func Test_上限を超えるモデル名が固定値に置換されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), nil)

	huge := strings.Repeat("m", MaxModelLength+1)
	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"`+huge+`"}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	for _, s := range h.spans.Ended() {
		if len(s.Name()) > MaxModelLength+64 {
			t.Errorf("span name is unbounded: %d chars", len(s.Name()))
		}
		if v, ok := attrOf(s, AttrRequestModel); ok && len(v.AsString()) > MaxModelLength {
			t.Errorf("model attribute is unbounded: %d chars", len(v.AsString()))
		}
	}
}

func Test_POST以外のリクエストが計装されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}), nil)

	req, _ := http.NewRequest("PROPFIND", h.proxy.URL+"/v1/chat/completions", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	if n := len(h.spans.Started()); n != 0 {
		t.Errorf("got %d spans for a non-POST request: %v", n, spanNames(h))
	}
}

func Test_remote_parentのsampling判断でlocalの判断が上書きされていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}), tracing.SamplerForRatio(1))

	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`))
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	waitForSpan(t, h.spans, trace.SpanKindServer)
	if serverSpan(h.spans) == nil {
		t.Fatal("an unsampled traceparent must not be able to erase the request")
	}
}

func waitForSpan(t *testing.T, rec *tracetest.SpanRecorder, kind trace.SpanKind) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range rec.Ended() {
			if s.SpanKind() == kind {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %v span within 10s", kind)
}

func serverSpan(rec *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			return s
		}
	}
	return nil
}

func Test_ResponsesAPIの完了イベントでstreamがcompleteになっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"type":"response.completed","response":{"model":"served-model","usage":{"input_tokens":41,"output_tokens":8}},"timings":{"prompt_ms":205.7}}`+"\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if v, ok := attrOf(server, AttrResponseIncomplete); ok && v.AsBool() {
		t.Error("a response.completed event terminates the stream; it is not incomplete")
	}
	if v := mustAttr(t, server, AttrInputTokens); v.AsInt64() != 41 {
		t.Errorf("input_tokens = %d, want 41", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrOutputTokens); v.AsInt64() != 8 {
		t.Errorf("output_tokens = %d, want 8", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrTimingsPromptMS); v.AsFloat64() != 205.7 {
		t.Errorf("prompt_ms = %v", v.AsFloat64())
	}
	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	if got := mustAttr(t, responses[0], AttrResponseBodyContent).AsString(); got != "hi" {
		t.Errorf("assembled response = %q", got)
	}
	if got := mustAttr(t, responses[0], AttrOutputIndex).AsInt64(); got != 0 {
		t.Errorf("output index = %d", got)
	}
	if _, ok := attrOf(responses[0], AttrResponseIncomplete); ok {
		t.Error("completed response output was marked incomplete")
	}
}

func Test_ResponsesAPIの未完了イベントで組み立て途中のresponseが記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"partial"}`+"\n\n")
		io.WriteString(w, `data: {"type":"response.incomplete","response":{"status":"incomplete"}}`+"\n\n")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeIncomplete) {
		t.Errorf("server outcome = %q", got)
	}
	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	response := responses[0]
	if got := mustAttr(t, response, AttrResponseBodyContent).AsString(); got != "partial" {
		t.Errorf("assembled response = %q", got)
	}
	if !mustAttr(t, response, AttrResponseIncomplete).AsBool() {
		t.Error("response output was not marked incomplete")
	}
	if response.Status().Code != codes.Error {
		t.Errorf("response status = %v", response.Status())
	}
}

func Test_ResponsesAPIのtool失敗後に終端なしでEOFになってもtoolの失敗が保持されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"type":"response.output_item.added","output_index":0,"item":{"id":"mcp_1","type":"mcp_call","name":"lookup"}}`+"\n\n")
		io.WriteString(w, `data: {"type":"response.mcp_call.failed","item_id":"mcp_1","output_index":0,"message":"tool failed"}`+"\n\n")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeIncomplete) {
		t.Errorf("server outcome = %q, want %q", got, OutcomeIncomplete)
	}
	tools := h.endedByName(SpanToolCall)
	if len(tools) != 1 {
		t.Fatalf("tool spans = %d, want 1", len(tools))
	}
	tool := tools[0]
	if got := mustAttr(t, tool, AttrOutcome).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("tool outcome = %q, want %q", got, OutcomeUpstreamError)
	}
	if got := mustAttr(t, tool, AttrUpstreamError).AsString(); got != "tool failed" {
		t.Errorf("tool error = %q", got)
	}
	if got := mustAttr(t, tool, AttrErrorType).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("tool error.type = %q", got)
	}
	if tool.Status().Code != codes.Error {
		t.Errorf("tool status = %v, want error", tool.Status())
	}
}

func Test_ResponsesAPIの完了snapshotにあるobject形式のtool引数が記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"type":"response.completed","response":{"status":"completed","output":[{"id":"search_1","type":"tool_search_call","status":"completed","arguments":{"query":"weather","limit":2}}]}}`+"\n\n")
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("server outcome = %q, want %q", got, OutcomeSuccess)
	}
	tools := h.endedByName(SpanToolCall)
	if len(tools) != 1 {
		t.Fatalf("tool spans = %d, want 1", len(tools))
	}
	tool := tools[0]
	if got := mustAttr(t, tool, AttrResponseBodyArguments).AsString(); got != `{"query":"weather","limit":2}` {
		t.Errorf("tool arguments = %q", got)
	}
	if got := mustAttr(t, tool, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("tool outcome = %q, want %q", got, OutcomeSuccess)
	}
}

func Test_ResponsesAPIのreasoningと複数partとtoolがoutput単位のspanになっている(t *testing.T) {
	responseBody := `data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","status":"in_progress","summary":[]}}` + "\n\n" +
		`data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":"考え"}` + "\n\n" +
		`data: {"type":"response.reasoning_summary_text.done","item_id":"rs_1","output_index":0,"summary_index":0,"text":"考えた"}` + "\n\n" +
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":"考えた"}]}}` + "\n\n" +
		`data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":1,"content_index":0,"delta":"Hello"}` + "\n\n" +
		`data: {"type":"response.output_text.done","item_id":"msg_1","output_index":1,"content_index":0,"text":"Hello"}` + "\n\n" +
		`data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":1,"content_index":1,"delta":" world"}` + "\n\n" +
		`data: {"type":"response.output_text.done","item_id":"msg_1","output_index":1,"content_index":1,"text":" world"}` + "\n\n" +
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","status":"completed","content":[{"type":"output_text","text":"Hello"},{"type":"output_text","text":" world"}]}}` + "\n\n" +
		`data: {"type":"response.output_item.added","output_index":2,"item":{"id":"fc_1","call_id":"call_1","type":"function_call","status":"in_progress","name":"lookup","arguments":""}}` + "\n\n" +
		`data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"{\"city\":\"Tokyo\"}"}` + "\n\n" +
		`data: {"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":2,"arguments":"{\"city\":\"Tokyo\"}"}` + "\n\n" +
		`data: {"type":"response.output_item.done","output_index":2,"item":{"id":"fc_1","call_id":"call_1","type":"function_call","status":"completed","name":"lookup","arguments":"{\"city\":\"Tokyo\"}"}}` + "\n\n" +
		`data: {"type":"response.output_item.added","output_index":3,"item":{"id":"ws_1","type":"web_search_call","status":"in_progress"}}` + "\n\n" +
		`data: {"type":"response.web_search_call.completed","item_id":"ws_1","output_index":3}` + "\n\n" +
		`data: {"type":"response.output_item.done","output_index":3,"item":{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"weather"}}}` + "\n\n" +
		`data: {"type":"response.completed","response":{"model":"served-model","usage":{"input_tokens":4,"output_tokens":7}}}` + "\n\n"
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, responseBody)
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	server := h.awaitServer(t)["chat"]

	reasoning := h.endedByName(SpanReasoning)
	responses := h.endedByName(SpanResponse)
	tools := h.endedByName(SpanToolCall)
	if len(reasoning) != 1 || len(responses) != 1 || len(tools) != 2 {
		t.Fatalf("reasoning=%d response=%d tools=%d: %v", len(reasoning), len(responses), len(tools), spanNames(h))
	}
	if got := mustAttr(t, reasoning[0], AttrResponseBodyReasoningContent).AsString(); got != "考えた" {
		t.Errorf("reasoning = %q", got)
	}
	if got := mustAttr(t, responses[0], AttrResponseBodyContent).AsString(); got != "Hello world" {
		t.Errorf("response = %q", got)
	}
	if _, ok := attrOf(responses[0], AttrContentIndex); ok {
		t.Error("multi-part response retained one content index")
	}
	for _, output := range append(append(reasoning, responses...), tools...) {
		if output.Parent().SpanID() != server.SpanContext().SpanID() {
			t.Errorf("%s is not a direct SERVER child", output.Name())
		}
	}
	toolByOutput := make(map[int64]sdktrace.ReadOnlySpan)
	for _, tool := range tools {
		toolByOutput[mustAttr(t, tool, AttrOutputIndex).AsInt64()] = tool
	}
	function := toolByOutput[2]
	if function == nil || mustAttr(t, function, AttrToolCallID).AsString() != "call_1" ||
		mustAttr(t, function, AttrResponseBodyName).AsString() != "lookup" ||
		mustAttr(t, function, AttrResponseBodyArguments).AsString() != `{"city":"Tokyo"}` {
		t.Errorf("function tool = %v", function)
	}
	builtIn := toolByOutput[3]
	if builtIn == nil || mustAttr(t, builtIn, AttrResponseBodyType).AsString() != "web_search_call" ||
		mustAttr(t, builtIn, AttrResponseBodyArguments).AsString() != `{"type":"search","query":"weather"}` {
		t.Errorf("built-in tool = %v", builtIn)
	}
	if got := len(h.spans.Ended()); got != 8 {
		t.Errorf("spans = %d, want 8: %v", got, spanNames(h))
	}
}

func Test_clientのHostヘッダーでUpstreamのHostが上書きされていない(t *testing.T) {
	type seen struct {
		host, xfHost, xfProto, xfFor string
	}
	got := make(chan seen, 1)
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{
			host:    r.Host,
			xfHost:  r.Header.Get("X-Forwarded-Host"),
			xfProto: r.Header.Get("X-Forwarded-Proto"),
			xfFor:   r.Header.Get("X-Forwarded-For"),
		}
		io.WriteString(w, nonStreamBody)
	}), nil)

	upstreamHost := strings.TrimPrefix(h.upstream.URL, "http://")
	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`))
	req.Host = "evil.example"
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	s := <-got
	if s.host != upstreamHost {
		t.Errorf("upstream saw Host %q, want the configured %q", s.host, upstreamHost)
	}
	if s.xfHost != "evil.example" {
		t.Errorf("X-Forwarded-Host = %q, want the original Host", s.xfHost)
	}
	if s.xfProto != "http" {
		t.Errorf("X-Forwarded-Proto = %q", s.xfProto)
	}
	if s.xfFor == "" {
		t.Error("want X-Forwarded-For to identify the client")
	}
}

func tokenStream(preambleDelay time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)

		io.WriteString(w, ": ping\n\n")
		fl.Flush()

		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n")
		fl.Flush()

		time.Sleep(preambleDelay)

		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"choices":[],"usage":{"prompt_tokens":6,"completion_tokens":8}}`+"\n\n")
		fl.Flush()
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
}

func Test_TimeToFirstTokenが最初のbyteではなく最初のtokenまでの時間になっている(t *testing.T) {
	const preamble = 300 * time.Millisecond
	h := newHarness(t, tokenStream(preamble), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	spans := h.ended(t)
	timeToFirstTokenSpan := spans[SpanTimeToFirstToken]
	if timeToFirstTokenSpan == nil {
		t.Fatal("no TimeToFirstToken span")
	}
	got := timeToFirstTokenSpan.EndTime().Sub(timeToFirstTokenSpan.StartTime())
	if got < preamble {
		t.Errorf("TimeToFirstToken = %v, but the first token only arrived after %v: "+
			"the span closed on the ping or the role-only chunk", got, preamble)
	}

	gen := spans[SpanGeneration]
	if gen == nil {
		t.Fatal("no generation span")
	}
	if !gen.StartTime().Equal(timeToFirstTokenSpan.EndTime()) {
		t.Errorf("generation starts at %v, TimeToFirstToken ends at %v", gen.StartTime(), timeToFirstTokenSpan.EndTime())
	}
}

func Test_stream本文内のエラーがspanに記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"error":{"code":500,"message":"context shift is disabled","type":"server_error"}}`+"\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	spans := h.ended(t)
	server := spans["chat"]
	if server.Status().Code != codes.Error {
		t.Errorf("SERVER status = %v, want error", server.Status())
	}
	if v, ok := attrOf(server, AttrUpstreamError); !ok || !strings.Contains(v.AsString(), "context shift") {
		t.Errorf("want the upstream message recorded, got %v", server.Attributes())
	}

	gen := spans[SpanGeneration]
	if gen == nil {
		t.Fatal("no generation span")
	}
	if gen.Status().Code != codes.Error {
		t.Errorf("generation status = %v, want error so it can be excluded from the latency histogram",
			gen.Status())
	}
	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	response := responses[0]
	if response.Status().Code != codes.Error {
		t.Errorf("response status = %v, want error", response.Status())
	}
	if got := mustAttr(t, response, AttrOutcome).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("response outcome = %q", got)
	}
	if got := mustAttr(t, response, AttrResponseBodyContent).AsString(); got != "partial" {
		t.Errorf("partial response = %q", got)
	}
	if got := mustAttr(t, response, AttrUpstreamError).AsString(); !strings.Contains(got, "context shift") {
		t.Errorf("upstream error = %q", got)
	}
	if got := mustAttr(t, response, AttrErrorType).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("response error.type = %q", got)
	}
}

func Test_generation中にcancelされたときphase_spanにcancelが記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 0; i < 200; i++ {
			if _, err := fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"%d\"}}]}\n\n", i); err != nil {
				return
			}
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	res.Body.Read(buf)
	cancel()
	res.Body.Close()

	waitFor(t, func() bool { return h.ended(t)[SpanGeneration] != nil })
	gen := h.ended(t)[SpanGeneration]
	if v := mustAttr(t, gen, AttrOutcome); v.AsString() != string(OutcomeClientCancel) {
		t.Errorf("generation outcome = %q, want %q", v.AsString(), OutcomeClientCancel)
	}
	if gen.Status().Code == codes.Error {
		t.Error("a client cancellation is not an error")
	}
}

func Test_native_completionのstopでstreamがcompleteになっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"content\":\"2\",\"stop\":false}\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"content":"","stop":true,"model":"/models/qwen.gguf","tokens_predicted":5,`+
			`"timings":{"prompt_ms":12.5,"predicted_ms":80.5}}`+"\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/completion", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["text_completion"]
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if v, ok := attrOf(server, AttrResponseIncomplete); ok && v.AsBool() {
		t.Error("stop:true terminates a native stream; it is not incomplete")
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v, want unset", server.Status())
	}
	if v := mustAttr(t, server, AttrTimingsPromptMS); v.AsFloat64() != 12.5 {
		t.Errorf("prompt_ms = %v", v.AsFloat64())
	}
	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	if got := mustAttr(t, responses[0], AttrResponseBodyContent).AsString(); got != "2" {
		t.Errorf("assembled response = %q", got)
	}
	if _, ok := attrOf(responses[0], AttrResponseIncomplete); ok {
		t.Error("native response was marked incomplete")
	}
}

func Test_Upstreamが5xxを返したときCLIENT_spanがerrorになっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}), nil)

	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	client := h.awaitServer(t)["chat m1"]
	if client == nil {
		t.Fatal("no client span")
	}
	if client.Status().Code != codes.Error {
		t.Errorf("CLIENT status = %v, want error for an upstream 500", client.Status())
	}
}

func Test_drain中にclientがcancelしたときshutdownとして記録されていない(t *testing.T) {
	started := make(chan struct{})
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fl.Flush()
		close(started)
		<-r.Context().Done()
	}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")

	done := make(chan struct{})
	go func() {
		defer close(done)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}()

	<-started
	h.handler.BeginDrain()
	cancel()
	<-done

	server := h.awaitServer(t)["chat"]
	if v := mustAttr(t, server, AttrOutcome); v.AsString() != string(OutcomeClientCancel) {
		t.Errorf("outcome = %q, want %q: Server.Shutdown waits for in-flight requests "+
			"rather than cutting them, so a cancel during the drain is still the client's",
			v.AsString(), OutcomeClientCancel)
	}
	if _, ok := attrOf(server, AttrShutdownInterrupted); ok {
		t.Error("a graceful drain must not claim the shutdown cut this request")
	}
}

func Test_強制終了で中断されたリクエストがshutdownとして記録されている(t *testing.T) {
	started := make(chan struct{})
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fl.Flush()
		close(started)
		<-r.Context().Done()
	}), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")

	done := make(chan struct{})
	go func() {
		defer close(done)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}()

	<-started
	h.handler.BeginDrain()
	h.handler.BeginForcedClose()
	cancel()
	<-done

	server := h.awaitServer(t)["chat"]
	if v := mustAttr(t, server, AttrOutcome); v.AsString() != string(OutcomeShutdown) {
		t.Errorf("outcome = %q, want %q", v.AsString(), OutcomeShutdown)
	}
	if server.Status().Code == codes.Error {
		t.Error("a restart is not an error")
	}
}
func (panickingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(&brokenReader{}),
		Request:    r,
	}, nil
}

type panickingTransport struct{}

func Test_handlerがpanicしてもSERVER_spanが終了している(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	px := httptest.NewServer(New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: panickingTransport{},
	}))
	defer px.Close()

	res, err := http.Post(px.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	waitForSpan(t, rec, trace.SpanKindServer)
	if serverSpan(rec) == nil {
		t.Fatal("the SERVER span was lost: ReverseProxy aborts a failed body copy " +
			"with panic(http.ErrAbortHandler), so the span can only be ended from a defer")
	}
}

func Test_requestStateのobserve内でpanicしてもspanが終了している(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "chat m1", trace.WithSpanKind(trace.SpanKindServer))
	st := &requestState{span: span, ctx: ctx, tracer: tracer, start: time.Now(), reqCtx: context.Background()}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("want the panic to propagate to net/http")
			}
		}()
		st.observe(func() { panic(http.ErrAbortHandler) })
	}()

	if len(rec.Ended()) != 1 {
		t.Fatalf("got %d ended spans, want 1", len(rec.Ended()))
	}
}

func gzipServerSentEventsUpstream() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
			fl.Flush()
			io.WriteString(w, `data: {"usage":{"prompt_tokens":6,"completion_tokens":8}}`+"\n\n")
			fl.Flush()
			io.WriteString(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		io.WriteString(zw, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		io.WriteString(zw, `data: {"usage":{"prompt_tokens":6,"completion_tokens":8}}`+"\n\n")
		io.WriteString(zw, "data: [DONE]\n\n")
		zw.Close()
	})
}

// A client that asks for gzip must not blind the instrumentation. Go's
// Transport only decompresses what it requested itself, and ReverseProxy
// forwards the client's Accept-Encoding, so the observer would otherwise be fed
// compressed bytes and silently report a stream with no tokens and no usage.
func Test_clientがgzip対応でもresponseのusageが取得されている(t *testing.T) {
	h := newHarness(t, gzipServerSentEventsUpstream(), nil)

	req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if v := mustAttr(t, server, AttrServerSentEventsChunkCount); v.AsInt64() != 2 {
		t.Errorf("Server-Sent Events chunk count = %d, want 2", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrInputTokens); v.AsInt64() != 6 {
		t.Errorf("input_tokens = %d", v.AsInt64())
	}
	if v := mustAttr(t, server, AttrOutcome); v.AsString() != string(OutcomeSuccess) {
		t.Errorf("outcome = %q, want success", v.AsString())
	}
}

// If an encoding still comes back, nothing is parsed and the fact is recorded,
// rather than reporting a stream with zero tokens as though it were real.
func Test_解析できないContentEncodingがresponse_encodingに記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "br")
		io.WriteString(w, "\x1b\x0a\x00\x00compressed")
	}), nil)

	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if v := mustAttr(t, server, AttrResponseEncoding); v.AsString() != "br" {
		t.Errorf("response_encoding = %q, want br", v.AsString())
	}
	if _, ok := attrOf(server, AttrInputTokens); ok {
		t.Error("nothing can be parsed out of an unsupported encoding")
	}
}

// A tool-call-only response generates no text, but it is still a generation and
// still has a first token.
func Test_tool_callだけのstreamでもTimeToFirstTokenが記録されている(t *testing.T) {
	const delay = 200 * time.Millisecond
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		fl.Flush()
		time.Sleep(delay)
		io.WriteString(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`+"\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"Tokyo\"}"}}]}}]}`+"\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":11}}`+"\n\n")
		fl.Flush()
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	h.awaitServer(t)

	spans := h.ended(t)
	timeToFirstTokenSpan := spans[SpanTimeToFirstToken]
	if timeToFirstTokenSpan == nil {
		t.Fatal("a tool-call-only stream still has a first token")
	}
	if got := timeToFirstTokenSpan.EndTime().Sub(timeToFirstTokenSpan.StartTime()); got < delay {
		t.Errorf("TimeToFirstToken = %v, want at least %v: it closed before the tool call arrived", got, delay)
	}
	if spans[SpanGeneration] == nil {
		t.Error("want a generation span")
	}
}

// A stream that stops cleanly without its terminator is a failed request, not a
// success with an extra attribute.
func Test_終端なしでEOFになったstreamがerrorとして記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		w.(http.Flusher).Flush()
	}), nil)

	res, _ := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	spans := h.awaitServer(t)
	server := spans["chat"]
	if server.Status().Code != codes.Error {
		t.Errorf("SERVER status = %v, want error", server.Status())
	}
	if v := mustAttr(t, server, AttrOutcome); v.AsString() != string(OutcomeIncomplete) {
		t.Errorf("outcome = %q, want %q", v.AsString(), OutcomeIncomplete)
	}
	if gen := spans[SpanGeneration]; gen == nil || gen.Status().Code != codes.Error {
		t.Error("the generation span must be excludable from the success histogram too")
	}
	responses := h.endedByName(SpanResponse)
	if len(responses) != 1 {
		t.Fatalf("response spans = %d, want 1", len(responses))
	}
	response := responses[0]
	if got := mustAttr(t, response, AttrResponseBodyContent).AsString(); got != "hi" {
		t.Errorf("response = %q, want hi", got)
	}
	if got := mustAttr(t, response, AttrOutcome).AsString(); got != string(OutcomeIncomplete) {
		t.Errorf("response outcome = %q, want %q", got, OutcomeIncomplete)
	}
	if !mustAttr(t, response, AttrResponseIncomplete).AsBool() {
		t.Error("partial response was not marked incomplete")
	}
	if response.Status().Code != codes.Error {
		t.Errorf("response status = %v, want error", response.Status())
	}
}

// An unexpected panic is a failure, not a span that merely exists.
func Test_想定外のpanicがinternal_errorとして記録されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("test").Start(context.Background(), "chat m1",
		trace.WithSpanKind(trace.SpanKindServer))
	st := &requestState{span: span, ctx: ctx, tracer: tp.Tracer("test"),
		start: time.Now(), reqCtx: context.Background()}

	func() {
		defer func() { recover() }()
		st.observe(func() { panic("unexpected invariant violation") })
	}()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("got %d spans", len(ended))
	}
	if ended[0].Status().Code != codes.Error {
		t.Errorf("status = %v, want error", ended[0].Status())
	}
	if len(ended[0].Events()) == 0 {
		t.Error("want the panic recorded as an error event")
	}
}

// The upstream must be reached directly: an ambient HTTP_PROXY would receive
// the prompts and any Authorization header meant for llama-swap.
func Test_ProxyのTransportに環境変数のproxyが使われていない(t *testing.T) {
	u, _ := url.Parse("http://llama-swap.internal:8080")
	h := New(Options{Upstream: u})
	rt, ok := h.proxy.Transport.(*proxyTransport)
	if !ok {
		t.Fatalf("transport = %T", h.proxy.Transport)
	}
	base, ok := rt.base.(*http.Transport)
	if !ok {
		t.Fatalf("base transport = %T", rt.base)
	}
	if base.Proxy != nil {
		t.Error("the upstream transport must not follow HTTP_PROXY")
	}
}

// With trust turned off, a caller can no longer choose this proxy's trace ID —
// which is what lets it merge unrelated requests into one trace, inject spans
// into a known trace, or steer a tail sampler. The incoming context is kept as
// a link so the two traces can still be associated.
func Test_trace_contextを信頼しないときlocal_rootのspanが開始されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, nonStreamBody)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	px := httptest.NewServer(New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: false,
	}))
	defer px.Close()

	const incoming = "00-11111111111111111111111111111111-2222222222222222-01"
	req, _ := http.NewRequest(http.MethodPost, px.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`))
	req.Header.Set("traceparent", incoming)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	waitForSpan(t, rec, trace.SpanKindServer)

	server := serverSpan(rec)
	if got := server.SpanContext().TraceID().String(); got == "11111111111111111111111111111111" {
		t.Error("the caller's trace id was adopted despite trust being off")
	}
	if server.Parent().IsValid() {
		t.Error("want a local root")
	}
	links := server.Links()
	if len(links) != 1 {
		t.Fatalf("got %d links, want the incoming context kept as one", len(links))
	}
	if got := links[0].SpanContext.TraceID().String(); got != "11111111111111111111111111111111" {
		t.Errorf("link trace id = %s", got)
	}
}

// A dropped terminal event is a gap in what llamaproxy could observe, not proof
// that the upstream failed. Reporting it as an incomplete stream would turn a
// healthy long generation into an error.
func Test_長大な終端イベントが破棄されたときupstream_failureとして記録されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"type":"response.completed","response":{"output":"`+
			strings.Repeat("x", serversentevents.MaxLine)+`"}}`+"\n\n")
		fl.Flush()
	}), nil)

	res, err := http.Post(h.proxy.URL+"/v1/responses", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	server := h.awaitServer(t)["chat"]
	if server == nil {
		t.Fatalf("no server span: %v", spanNames(h))
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v: a terminal event too large to parse is not an upstream failure",
			server.Status())
	}
	if v, ok := attrOf(server, AttrResponseBodyTruncated); !ok || !v.AsBool() {
		t.Error("want the gap recorded as a truncated body")
	}
	if v := mustAttr(t, server, AttrServerSentEventsDroppedLineCount); v.AsInt64() != 1 {
		t.Errorf("Server-Sent Events dropped line count = %d, want 1", v.AsInt64())
	}
}

type failingWriter struct {
	header http.Header
	status int
}

func (f *failingWriter) Header() http.Header {
	if f.header == nil {
		f.header = http.Header{}
	}
	return f.header
}
func (f *failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (f *failingWriter) WriteHeader(status int)    { f.status = status }
func (f *failingWriter) Flush()                    {}

// When the client goes away, the failure can surface as a write error rather
// than as a cancelled context, and which one arrives first is up to the
// scheduler. Either way the request must not be recorded as a success.
func Test_downstreamへの書き込みが失敗したときsuccessとして記録されていない(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 0; i < 20; i++ {
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
			fl.Flush()
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	req.Header.Set("Content-Type", "application/json")

	func() {
		// ReverseProxy aborts the handler when the copy fails.
		defer func() { recover() }()
		h.ServeHTTP(&failingWriter{}, req)
	}()

	waitForSpan(t, rec, trace.SpanKindServer)
	server := serverSpan(rec)
	if server == nil {
		t.Fatal("no server span")
	}
	if v := mustAttr(t, server, AttrOutcome); v.AsString() == string(OutcomeSuccess) {
		t.Error("a response that never reached the client is not a success")
	}
}
