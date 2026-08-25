package proxy

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/genai"
)

type countingCloser struct {
	io.Reader
	closes int
}

func (c *countingCloser) Close() error { c.closes++; return nil }

func Test_EOF後にCloseしてもbodyObserverの完了処理が1回だけ呼ばれている(t *testing.T) {
	inner := &countingCloser{Reader: strings.NewReader(nonStreamBody)}
	var done int
	b := &bodyObserver{inner: inner, onDone: func(bodyResult) { done++ }}

	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	if done != 1 {
		t.Fatalf("done = %d after EOF, want 1", done)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if done != 1 {
		t.Errorf("done = %d after Close, want it to stay 1", done)
	}
	if inner.closes != 1 {
		t.Errorf("inner closes = %d", inner.closes)
	}
}

func Test_EOF前にCloseしたときbodyObserverの完了処理が呼ばれている(t *testing.T) {
	inner := &countingCloser{Reader: strings.NewReader(nonStreamBody)}
	var res bodyResult
	var done int
	b := &bodyObserver{inner: inner, onDone: func(r bodyResult) { done++; res = r }}

	buf := make([]byte, 4)
	if _, err := b.Read(buf); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if done != 1 {
		t.Fatalf("done = %d, want 1", done)
	}
	if res.FirstByte.IsZero() || res.LastByte.IsZero() {
		t.Error("want first/last byte timestamps")
	}
}

type erroringReader struct{ n int }

func (e *erroringReader) Read(p []byte) (int, error) {
	if e.n == 0 {
		e.n++
		copy(p, "x")
		return 1, nil
	}
	return 0, errors.New("connection reset")
}
func (e *erroringReader) Close() error { return nil }

func Test_本文の読み込みエラー時にbodyObserverの完了処理へエラーが渡されている(t *testing.T) {
	var done int
	b := &bodyObserver{inner: &erroringReader{}, onDone: func(bodyResult) { done++ }}
	if _, err := io.ReadAll(b); err == nil {
		t.Fatal("want an error")
	}
	if done != 1 {
		t.Errorf("done = %d, want 1", done)
	}
}

func Test_非ストリーミング本文が収集上限を超えたときtruncatedになっている(t *testing.T) {
	big := strings.Repeat("z", maxCollectedBodySize+100)
	var res bodyResult
	b := &bodyObserver{inner: io.NopCloser(strings.NewReader(big)), onDone: func(r bodyResult) { res = r }}
	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	if !res.Truncated {
		t.Error("want Truncated")
	}
	if len(res.Payload) != 0 {
		t.Errorf("a truncated body must not be handed on for parsing (%d bytes)", len(res.Payload))
	}
}

func Test_空の本文を読み終えたとき空のbodyResultが通知されている(t *testing.T) {
	var res bodyResult
	var done int
	b := &bodyObserver{inner: io.NopCloser(strings.NewReader("")), onDone: func(r bodyResult) { done++; res = r }}
	io.ReadAll(b)
	if done != 1 {
		t.Fatalf("done = %d", done)
	}
	if !res.FirstByte.IsZero() {
		t.Error("an empty body has no first byte")
	}
}

func Test_空行なしのChatServerSentEventsがEOFで未完了出力として通知されている(t *testing.T) {
	var res bodyResult
	var emitted int
	b := newBodyObserver(
		io.NopCloser(strings.NewReader(`data: {"choices":[{"index":0,"delta":{"content":"hello"}}]}`)),
		true,
		func(r bodyResult) { res = r },
	)
	b.onStreamOutput = func(genai.StreamOutput) { emitted++ }

	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	if emitted != 0 {
		t.Errorf("emitted outputs = %d, want 0", emitted)
	}
	if len(res.StreamOutputs) != 1 {
		t.Fatalf("stream outputs = %d, want 1", len(res.StreamOutputs))
	}
	output := res.StreamOutputs[0]
	if output.Kind != genai.StreamOutputResponse {
		t.Errorf("kind = %v, want response", output.Kind)
	}
	if output.Content != "hello" {
		t.Errorf("content = %q, want hello", output.Content)
	}
	if output.Complete {
		t.Error("output is complete, want incomplete")
	}
	if !output.HasChoiceIndex || output.ChoiceIndex != 0 {
		t.Errorf("choice index = (%v, %d), want (true, 0)", output.HasChoiceIndex, output.ChoiceIndex)
	}
	if output.Start.IsZero() || output.End.IsZero() {
		t.Error("want output timestamps")
	}
}

func Test_EOF前のCloseで未完ServerSentEventsフレームから出力が通知されていない(t *testing.T) {
	const frame = `data: {"choices":[{"index":0,"delta":{"content":"hello"}}]}`
	var res bodyResult
	var emitted int
	b := newBodyObserver(io.NopCloser(strings.NewReader(frame)), true, func(r bodyResult) { res = r })
	b.onStreamOutput = func(genai.StreamOutput) { emitted++ }
	buf := make([]byte, len(frame))
	if _, err := b.Read(buf); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if emitted != 0 {
		t.Errorf("emitted outputs = %d, want 0", emitted)
	}
	if len(res.StreamOutputs) != 0 {
		t.Errorf("stream outputs = %d, want 0", len(res.StreamOutputs))
	}
}

func Test_不正なServerSentEventsイベント後はTimeToFirstTokenを確定せず出力を観測欠落としている(t *testing.T) {
	body := "data: {\"choices\":\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	var res bodyResult
	var tokens int
	var outputs []genai.StreamOutput
	b := newBodyObserver(io.NopCloser(strings.NewReader(body)), true, func(r bodyResult) { res = r })
	b.onTimeToFirstToken = func(time.Time) { tokens++ }
	b.onStreamOutput = func(output genai.StreamOutput) { outputs = append(outputs, output) }
	if _, err := io.ReadAll(b); err != nil {
		t.Fatal(err)
	}
	if tokens != 0 {
		t.Errorf("TimeToFirstToken callbacks = %d, want 0", tokens)
	}
	if !res.StreamLimited {
		t.Error("stream observation was not marked limited")
	}
	if len(outputs) != 1 || !outputs[0].ObservationLimited || outputs[0].Content != "hello" {
		t.Errorf("outputs = %+v", outputs)
	}
}

func Test_完了後のServerSentEvents出力でTimeToFirstTokenを確定していない(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "Chat choice",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"late\"}}]}\n\n" +
				"data: [DONE]\n\n",
		},
		{
			name: "native response",
			body: "data: {\"content\":\"\",\"stop\":true}\n\n" +
				"data: {\"content\":\"late\",\"stop\":false}\n\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var res bodyResult
			var tokens int
			var outputs int
			b := newBodyObserver(io.NopCloser(strings.NewReader(tt.body)), true, func(r bodyResult) { res = r })
			b.onTimeToFirstToken = func(time.Time) { tokens++ }
			b.onStreamOutput = func(genai.StreamOutput) { outputs++ }
			if _, err := io.ReadAll(b); err != nil {
				t.Fatal(err)
			}
			if tokens != 0 || outputs != 0 || !res.FirstToken.IsZero() || !res.SawDone {
				t.Errorf("tokens=%d outputs=%d first=%v terminal=%v", tokens, outputs, res.FirstToken, res.SawDone)
			}
		})
	}
}

func Test_応答モデルのファイルパスからモデル識別子を取り出している(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"served-model", "served-model"},
		{"/var/lib/models/Qwen3.8-27B-UD-Q8_K_XL.gguf", "Qwen3.8-27B-UD-Q8_K_XL"},
		{`C:\models\Qwen3.gguf`, "Qwen3"},
		{"Qwen3.8-27B-UD-Q8_K_XL.gguf", "Qwen3.8-27B-UD-Q8_K_XL"},
		{"/var/lib/models/", ""},
		{"", ""},
	} {
		if got := responseModelIdentifier(c.in); got != c.want {
			t.Errorf("responseModelIdentifier(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
