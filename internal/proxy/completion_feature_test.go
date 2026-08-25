package proxy

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func streamOutcome(t *testing.T, body string) (map[string]sdktrace.ReadOnlySpan, *harness) {
	t.Helper()
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return h.awaitServer(t), h
}

func Test_終端イベントの末尾に空行がなくてもresponseがcompleteになっている(t *testing.T) {
	spans, _ := streamOutcome(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
			"data: {\"stop\":true,\"usage\":{\"completion_tokens\":3}}")

	server := spans["chat"]
	if server == nil {
		t.Fatal("no SERVER span")
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("outcome = %q, want %q", got, OutcomeSuccess)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v, want no error", server.Status())
	}
	if got := mustAttr(t, server, AttrServerSentEventsChunkCount).AsInt64(); got != 2 {
		t.Errorf("Server-Sent Events chunk count = %d, want 2", got)
	}
	if got := mustAttr(t, server, AttrOutputTokens).AsInt64(); got != 3 {
		t.Errorf("output_tokens = %d, want 3", got)
	}
}

func Test_終端イベントが途中で切れたときresponseがincompleteになっている(t *testing.T) {
	spans, _ := streamOutcome(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
			"data: {\"stop\":true,\"usa")

	server := spans["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeIncomplete) {
		t.Errorf("outcome = %q, want %q", got, OutcomeIncomplete)
	}
	if _, ok := attrOf(server, AttrOutputTokens); ok {
		t.Error("a truncated payload must not contribute usage")
	}
}

func Test_複数data行の終端イベントが1chunkとして記録されている(t *testing.T) {
	spans, _ := streamOutcome(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
			"data: {\"stop\":true,\n"+
			"data: \"usage\":{\"completion_tokens\":3}}\n\n")

	server := spans["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("outcome = %q, want %q", got, OutcomeSuccess)
	}
	if got := mustAttr(t, server, AttrServerSentEventsChunkCount).AsInt64(); got != 2 {
		t.Errorf("Server-Sent Events chunk count = %d, want 2", got)
	}
	if got := mustAttr(t, server, AttrOutputTokens).AsInt64(); got != 3 {
		t.Errorf("output_tokens = %d, want 3", got)
	}
}

func Test_DONEの後に空行がなくてもresponseがcompleteになっている(t *testing.T) {
	spans, _ := streamOutcome(t,
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n")

	server := spans["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("outcome = %q, want %q", got, OutcomeSuccess)
	}
}
