package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func Test_Upstreamのstatus記録後にpanicしたときoutcomeとerror_typeがpanicとして記録されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("test").Start(context.Background(), "server",
		trace.WithSpanKind(trace.SpanKindServer))
	st := &requestState{span: span, ctx: ctx, tracer: tp.Tracer("test"),
		start: time.Now(), reqCtx: context.Background()}
	st.responseStarted(http.StatusInternalServerError, false, "")

	func() {
		defer func() { _ = recover() }()
		st.observe(func() { panic(errors.New("boom")) })
	}()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d", len(ended))
	}
	got := ended[0]
	if value := mustAttr(t, got, AttrOutcome).AsString(); value != string(OutcomeInternalError) {
		t.Fatalf("outcome = %q", value)
	}
	if value := mustAttr(t, got, AttrErrorType).AsString(); value != "panic" {
		t.Errorf("error.type = %q, want \"panic\" to match the outcome", value)
	}
}

type deadlineTransport struct{}

func (deadlineTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return nil, &url.Error{Op: "Post", URL: r.URL.String(), Err: context.DeadlineExceeded}
}

func Test_Upstreamがdeadlineを超えたときSERVERとCLIENTのspanがエラーになっている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	h := New(Options{
		Upstream:       u,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		TracerProvider: tp,
		Transport:      deadlineTransport{},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want %d", w.Code, http.StatusGatewayTimeout)
	}

	var server, client sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindServer:
			server = s
		case trace.SpanKindClient:
			client = s
		}
	}
	if server == nil || client == nil {
		t.Fatalf("server=%v client=%v", server != nil, client != nil)
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("SERVER outcome = %q, want %q", got, OutcomeUpstreamError)
	}
	if got := mustAttr(t, server, AttrErrorType).AsString(); got != "504" {
		t.Errorf("SERVER error.type = %q, want \"504\"", got)
	}

	if got := mustAttr(t, client, AttrOutcome).AsString(); got != string(OutcomeUpstreamError) {
		t.Errorf("CLIENT outcome = %q, want %q", got, OutcomeUpstreamError)
	}
	if got := mustAttr(t, client, AttrErrorType).AsString(); got != "transport_error" {
		t.Errorf("CLIENT error.type = %q, want \"transport_error\"", got)
	}
	if client.Status().Code != codes.Error {
		t.Errorf("CLIENT status = %v, want error", client.Status())
	}
}
