package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func Test_レスポンス開始前に強制終了したとき503とshutdownのspanが返されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	transport := &cancellationTransport{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: transport,
	})
	px := httptest.NewServer(h)
	defer px.Close()

	type answer struct {
		status int
		body   string
		err    error
	}
	got := make(chan answer, 1)
	go func() {
		res, err := http.Post(px.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"m1","stream":true}`))
		if err != nil {
			got <- answer{err: err}
			return
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		got <- answer{status: res.StatusCode, body: string(body)}
	}()

	<-transport.started
	h.BeginForcedClose()
	<-transport.canceled
	close(transport.release)

	a := <-got
	if a.err != nil {
		t.Fatalf("request failed: %v", a.err)
	}
	if a.status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %q, want 503", a.status, a.body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	var server sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			server = span
		}
	}
	if server == nil {
		t.Fatal("no SERVER span")
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeShutdown) {
		t.Errorf("outcome = %q, want %q", got, OutcomeShutdown)
	}
	if got := mustAttr(t, server, AttrStatusCode).AsInt64(); got != http.StatusServiceUnavailable {
		t.Errorf("http.response.status_code = %d, want 503", got)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v, want unset", server.Status())
	}
}

func Test_passthroughのstreamが上限を占有しても計装対象リクエストが処理されている(t *testing.T) {
	hold := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/logs/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case <-hold:
			case <-r.Context().Done():
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, nonStreamBody)
	}))
	defer upstream.Close()
	defer close(hold)

	u, _ := url.Parse(upstream.URL)
	px := httptest.NewServer(New(Options{Upstream: u, MaxConcurrentRequests: 2}))
	defer px.Close()

	client := &http.Client{}
	streams := make([]*http.Response, 0, 4)
	defer func() {
		for _, res := range streams {
			_ = res.Body.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		res, err := client.Get(px.URL + "/logs/stream")
		if err != nil {
			t.Fatalf("log stream %d: %v", i, err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("log stream %d: status = %d", i, res.StatusCode)
		}
		streams = append(streams, res)
	}

	res, err := client.Post(px.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("inference status = %d body = %q, want 200", res.StatusCode, body)
	}
}

func Test_passthroughの同時実行上限が計装対象とは別に適用されている(t *testing.T) {
	hold := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-hold:
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	defer close(hold)

	u, _ := url.Parse(upstream.URL)
	px := httptest.NewServer(New(Options{
		Upstream: u, MaxConcurrentRequests: 8, MaxConcurrentPassthroughRequests: 1,
	}))
	defer px.Close()

	client := &http.Client{}
	first, err := client.Get(px.URL + "/logs/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first stream status = %d", first.StatusCode)
	}

	second, err := client.Get(px.URL + "/logs/stream")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, second.Body)
	_ = second.Body.Close()
	if second.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second stream status = %d, want 503", second.StatusCode)
	}
	if second.Header.Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q", second.Header.Get("Retry-After"))
	}
}
