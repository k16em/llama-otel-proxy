package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/k16em/llama-otel-proxy/internal/config"
	"github.com/k16em/llama-otel-proxy/internal/serversentevents"
	"github.com/k16em/llama-otel-proxy/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func Test_終了したすべてのspanにoutcome属性が記録されている(t *testing.T) {
	h := newHarness(t, serverSentEventsUpstream(2, time.Millisecond), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	h.awaitServer(t)
	for _, span := range h.spans.Ended() {
		if _, ok := attrOf(span, AttrOutcome); !ok {
			t.Errorf("span %q has no outcome: %v", span.Name(), span.Attributes())
		}
	}
}

func Test_spanのoutcomeとstatusとerror_typeの整合性が保たれている(t *testing.T) {
	tests := []struct {
		name     string
		upstream http.Handler
		stream   bool
	}{
		{
			name: "success",
			upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, nonStreamBody)
			}),
		},
		{
			name: "http error",
			upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "boom", http.StatusInternalServerError)
			}),
		},
		{
			name: "incomplete stream",
			upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
			}),
			stream: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.upstream, nil)
			body := `{"model":"m1"}`
			if tt.stream {
				body = `{"model":"m1","stream":true}`
			}
			res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
			h.awaitServer(t)
			for _, span := range h.spans.Ended() {
				outcome := mustAttr(t, span, AttrOutcome).AsString()
				if span.Status().Code == codes.Error {
					if outcome == string(OutcomeSuccess) {
						t.Errorf("span %q has Error status with success outcome", span.Name())
					}
					if _, ok := attrOf(span, AttrErrorType); !ok {
						t.Errorf("span %q has Error status without error.type", span.Name())
					}
				}
				failedOutcome := outcome == string(OutcomeUpstreamError) ||
					outcome == string(OutcomeIncomplete) || outcome == string(OutcomeInternalError)
				if failedOutcome && span.Status().Code != codes.Error {
					t.Errorf("span %q has failed outcome %q without Error status", span.Name(), outcome)
				}
			}
		})
	}
}

func Test_UpstreamのHTTPエラーがSERVERとCLIENTのspanに一貫して記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	spans := h.awaitServer(t)
	for _, name := range []string{"chat", "chat m1"} {
		span := spans[name]
		if span == nil {
			t.Fatalf("missing span %q", name)
		}
		if span.Status().Code != codes.Error {
			t.Errorf("%s status = %v, want Error", name, span.Status())
		}
		if got := mustAttr(t, span, AttrOutcome).AsString(); got != string(OutcomeUpstreamError) {
			t.Errorf("%s outcome = %q", name, got)
		}
		if got := mustAttr(t, span, AttrErrorType).AsString(); got != "500" {
			t.Errorf("%s error.type = %q, want 500", name, got)
		}
	}
	if spans[SpanTimeToFirstToken] != nil {
		t.Error("an HTTP error response must not emit TimeToFirstToken")
	}
}

func Test_panicの値に不正なUTF8が含まれてもtelemetryが有効なUTF8になっている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("test").Start(context.Background(), "server", trace.WithSpanKind(trace.SpanKindServer))
	st := &requestState{span: span, ctx: ctx, tracer: tp.Tracer("test"), start: time.Now(), reqCtx: context.Background()}
	hostile := errors.New(strings.Repeat("a", 255) + string([]byte{0xff}))
	func() {
		defer func() { _ = recover() }()
		st.observe(func() { panic(hostile) })
	}()
	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d", len(ended))
	}
	got := ended[0]
	if got.Status().Code != codes.Error {
		t.Fatalf("status = %v", got.Status())
	}
	if value := mustAttr(t, got, AttrOutcome).AsString(); value != string(OutcomeInternalError) {
		t.Errorf("outcome = %q", value)
	}
	if value := mustAttr(t, got, AttrErrorType).AsString(); value != "panic" {
		t.Errorf("error.type = %q", value)
	}
	for _, event := range got.Events() {
		for _, kv := range event.Attributes {
			if kv.Value.Type() == attribute.STRING && !utf8.ValidString(kv.Value.AsString()) {
				t.Errorf("event %s contains invalid UTF-8 in %s", event.Name, kv.Key)
			}
		}
	}
}

type invalidErrorTransport struct{}

func (invalidErrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New(strings.Repeat("a", 255) + string([]byte{0xff}))
}

func Test_Transportエラーに不正なUTF8が含まれてもOpenTelemetryProtocolでexportされている(t *testing.T) {
	var requests atomic.Int32
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()

	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	oldHandler := otel.GetErrorHandler()
	defer otel.SetTracerProvider(oldProvider)
	defer otel.SetTextMapPropagator(oldPropagator)
	defer otel.SetErrorHandler(oldHandler)
	reported := make(chan error, 4)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { reported <- err }))

	cfg := config.Defaults()
	cfg.OpenTelemetry.Endpoint = collector.URL
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	shutdown, err := tracing.Init(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse("http://upstream.invalid")
	h := New(Options{Upstream: u, Logger: logger, Transport: invalidErrorTransport{}})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m1"}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() == 0 {
		t.Fatal("no OTLP request was exported")
	}
	select {
	case err := <-reported:
		t.Fatalf("SDK export error: %v", err)
	default:
	}
}

func Test_未知の長大なServerSentEvents行が破棄されたときoutcomeがobservation_limitedになっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+strings.Repeat("x", serversentevents.MaxLine+1)+"\n\n")
	}), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	server := h.awaitServer(t)["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeObservationLimited) {
		t.Errorf("outcome = %q", got)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("observation limit is not proof of upstream failure: %v", server.Status())
	}
	for _, name := range []string{SpanReasoning, SpanResponse, SpanToolCall} {
		if got := len(h.endedByName(name)); got != 0 {
			t.Errorf("%s spans = %d, want 0", name, got)
		}
	}
}

func Test_複数のContentEncodingヘッダーがあるとき最後のencodingが記録されている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Encoding", "identity")
		w.Header().Add("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `{"opaque":true}`)
	}), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	server := h.awaitServer(t)["chat"]
	if got := mustAttr(t, server, AttrResponseEncoding).AsString(); got != "gzip" {
		t.Errorf("response_encoding = %q", got)
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeObservationLimited) {
		t.Errorf("outcome = %q", got)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("unparsed encoding is not an upstream failure: %v", server.Status())
	}
	if got := mustAttr(t, server, AttrResponseBody).AsString(); got != `{"opaque":true}` {
		t.Errorf("response body = %q", got)
	}
	if _, ok := attrOf(server, attribute.Key("llamaproxy.response.body.opaque")); ok {
		t.Error("opaque response body was split as JSON")
	}
	for _, name := range []string{SpanReasoning, SpanResponse, SpanToolCall} {
		if got := len(h.endedByName(name)); got != 0 {
			t.Errorf("%s spans = %d, want 0", name, got)
		}
	}
}

func Test_trace_contextを信頼しないときtraceparentとtracestateがまとめて置換されている(t *testing.T) {
	got := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
		_, _ = io.WriteString(w, nonStreamBody)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	handler := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagator,
		ModelInSpanName: false, TrustTraceContext: false,
	})
	outerBaggage, err := baggage.Parse("middleware=secret")
	if err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := baggage.ContextWithBaggage(r.Context(), outerBaggage)
		handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer h.Close()
	req, _ := http.NewRequest(http.MethodPost, h.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m1"}`))
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	req.Header.Set("tracestate", "vendor=attacker-controlled")
	req.Header.Set("baggage", "secret=attacker-controlled")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	headers := <-got
	if strings.Contains(headers.Get("traceparent"), "11111111111111111111111111111111") {
		t.Errorf("old traceparent was forwarded: %q", headers.Get("traceparent"))
	}
	if headers.Get("tracestate") != "" {
		t.Errorf("old tracestate was forwarded: %q", headers.Get("tracestate"))
	}
	if headers.Get("baggage") != "" {
		t.Errorf("old baggage was forwarded: %q", headers.Get("baggage"))
	}
}

type stagedBody struct {
	step    int
	release <-chan struct{}
}

func (b *stagedBody) Read(p []byte) (int, error) {
	switch b.step {
	case 0:
		b.step++
		return copy(p, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n"), nil
	case 1:
		<-b.release
		b.step++
		return copy(p, "data: [DONE]\n\n"), nil
	default:
		return 0, io.EOF
	}
}

func (b *stagedBody) Close() error { return nil }

type stagedTransport struct {
	body io.ReadCloser
}

func (t stagedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       t.body,
		Request:    r,
	}, nil
}

type signalWriter struct {
	header http.Header
	once   sync.Once
	wrote  chan struct{}
}

func (w *signalWriter) Header() http.Header { return w.header }
func (w *signalWriter) WriteHeader(int)     {}
func (w *signalWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.wrote) })
	return len(p), nil
}
func (w *signalWriter) Flush() {}

func Test_最初のtokenが転送された時点でTimeToFirstToken_spanが終了している(t *testing.T) {
	release := make(chan struct{})
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true,
		Transport: stagedTransport{body: &stagedBody{release: release}},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	w := &signalWriter{header: make(http.Header), wrote: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, req)
		close(done)
	}()
	select {
	case <-w.wrote:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("first token was not forwarded")
	}
	found := false
	for _, span := range rec.Ended() {
		if span.Name() == SpanTimeToFirstToken {
			found = true
			if got := mustAttr(t, span, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
				t.Errorf("TimeToFirstToken outcome = %q", got)
			}
		}
	}
	close(release)
	<-done
	if !found {
		t.Fatal("TimeToFirstToken was not ended at the first token")
	}
}

type readWriteBody struct{ bytes.Buffer }

func (b *readWriteBody) Close() error                { return nil }
func (b *readWriteBody) Write(p []byte) (int, error) { return b.Buffer.Write(p) }

func Test_101応答のbodyでReadWriteCloserが保持されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("test").Start(context.Background(), "server")
	st := &requestState{span: span, ctx: ctx, tracer: tp.Tracer("test"), start: time.Now(), reqCtx: context.Background()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req = req.WithContext(context.WithValue(req.Context(), requestStateKey{}, st))
	body := &readWriteBody{}
	res := &http.Response{StatusCode: http.StatusSwitchingProtocols, Header: make(http.Header), Body: body, Request: req}
	u, _ := url.Parse("http://upstream.invalid")
	h := New(Options{Upstream: u})
	if err := h.modifyResponse(res); err != nil {
		t.Fatal(err)
	}
	if res.Body != body {
		t.Fatalf("upgrade body was replaced with %T", res.Body)
	}
	if _, ok := res.Body.(io.ReadWriteCloser); !ok {
		t.Fatalf("upgrade body lost ReadWriteCloser: %T", res.Body)
	}
}

func Test_計装対象リクエストでもHTTP_upgradeの双方向通信が維持されている(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test-protocol\r\n\r\n")
		_ = rw.Flush()
		request := make([]byte, 4)
		if _, err := io.ReadFull(rw, request); err != nil {
			return
		}
		if string(request) == "ping" {
			_, _ = rw.WriteString("pong")
			_ = rw.Flush()
		}
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	proxyServer := httptest.NewServer(New(Options{Upstream: u}))
	defer proxyServer.Close()
	proxyURL, _ := url.Parse(proxyServer.URL)
	conn, err := net.DialTimeout("tcp", proxyURL.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: test-protocol\r\nContent-Length: 0\r\n\r\n", proxyURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("status line = %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(reader, reply); err != nil {
		t.Fatal(err)
	}
	if string(reply) != "pong" {
		t.Fatalf("upgrade reply = %q", reply)
	}
}

type cancellationTransport struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

type cancellationErrorTransport struct {
	started chan struct{}
}

func (t *cancellationErrorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	close(t.started)
	<-r.Context().Done()
	return nil, errors.New("use of closed network connection")
}

func Test_client_cancel後にTransportのIOエラーが起きてもoutcomeがclient_cancelになっている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	transport := &cancellationErrorTransport{started: make(chan struct{})}
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: transport,
	})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1"}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-transport.started
	cancel()
	<-done
	checked := 0
	for _, span := range rec.Ended() {
		if span.SpanKind() != trace.SpanKindServer && span.SpanKind() != trace.SpanKindClient {
			continue
		}
		checked++
		if got := mustAttr(t, span, AttrOutcome).AsString(); got != string(OutcomeClientCancel) {
			t.Errorf("%s outcome = %q", span.Name(), got)
		}
		if span.Status().Code == codes.Error {
			t.Errorf("%s status = %v", span.Name(), span.Status())
		}
	}
	if checked != 2 {
		t.Errorf("checked SERVER/CLIENT spans = %d, want 2", checked)
	}
}

type cancellationErrorBody struct {
	ctx     context.Context
	started chan struct{}
	once    sync.Once
}

func (b *cancellationErrorBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.ctx.Done()
	return 0, errors.New("use of closed network connection")
}

func (*cancellationErrorBody) Close() error { return nil }

type cancellationBodyTransport struct {
	started chan struct{}
}

func (t *cancellationBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       &cancellationErrorBody{ctx: r.Context(), started: t.started},
		Request:    r,
	}, nil
}

func Test_client_cancel後にresponse_bodyのIOエラーが起きてもoutcomeがclient_cancelになっている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	transport := &cancellationBodyTransport{started: make(chan struct{})}
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: transport,
	})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-transport.started
	cancel()
	<-done
	var server sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			server = span
		}
	}
	if server == nil {
		t.Fatal("server span was not ended")
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeClientCancel) {
		t.Errorf("outcome = %q", got)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v", server.Status())
	}
}

func Test_最初のtokenがないstreamではtoken_phase_spanが生成されていない(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}), nil)
	res, err := http.Post(h.proxy.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	spans := h.awaitServer(t)
	if spans[SpanTimeToFirstToken] != nil {
		t.Error("TimeToFirstToken was emitted without a first token")
	}
	if spans[SpanGeneration] != nil {
		t.Error("generation was emitted without a first token")
	}
	server := spans["chat"]
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeSuccess) {
		t.Errorf("outcome = %q", got)
	}
}

func (t *cancellationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	close(t.started)
	<-r.Context().Done()
	close(t.canceled)
	<-t.release
	return nil, context.Cause(r.Context())
}

func Test_header送信前に強制終了したときoutcomeがshutdownになっている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	transport := &cancellationTransport{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: transport,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m1"}`))
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-transport.started
	h.BeginForcedClose()
	<-transport.canceled
	close(transport.release)
	<-done
	for _, span := range rec.Ended() {
		if span.SpanKind() != trace.SpanKindServer && span.SpanKind() != trace.SpanKindClient {
			continue
		}
		if got := mustAttr(t, span, AttrOutcome).AsString(); got != string(OutcomeShutdown) {
			t.Errorf("%s outcome = %q", span.Name(), got)
		}
		if span.Status().Code == codes.Error {
			t.Errorf("%s status = %v", span.Name(), span.Status())
		}
	}
}

func Test_Serverを強制終了したときshutdownのcauseがspanに保持されている(t *testing.T) {
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	u, _ := url.Parse(upstream.URL)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true,
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()

	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest(http.MethodPost, "http://"+listener.Addr().String()+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true}`))
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	h.BeginDrain()
	h.BeginForcedClose()
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	idleCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.WaitIdle(idleCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve() = %v", err)
	}

	var server sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			server = span
			break
		}
	}
	if server == nil {
		t.Fatal("server span was not ended")
	}
	if got := mustAttr(t, server, AttrOutcome).AsString(); got != string(OutcomeShutdown) {
		t.Errorf("outcome = %q", got)
	}
	if server.Status().Code == codes.Error {
		t.Errorf("status = %v", server.Status())
	}
}

func Test_client_cancel後に強制終了してもclient_cancelのcauseが保持されている(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	u, _ := url.Parse("http://upstream.invalid")
	transport := &cancellationTransport{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	h := New(Options{
		Upstream: u, TracerProvider: tp, Propagator: propagation.TraceContext{},
		ModelInSpanName: false, TrustTraceContext: true, Transport: transport,
	})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m1"}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-transport.started
	cancel()
	<-transport.canceled
	h.BeginForcedClose()
	close(transport.release)
	<-done
	for _, span := range rec.Ended() {
		if span.SpanKind() != trace.SpanKindServer && span.SpanKind() != trace.SpanKindClient {
			continue
		}
		if got := mustAttr(t, span, AttrOutcome).AsString(); got != string(OutcomeClientCancel) {
			t.Errorf("%s outcome = %q", span.Name(), got)
		}
	}
}

func Test_同時実行上限を超えたリクエストが503で拒否されている(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, nonStreamBody)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	h := httptest.NewServer(New(Options{Upstream: u, MaxConcurrentRequests: 1}))
	defer h.Close()
	firstDone := make(chan struct{})
	go func() {
		res, err := http.Post(h.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m1"}`))
		if err == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
		}
		close(firstDone)
	}()
	<-started
	res, err := http.Post(h.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m2"}`))
	if err != nil {
		close(release)
		<-firstDone
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	close(release)
	<-firstDone
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if res.Header.Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q", res.Header.Get("Retry-After"))
	}
}
