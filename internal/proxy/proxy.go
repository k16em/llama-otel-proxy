package proxy

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/genai"
	"github.com/k16em/llama-otel-proxy/internal/route"
	"github.com/k16em/llama-otel-proxy/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	Upstream *url.URL
	Logger   *slog.Logger

	ModelInSpanName bool

	RequestBodyLimit      int64
	MaxConcurrentRequests int

	MaxConcurrentPassthroughRequests int

	TrustTraceContext   bool
	SessionTraceIDRoots bool

	SessionIdleTimeout time.Duration

	TracerProvider trace.TracerProvider
	Propagator     propagation.TextMapPropagator

	Transport http.RoundTripper
}

type Handler struct {
	upstreamAttrs       []attribute.KeyValue
	proxy               *httputil.ReverseProxy
	tracer              trace.Tracer
	propagator          propagation.TextMapPropagator
	opts                Options
	logger              *slog.Logger
	sessionTraceIDRoots bool
	sessions            *sessionRegistry

	slots            chan struct{}
	passthroughSlots chan struct{}

	activeMu sync.Mutex
	active   map[*activeRequest]struct{}
	idle     chan struct{}

	draining    atomic.Bool
	forcedClose atomic.Bool
}

type activeRequest struct {
	cancel context.CancelCauseFunc
}

const (
	defaultMaxConcurrentRequests            = 16
	defaultMaxConcurrentPassthroughRequests = 128
)

func (h *Handler) BeginDrain() {
	h.activeMu.Lock()
	h.draining.Store(true)
	h.activeMu.Unlock()
}

func (h *Handler) BeginForcedClose() {
	if !h.forcedClose.CompareAndSwap(false, true) {
		return
	}
	h.activeMu.Lock()
	active := make([]*activeRequest, 0, len(h.active))
	for req := range h.active {
		active = append(active, req)
	}
	h.activeMu.Unlock()
	for _, req := range active {
		req.cancel(errForcedClose)
	}
}

func (h *Handler) WaitIdle(ctx context.Context) error {
	for {
		h.activeMu.Lock()
		if len(h.active) == 0 {
			h.activeMu.Unlock()
			return nil
		}
		idle := h.idle
		h.activeMu.Unlock()
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func New(opts Options) *Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.RequestBodyLimit == 0 {
		opts.RequestBodyLimit = genai.DefaultRequestLimit
	}
	if opts.MaxConcurrentRequests <= 0 {
		opts.MaxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if opts.MaxConcurrentPassthroughRequests <= 0 {
		opts.MaxConcurrentPassthroughRequests = defaultMaxConcurrentPassthroughRequests
	}

	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
	if opts.Propagator == nil {
		opts.Propagator = otel.GetTextMapPropagator()
	}

	h := &Handler{
		upstreamAttrs: upstreamAttributes(opts.Upstream),
		tracer: opts.TracerProvider.Tracer(scopeName,
			trace.WithInstrumentationVersion(scopeVersion),
			trace.WithSchemaURL(semconv.SchemaURL),
		),
		propagator:          opts.Propagator,
		opts:                opts,
		logger:              opts.Logger,
		sessionTraceIDRoots: opts.SessionTraceIDRoots,
		slots:               make(chan struct{}, opts.MaxConcurrentRequests),
		passthroughSlots:    make(chan struct{}, opts.MaxConcurrentPassthroughRequests),
		active:              make(map[*activeRequest]struct{}),
	}

	if opts.SessionTraceIDRoots {
		h.sessions = newSessionRegistry(h.tracer, opts.SessionIdleTimeout)
	}

	transport := opts.Transport
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.ResponseHeaderTimeout = 0
		t.MaxResponseHeaderBytes = 1 << 20
		t.Proxy = nil
		transport = t
	}

	h.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(opts.Upstream)
			pr.SetXForwarded()
			if requestStateFromContext(pr.In.Context()) != nil {
				pr.Out.Header.Set("Accept-Encoding", "identity")
			}
		},
		Transport: &proxyTransport{
			base:              transport,
			tracer:            h.tracer,
			propagator:        h.propagator,
			trustTraceContext: opts.TrustTraceContext,
		},

		FlushInterval:  -1,
		BufferPool:     newBufferPool(),
		ModifyResponse: h.modifyResponse,
		ErrorHandler:   h.errorHandler,
		ErrorLog:       slog.NewLogLogger(opts.Logger.Handler(), slog.LevelWarn),
	}
	return h
}

const (
	scopeName    = "github.com/k16em/llama-otel-proxy"
	scopeVersion = "0.1.0"
)

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op, instrumented := route.Operation(r.URL.Path)
	if instrumented && r.Method != http.MethodPost {
		instrumented = false
	}

	reqCtx, release, admitted := h.admitRequest(r.Context(), instrumented)
	if !admitted {
		h.rejectOverload(w)
		return
	}
	defer release()
	r = r.WithContext(reqCtx)

	start := time.Now()

	if !instrumented {
		h.proxy.ServeHTTP(w, r)
		return
	}

	requestHeader := traceRequestHeader(r)
	req, _ := genai.PeekRequest(r, h.opts.RequestBodyLimit)
	requestBody, requestBodyTruncated := boundedTraceBody(req.Body)
	model, modelOK := boundedModel(req)

	inferenceName := op
	if modelOK {
		inferenceName = op + " " + string(model)
	}
	name := op
	if h.opts.ModelInSpanName {
		name = inferenceName
	}

	ctx, incoming, newRoot := h.traceContext(r)

	dims := []attribute.KeyValue{
		AttrOperationName.String(op),
	}

	var sessionAttrs []attribute.KeyValue
	if h.sessions != nil {
		if id, traceID, ok := h.sessionKey(r); ok {
			parent, release := h.sessions.acquire(id, traceID, start, string(model), r.UserAgent())
			if release != nil {
				defer func() { release(time.Now()) }()
				sessionAttrs = append(sessionAttrs, semconv.SessionID(id), AttrConversationID.String(id))
				if parent.IsValid() {
					ctx = trace.ContextWithSpanContext(ctx, parent)
					newRoot = false
				}
			}
		}
	}

	if req.StreamKnown {
		dims = append(dims, AttrRequestStream.Bool(req.Stream))
	}
	if modelOK {
		dims = append(dims, AttrRequestModel.String(string(model)))
	}

	inferenceAttrs := append([]attribute.KeyValue{
		AttrProviderName.String(System),
		AttrHTTPMethod.String(r.Method),
		AttrURLPath.String(r.URL.Path),
	}, dims...)
	inferenceAttrs = append(inferenceAttrs, sessionAttrs...)
	inferenceAttrs = append(inferenceAttrs, h.upstreamAttrs...)
	inferenceAttrs = append(inferenceAttrs, requestParameterAttributes(req.Parameters)...)
	attrs := inferenceAttrs

	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	}
	if incoming.IsValid() {
		startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: incoming}))
	}
	if newRoot {
		startOpts = append(startOpts, trace.WithNewRoot())
	}
	ctx, span := h.tracer.Start(ctx, name, startOpts...)

	observed, responseWriter := newObservedWriter(w)
	observed.setCapture(span.IsRecording())
	st := &requestState{
		span:                 span,
		ctx:                  ctx,
		tracer:               h.tracer,
		start:                start,
		dims:                 dims,
		inferenceSpanName:    inferenceName,
		inferenceAttrs:       inferenceAttrs,
		reqCtx:               reqCtx,
		writeErr:             observed.writeErr,
		snapshotResponse:     observed.snapshot,
		disableBodyCapture:   observed.disableBodyCapture,
		requestHeader:        requestHeader,
		requestBody:          requestBody,
		requestBodyTruncated: req.BodyTruncated || requestBodyTruncated,
	}
	ctx = context.WithValue(ctx, requestStateKey{}, st)

	st.observe(func() {
		h.proxy.ServeHTTP(responseWriter, r.WithContext(ctx))
	})
}

func upstreamAttributes(upstream *url.URL) []attribute.KeyValue {
	if upstream == nil {
		return nil
	}
	host := upstream.Hostname()
	if host == "" {
		return nil
	}
	attrs := []attribute.KeyValue{semconv.ServerAddress(host)}
	port := upstream.Port()
	if port == "" {
		switch upstream.Scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	if number, err := strconv.Atoi(port); err == nil {
		attrs = append(attrs, semconv.ServerPort(number))
	}
	return attrs
}

func requestParameterAttributes(params genai.RequestParameters) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 10)
	if params.MaxTokens != nil {
		attrs = append(attrs, AttrRequestMaxTokens.Int64(*params.MaxTokens))
	}
	if params.Temperature != nil {
		attrs = append(attrs, AttrRequestTemperature.Float64(*params.Temperature))
	}
	if params.TopP != nil {
		attrs = append(attrs, AttrRequestTopP.Float64(*params.TopP))
	}
	if params.TopK != nil {
		attrs = append(attrs, AttrRequestTopK.Int64(*params.TopK))
	}
	if params.FrequencyPenalty != nil {
		attrs = append(attrs, AttrRequestFrequencyPenalty.Float64(*params.FrequencyPenalty))
	}
	if params.PresencePenalty != nil {
		attrs = append(attrs, AttrRequestPresencePenalty.Float64(*params.PresencePenalty))
	}
	if params.Seed != nil {
		attrs = append(attrs, AttrRequestSeed.Int64(*params.Seed))
	}
	if params.ChoiceCount != nil {
		attrs = append(attrs, AttrRequestChoiceCount.Int64(*params.ChoiceCount))
	}
	if len(params.StopSequences) > 0 {
		attrs = append(attrs, AttrRequestStopSequences.StringSlice(params.StopSequences))
	}
	if params.OutputType != "" {
		attrs = append(attrs, AttrOutputType.String(params.OutputType))
	}
	return attrs
}

func traceRequestHeader(r *http.Request) http.Header {
	header := r.Header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	if r.Host != "" {
		header.Set("Host", r.Host)
	}
	if r.ContentLength > 0 && header.Get("Content-Length") == "" {
		header.Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
	}
	if len(r.TransferEncoding) > 0 && header.Get("Transfer-Encoding") == "" {
		header["Transfer-Encoding"] = append([]string(nil), r.TransferEncoding...)
	}
	return header
}

func (h *Handler) rejectOverload(w http.ResponseWriter) {
	w.Header().Set("Connection", "close")
	w.Header().Set("Retry-After", "1")
	http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
}

func (h *Handler) admitRequest(parent context.Context, instrumented bool) (context.Context, func(), bool) {
	slots := h.passthroughSlots
	if instrumented {
		slots = h.slots
	}
	ctx, cancel := context.WithCancelCause(parent)
	req := &activeRequest{cancel: cancel}
	h.activeMu.Lock()
	if h.draining.Load() || h.forcedClose.Load() {
		h.activeMu.Unlock()
		cancel(nil)
		return nil, nil, false
	}
	select {
	case slots <- struct{}{}:
	default:
		h.activeMu.Unlock()
		cancel(nil)
		return nil, nil, false
	}
	if len(h.active) == 0 {
		h.idle = make(chan struct{})
	}
	h.active[req] = struct{}{}
	h.activeMu.Unlock()
	return ctx, func() {
		h.activeMu.Lock()
		delete(h.active, req)
		if len(h.active) == 0 {
			close(h.idle)
		}
		h.activeMu.Unlock()
		<-slots
		cancel(nil)
	}, true
}

func (h *Handler) sessionKey(r *http.Request) (string, trace.TraceID, bool) {
	if !h.opts.TrustTraceContext || !h.sessionTraceIDRoots {
		return "", trace.TraceID{}, false
	}
	id := strings.TrimSpace(r.Header.Get("X-Session-Id"))
	if id == "" {
		return "", trace.TraceID{}, false
	}
	traceID, ok := sessionTraceID(id)
	if !ok {
		return "", trace.TraceID{}, false
	}
	return id, traceID, true
}

func (h *Handler) CloseSessions() {
	if h.sessions != nil {
		h.sessions.closeAll()
	}
}

func (h *Handler) traceContext(r *http.Request) (context.Context, trace.SpanContext, bool) {
	if h.opts.TrustTraceContext {
		extracted := h.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		incoming := trace.SpanContextFromContext(extracted)
		if _, traceID, ok := h.sessionKey(r); ok {
			return tracing.ContextWithTraceID(extracted, traceID), incoming, true
		}
		return extracted, trace.SpanContext{}, false
	}
	clean := baggage.ContextWithoutBaggage(trace.ContextWithSpanContext(r.Context(), trace.SpanContext{}))
	extracted := h.propagator.Extract(clean, propagation.HeaderCarrier(r.Header))
	return clean, trace.SpanContextFromContext(extracted), true
}

func sessionTraceID(sessionID string) (trace.TraceID, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return trace.TraceID{}, false
	}
	hexID := strings.ReplaceAll(sessionID, "-", "")
	if len(hexID) == 32 {
		if traceID, err := trace.TraceIDFromHex(hexID); err == nil && traceID.IsValid() {
			return traceID, true
		}
	}
	digest := sha256.Sum256([]byte(sessionID))
	var traceID trace.TraceID
	copy(traceID[:], digest[:len(traceID)])
	return traceID, true
}

func (h *Handler) modifyResponse(res *http.Response) error {
	st := requestStateFromContext(res.Request.Context())
	if st == nil {
		return nil
	}

	encoding := contentEncoding(res.Header)
	serverSentEvents := isEventStream(res.Header.Get("Content-Type"))
	st.responseStarted(res.StatusCode, serverSentEvents, encoding)
	if serverSentEvents && encoding == "" && st.disableBodyCapture != nil {
		st.disableBodyCapture()
	}

	if res.StatusCode == http.StatusSwitchingProtocols || res.Body == nil || res.Body == http.NoBody {
		return nil
	}
	observer := newBodyObserver(res.Body, serverSentEvents, st.bodyDone)
	observer.opaque = encoding != ""
	observer.onTimeToFirstToken = st.recordTimeToFirstToken
	observer.onStreamOutput = st.recordStreamOutput
	res.Body = observer
	return nil
}

func contentEncoding(header http.Header) string {
	var codings []string
	for _, value := range header.Values("Content-Encoding") {
		for _, raw := range strings.Split(value, ",") {
			coding := strings.TrimSpace(raw)
			if coding == "" || strings.EqualFold(coding, "identity") {
				continue
			}
			codings = append(codings, strings.ToLower(coding))
		}
	}
	return strings.Join(codings, ", ")
}

func isEventStream(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mediaType == "text/event-stream"
}

const MaxModelLength = 128

const oversizedModel telemetryModel = "(oversized)"

type telemetryModel string

func boundedModel(req genai.Request) (telemetryModel, bool) {
	if !req.ModelKnown {
		return "", false
	}
	if len(req.Model) > MaxModelLength {
		return oversizedModel, true
	}
	return telemetryModel(req.Model), true
}

func (h *Handler) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	st := requestStateFromContext(r.Context())
	cause := context.Cause(r.Context())
	if errors.Is(cause, errForcedClose) {
		if st != nil {
			st.recordLocalStatus(http.StatusServiceUnavailable)
		}
		h.rejectOverload(w)
		return
	}
	if cause != nil || errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if st != nil {
			st.recordUpstreamFailure(http.StatusGatewayTimeout, err)
		}
		h.logger.Warn("upstream request timed out", "path", r.URL.Path, "err", err)
		w.WriteHeader(http.StatusGatewayTimeout)
		return
	}
	if st != nil {
		st.recordUpstreamFailure(http.StatusBadGateway, err)
	}
	h.logger.Warn("upstream request failed", "path", r.URL.Path, "err", err)
	w.WriteHeader(http.StatusBadGateway)
}

type proxyTransport struct {
	base              http.RoundTripper
	tracer            trace.Tracer
	propagator        propagation.TextMapPropagator
	trustTraceContext bool
}

func (t *proxyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	st := requestStateFromContext(r.Context())
	if st == nil {
		if t.trustTraceContext {
			return t.base.RoundTrip(r)
		}
		out := r.Clone(r.Context())
		stripTraceHeaders(out.Header)
		return t.base.RoundTrip(out)
	}

	ctx, span := t.tracer.Start(r.Context(), st.inferenceSpanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(st.inferenceAttrs...),
	)

	out := r.Clone(ctx)
	stripTraceContextHeaders(out.Header)
	if !t.trustTraceContext {
		out.Header.Del("baggage")
	}
	t.propagator.Inject(ctx, propagation.HeaderCarrier(out.Header))

	res, err := t.base.RoundTrip(out)

	if err != nil {
		cause := context.Cause(r.Context())
		switch {
		case errors.Is(cause, errForcedClose):
			span.SetAttributes(AttrOutcome.String(string(OutcomeShutdown)))
		case cause != nil, errors.Is(err, context.Canceled):
			span.SetAttributes(AttrOutcome.String(string(OutcomeClientCancel)))
		default:
			span.SetAttributes(
				AttrOutcome.String(string(OutcomeUpstreamError)),
				AttrErrorType.String("transport_error"),
			)
			recordSpanError(span, err)
			span.SetStatus(codes.Error, genai.Truncate(err.Error()))
		}
	} else {
		span.SetAttributes(
			AttrStatusCode.Int(res.StatusCode),
			AttrOutcome.String(string(OutcomeSuccess)),
		)

		if res.StatusCode >= 400 {
			span.SetAttributes(
				AttrOutcome.String(string(OutcomeUpstreamError)),
				AttrErrorType.String(strconv.Itoa(res.StatusCode)),
			)
			span.SetStatus(codes.Error, http.StatusText(res.StatusCode))
		}
	}
	span.End()
	return res, err
}

func stripTraceHeaders(header http.Header) {
	stripTraceContextHeaders(header)
	header.Del("baggage")
}

func stripTraceContextHeaders(header http.Header) {
	header.Del("traceparent")
	header.Del("tracestate")
}

func newBufferPool() httputil.BufferPool { return &bufferPool{} }

type bufferPool struct {
	pool sync.Pool
}

func (p *bufferPool) Get() []byte {
	if b, ok := p.pool.Get().(*[]byte); ok {
		return *b
	}
	b := make([]byte, 32*1024)
	return b
}

func (p *bufferPool) Put(b []byte) { p.pool.Put(&b) }
