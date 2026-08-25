package proxy

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/genai"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type requestStateKey struct{}

var errForcedClose = errors.New("proxy forced close")

func requestStateFromContext(ctx context.Context) *requestState {
	st, _ := ctx.Value(requestStateKey{}).(*requestState)
	return st
}

type requestState struct {
	span   trace.Span
	ctx    context.Context
	tracer trace.Tracer
	start  time.Time

	reqCtx             context.Context
	writeErr           func() error
	snapshotResponse   func() responseSnapshot
	disableBodyCapture func()

	dims                 []attribute.KeyValue
	inferenceAttrs       []attribute.KeyValue
	inferenceSpanName    string
	requestHeader        http.Header
	requestBody          []byte
	requestBodyTruncated bool

	mu                       sync.Mutex
	ended                    bool
	statusCode               int
	serverSentEventsResponse bool
	encoding                 string
	body                     *bodyResult
	panicRecorded            bool
	panicErr                 error
	upstreamErr              error
	timeToFirstTokenOnce     sync.Once
}

func (s *requestState) responseStarted(statusCode int, serverSentEvents bool, encoding string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = statusCode
	s.serverSentEventsResponse = serverSentEvents
	s.encoding = encoding
}

func (s *requestState) bodyDone(res bodyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = &res
}

func (s *requestState) observe(serve func()) {
	defer func() {
		if p := recover(); p != nil {
			s.recordPanic(p)
			s.end()
			panic(p)
		}
		s.end()
	}()
	serve()
}

func (s *requestState) recordPanic(p any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		return
	}

	err, ok := p.(error)
	if !ok {
		err = errors.New(genai.Truncate(toString(p)))
	}
	s.panicRecorded = true
	s.panicErr = err
	recordSpanError(s.span, err)
}

func (s *requestState) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true

	res := s.body
	if res == nil {
		res = &bodyResult{}
	}
	parsed, parsedOK := genai.ParseResponse(res.Payload)
	out := s.outcome(res, parsed, parsedOK)

	for _, output := range res.StreamOutputs {
		s.emitStreamOutput(output, s.streamOutputOutcome(output, out))
	}
	s.emitPhaseSpans(res, out)
	s.setServerAttributes(res, parsed, parsedOK, out)
	s.setExchangeAttributes()
	s.span.End()
}

func (s *requestState) setExchangeAttributes() {
	if !s.span.IsRecording() {
		return
	}
	attrs := traceBodyAttributes(AttrRequestBody, AttrTraceRequestBodySplitLimited, s.requestBody, s.requestBodyTruncated)
	if s.requestBodyTruncated {
		attrs = append(attrs, AttrTraceRequestBodyTruncated.Bool(true))
	}
	response := responseSnapshot{}
	if s.snapshotResponse != nil {
		response = s.snapshotResponse()
	}
	if s.encoding != "" {
		attrs = append(attrs, AttrResponseBody.String(traceBody(response.Body)))
		if response.BodyTruncated {
			attrs = append(attrs, AttrTraceResponseBodyTruncated.Bool(true))
		}
	} else if !s.serverSentEventsResponse {
		attrs = append(attrs, traceBodyAttributes(AttrResponseBody, AttrTraceResponseBodySplitLimited, response.Body, response.BodyTruncated)...)
		if response.BodyTruncated {
			attrs = append(attrs, AttrTraceResponseBodyTruncated.Bool(true))
		}
	}
	requestHeaders, requestHeadersLimited := requestHeaderAttributes(s.requestHeader)
	attrs = append(attrs, requestHeaders...)
	if requestHeadersLimited {
		attrs = append(attrs, AttrTraceRequestHeaderLimited.Bool(true))
	}
	responseHeaders, responseHeadersLimited := responseHeaderAttributes(response.Header)
	attrs = append(attrs, responseHeaders...)
	if responseHeadersLimited {
		attrs = append(attrs, AttrTraceResponseHeaderLimited.Bool(true))
	}
	s.span.SetAttributes(attrs...)
}

func (s *requestState) outcome(res *bodyResult, parsed genai.Response, parsedOK bool) Outcome {
	var cause error
	if s.reqCtx != nil {
		cause = context.Cause(s.reqCtx)
	}
	cancelled := cause != nil ||
		errors.Is(res.Err, context.Canceled) || errors.Is(res.Err, context.DeadlineExceeded) ||
		(s.writeErr != nil && s.writeErr() != nil)

	switch {
	case s.panicRecorded:
		return OutcomeInternalError
	case errors.Is(cause, errForcedClose):
		return OutcomeShutdown
	case cause != nil:
		return OutcomeClientCancel
	case s.upstreamErr != nil:
		return OutcomeUpstreamError
	case res.Err != nil && !errors.Is(res.Err, context.Canceled) && !errors.Is(res.Err, context.DeadlineExceeded):
		return OutcomeUpstreamError
	case s.statusCode >= http.StatusBadRequest:
		return OutcomeUpstreamError
	case parsedOK && parsed.Failed:
		return OutcomeUpstreamError
	case parsedOK && parsed.Incomplete:
		return OutcomeIncomplete
	case cancelled:
		return OutcomeClientCancel
	case s.encoding != "" || res.Truncated || res.StreamLimited || res.Dropped > 0:
		return OutcomeObservationLimited
	case s.serverSentEventsResponse && s.hasSuccessfulHTTPStatus() && !s.responseTerminated(res, parsed, parsedOK):
		return OutcomeIncomplete
	default:
		return OutcomeSuccess
	}
}

func (s *requestState) responseTerminated(res *bodyResult, parsed genai.Response, parsedOK bool) bool {
	return res.SawDone || (parsedOK && parsed.Terminal)
}

func (s *requestState) hasSuccessfulHTTPStatus() bool {
	return s.statusCode >= 200 && s.statusCode < 300
}

func (s *requestState) phaseAttrs(out Outcome) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(s.dims)+2)
	attrs = append(attrs, s.dims...)
	if s.statusCode != 0 {
		attrs = append(attrs, AttrStatusCode.Int(s.statusCode))
	}
	attrs = append(attrs, AttrOutcome.String(string(out)))
	return attrs
}

func (s *requestState) recordTimeToFirstToken(at time.Time) {
	s.timeToFirstTokenOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.hasSuccessfulHTTPStatus() {
			return
		}
		_, span := s.tracer.Start(s.ctx, SpanTimeToFirstToken,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(s.start),
			trace.WithAttributes(s.phaseAttrs(OutcomeSuccess)...),
		)
		span.End(trace.WithTimestamp(at))
	})
}

func (s *requestState) recordStreamOutput(output genai.StreamOutput) {
	if !s.span.IsRecording() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.emitStreamOutput(output, s.streamOutputOutcome(output, OutcomeSuccess))
}

func (s *requestState) streamOutputOutcome(output genai.StreamOutput, fallback Outcome) Outcome {
	switch {
	case s.statusCode >= http.StatusBadRequest || output.Failed:
		return OutcomeUpstreamError
	case output.ProtocolIncomplete:
		return OutcomeIncomplete
	case output.ObservationLimited:
		return OutcomeObservationLimited
	case !output.Complete && fallback == OutcomeSuccess:
		return OutcomeIncomplete
	case !output.Complete:
		return fallback
	default:
		return OutcomeSuccess
	}
}

func (s *requestState) emitStreamOutput(output genai.StreamOutput, out Outcome) {
	if !s.span.IsRecording() {
		return
	}
	attrs := s.phaseAttrs(out)
	if output.HasChoiceIndex {
		attrs = append(attrs, AttrChoiceIndex.Int(output.ChoiceIndex))
	}
	if output.HasOutputIndex {
		attrs = append(attrs, AttrOutputIndex.Int(output.OutputIndex))
	}
	if output.HasContentIndex {
		attrs = append(attrs, AttrContentIndex.Int(output.ContentIndex))
	}
	if output.HasToolCallIndex {
		attrs = append(attrs, AttrToolCallIndex.Int(output.ToolCallIndex))
	}
	if output.ToolCallID != "" {
		attrs = append(attrs, AttrToolCallID.String(output.ToolCallID))
	}
	if output.ItemID != "" {
		attrs = append(attrs, AttrOutputItemID.String(output.ItemID))
	}
	if output.Reasoning != "" {
		attrs = append(attrs, AttrResponseBodyReasoningContent.String(output.Reasoning))
	}
	if output.Content != "" {
		attrs = append(attrs, AttrResponseBodyContent.String(output.Content))
	}
	if output.Refusal != "" {
		attrs = append(attrs, AttrResponseBodyRefusal.String(output.Refusal))
	}
	if output.ToolType != "" {
		attrs = append(attrs, AttrResponseBodyType.String(output.ToolType))
	}
	if output.Name != "" {
		attrs = append(attrs, AttrResponseBodyName.String(output.Name))
	}
	if output.Arguments != "" {
		attrs = append(attrs, AttrResponseBodyArguments.String(output.Arguments))
	}
	if output.FinishReason != "" {
		attrs = append(attrs, AttrResponseBodyFinishReason.String(output.FinishReason))
	}
	if output.Truncated {
		attrs = append(attrs, AttrTraceResponseBodyTruncated.Bool(true))
	}
	if !output.Complete {
		attrs = append(attrs, AttrResponseIncomplete.Bool(true))
	}
	if output.Failed && output.ErrorMessage != "" {
		attrs = append(attrs, AttrUpstreamError.String(output.ErrorMessage))
	}
	if out.failed() {
		attrs = append(attrs, AttrErrorType.String(s.errorType(out)))
	}

	spanName := SpanResponse
	switch output.Kind {
	case genai.StreamOutputReasoning:
		spanName = SpanReasoning
	case genai.StreamOutputToolCall:
		spanName = SpanToolCall
	}
	start := output.Start
	end := output.End
	if start.IsZero() {
		start = end
	}
	if end.IsZero() || end.Before(start) {
		end = start
	}
	_, span := s.tracer.Start(s.ctx, spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	)
	if out.failed() {
		message := string(out)
		if output.ErrorMessage != "" {
			message = output.ErrorMessage
		}
		span.SetStatus(codes.Error, message)
	}
	span.End(trace.WithTimestamp(end))
}

func (s *requestState) emitPhaseSpans(res *bodyResult, out Outcome) {
	if res.FirstByte.IsZero() || res.FirstToken.IsZero() {
		return
	}

	if !s.serverSentEventsResponse || !s.hasSuccessfulHTTPStatus() {
		return
	}
	genAttrs := append(s.phaseAttrs(out), AttrServerSentEventsChunkCount.Int(res.Chunks))
	_, span := s.tracer.Start(s.ctx, SpanGeneration,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(res.FirstToken),
		trace.WithAttributes(genAttrs...),
	)
	s.applyStatus(span, out, res)
	span.End(trace.WithTimestamp(res.LastByte))
}

func (s *requestState) applyStatus(span trace.Span, out Outcome, res *bodyResult) {
	if !out.failed() {
		return
	}
	span.SetAttributes(AttrErrorType.String(string(out)))
	if res.Err != nil {
		recordSpanError(span, res.Err)
	}
	span.SetStatus(codes.Error, string(out))
}

func (s *requestState) setServerAttributes(res *bodyResult, parsed genai.Response, parsedOK bool, out Outcome) {
	attrs := make([]attribute.KeyValue, 0, 16)
	attrs = append(attrs, AttrOutcome.String(string(out)))
	if s.statusCode != 0 {
		attrs = append(attrs, AttrStatusCode.Int(s.statusCode))
	}

	if s.serverSentEventsResponse {
		attrs = append(attrs, AttrServerSentEventsChunkCount.Int(res.Chunks))
		if res.Dropped > 0 {
			attrs = append(attrs, AttrServerSentEventsDroppedLineCount.Int(res.Dropped))
		}
	}
	if res.Truncated || res.StreamLimited || res.Dropped > 0 {
		attrs = append(attrs, AttrResponseBodyTruncated.Bool(true))
	}
	if s.encoding != "" {
		attrs = append(attrs, AttrResponseEncoding.String(s.encoding))
	}

	switch out {
	case OutcomeIncomplete:
		attrs = append(attrs, AttrResponseIncomplete.Bool(true))
	case OutcomeClientCancel:
		attrs = append(attrs, AttrClientDisconnected.Bool(true))
	case OutcomeShutdown:
		attrs = append(attrs, AttrShutdownInterrupted.Bool(true))
	case OutcomeUpstreamError:
		if parsedOK && parsed.Failed {
			attrs = append(attrs, AttrUpstreamError.String(parsed.Message))
		}
	}

	if out.failed() {
		attrs = append(attrs, AttrErrorType.String(s.errorType(out)))
		if res.Err != nil {
			recordSpanError(s.span, res.Err)
		}
		s.span.SetStatus(codes.Error, s.errorMessage(parsed, parsedOK, out, res))
	}

	if reasons := s.finishReasons(res, parsed, parsedOK); len(reasons) > 0 {
		attrs = append(attrs, AttrResponseFinishReasons.StringSlice(reasons))
	}

	if parsedOK {
		if model := responseModelIdentifier(parsed.Model); model != "" {
			attrs = append(attrs, AttrResponseModel.String(model))
		}
		if parsed.ID != "" {
			attrs = append(attrs, AttrResponseID.String(genai.Truncate(parsed.ID)))
		}
		if parsed.HasInputTokens {
			attrs = append(attrs, AttrInputTokens.Int64(parsed.InputTokens))
		}
		if parsed.HasOutputTokens {
			attrs = append(attrs, AttrOutputTokens.Int64(parsed.OutputTokens))
		}
		if t := parsed.Timings; t != nil {
			if t.HasPromptMS {
				attrs = append(attrs, AttrTimingsPromptMS.Float64(t.PromptMS))
			}
			if t.HasPredictedMS {
				attrs = append(attrs, AttrTimingsPredictedMS.Float64(t.PredictedMS))
			}
			if t.HasPredictedPerSecond {
				attrs = append(attrs, AttrTimingsPredictedPerSecond.Float64(t.PredictedPerSecond))
			}
			if t.HasCacheN {
				attrs = append(attrs, AttrTimingsCacheN.Int64(t.CacheN))
			}
		}
	}
	s.span.SetAttributes(attrs...)
}

func (s *requestState) finishReasons(res *bodyResult, parsed genai.Response, parsedOK bool) []string {
	seen := make(map[string]struct{})
	reasons := make([]string, 0, 4)
	add := func(reason string) {
		if reason == "" {
			return
		}
		if _, done := seen[reason]; done {
			return
		}
		seen[reason] = struct{}{}
		reasons = append(reasons, reason)
	}
	for _, output := range res.StreamOutputs {
		add(output.FinishReason)
	}
	if parsedOK {
		for _, reason := range parsed.FinishReasons {
			add(reason)
		}
	}
	return reasons
}

func (s *requestState) errorMessage(parsed genai.Response, parsedOK bool, out Outcome, res *bodyResult) string {
	switch {
	case s.panicErr != nil:
		return genai.Truncate("handler panic: " + s.panicErr.Error())
	case s.upstreamErr != nil:
		return genai.Truncate(s.upstreamErr.Error())
	case parsedOK && parsed.Failed && parsed.Message != "":
		return parsed.Message
	case res.Err != nil:
		return genai.Truncate("upstream response body: " + res.Err.Error())
	default:
		return string(out)
	}
}

func (s *requestState) errorType(out Outcome) string {
	if out == OutcomeInternalError {
		return "panic"
	}
	if s.statusCode >= http.StatusBadRequest {
		return strconv.Itoa(s.statusCode)
	}
	return string(out)
}

func (s *requestState) recordLocalStatus(statusCode int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statusCode == 0 {
		s.statusCode = statusCode
	}
}

func (s *requestState) recordUpstreamFailure(statusCode int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = statusCode
	s.upstreamErr = err
	recordSpanError(s.span, err)
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(errors.New(genai.Truncate(err.Error())))
}

func toString(p any) string {
	if s, ok := p.(string); ok {
		return s
	}
	if err, ok := p.(error); ok {
		return err.Error()
	}
	return "non-error panic"
}
