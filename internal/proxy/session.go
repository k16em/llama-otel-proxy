package proxy

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const SpanSession = "session"

const (
	defaultSessionIdleTimeout = 5 * time.Minute
	maxTrackedSessions        = 1024
	maxSessionModels          = 32
)

const (
	SessionEndIdle     = "idle"
	SessionEndEvicted  = "evicted"
	SessionEndShutdown = "shutdown"
)

type sessionState struct {
	id        string
	span      trace.Span
	refs      int
	requests  int64
	last      time.Time
	models    []string
	userAgent string
	timer     *time.Timer
}

func (s *sessionState) observeModel(model string) {
	if model == "" || len(s.models) >= maxSessionModels || slices.Contains(s.models, model) {
		return
	}
	s.models = append(s.models, model)
}

type sessionRegistry struct {
	tracer trace.Tracer
	idle   time.Duration
	now    func() time.Time

	mu       sync.Mutex
	sessions map[string]*sessionState
	closed   bool
}

func newSessionRegistry(tracer trace.Tracer, idle time.Duration) *sessionRegistry {
	if idle <= 0 {
		idle = defaultSessionIdleTimeout
	}
	return &sessionRegistry{
		tracer:   tracer,
		idle:     idle,
		now:      time.Now,
		sessions: make(map[string]*sessionState),
	}
}

func (r *sessionRegistry) acquire(id string, traceID trace.TraceID, start time.Time, model, userAgent string) (trace.SpanContext, func(time.Time)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return trace.SpanContext{}, nil
	}
	state, live := r.sessions[id]
	if !live {
		r.evictLocked()
		ctx := tracing.ContextWithTraceID(context.Background(), traceID)
		_, span := r.tracer.Start(ctx, SpanSession,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(start),
			trace.WithNewRoot(),
			trace.WithAttributes(
				semconv.SessionID(id),
				AttrConversationID.String(id),
				AttrProviderName.String(System),
			),
		)
		state = &sessionState{id: id, span: span, last: start}
		r.sessions[id] = state
	}
	state.refs++
	state.requests++
	if start.After(state.last) {
		state.last = start
	}
	state.observeModel(model)
	if state.userAgent == "" {
		state.userAgent = userAgent
	}
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	return state.span.SpanContext(), func(at time.Time) { r.release(id, at) }
}

func (r *sessionRegistry) release(id string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, live := r.sessions[id]
	if !live {
		return
	}
	state.refs--
	if at.After(state.last) {
		state.last = at
	}
	if state.refs > 0 || r.closed {
		return
	}
	r.armLocked(state, r.idle)
}

func (r *sessionRegistry) armLocked(state *sessionState, after time.Duration) {
	if state.timer != nil {
		state.timer.Stop()
	}
	id := state.id
	state.timer = time.AfterFunc(after, func() { r.expire(id) })
}

func (r *sessionRegistry) expire(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, live := r.sessions[id]
	if !live || state.refs > 0 {
		return
	}
	if remaining := r.idle - r.now().Sub(state.last); remaining > 0 {
		r.armLocked(state, remaining)
		return
	}
	r.finishLocked(state, SessionEndIdle)
}

func (r *sessionRegistry) evictLocked() {
	for len(r.sessions) >= maxTrackedSessions {
		var oldest *sessionState
		for _, state := range r.sessions {
			if state.refs > 0 {
				continue
			}
			if oldest == nil || state.last.Before(oldest.last) {
				oldest = state
			}
		}
		if oldest == nil {
			return
		}
		r.finishLocked(oldest, SessionEndEvicted)
	}
}

func (r *sessionRegistry) finishLocked(state *sessionState, reason string) {
	delete(r.sessions, state.id)
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	attrs := []attribute.KeyValue{
		AttrSessionRequestCount.Int64(state.requests),
		AttrSessionEndReason.String(reason),
	}
	if len(state.models) > 0 {
		attrs = append(attrs, AttrSessionModels.StringSlice(state.models))
	}
	if state.userAgent != "" {
		attrs = append(attrs, semconv.UserAgentOriginal(state.userAgent))
	}
	state.span.SetAttributes(attrs...)
	state.span.End(trace.WithTimestamp(state.last))
}

func (r *sessionRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, state := range r.sessions {
		r.finishLocked(state, SessionEndShutdown)
	}
}
