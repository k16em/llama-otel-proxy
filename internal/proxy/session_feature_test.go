package proxy

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func newSessionHarness(t *testing.T, idle time.Duration) (*sessionRegistry, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithIDGenerator(tracing.NewIDGenerator()),
	)
	t.Cleanup(func() { tp.Shutdown(t.Context()) })
	return newSessionRegistry(tp.Tracer("test"), idle), rec
}

func sessionTraceIDFor(t *testing.T, id string) trace.TraceID {
	t.Helper()
	traceID, ok := sessionTraceID(id)
	if !ok {
		t.Fatalf("session trace id for %q", id)
	}
	return traceID
}

func Test_セッションのSERVER_spanが1つのsession_spanへぶら下がっている(t *testing.T) {
	h := newHarness(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, nonStreamBody)
	}), nil)

	const sessionID = "ses_multi_model"
	for _, model := range []string{"m1", "m2", "m1"} {
		req, _ := http.NewRequest(http.MethodPost, h.proxy.URL+"/v1/chat/completions", strings.NewReader(`{"model":"`+model+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Session-Id", sessionID)
		req.Header.Set("User-Agent", "harness/1.0")
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
		return count == 3
	})
	h.handler.CloseSessions()

	sessions := h.endedByName(SpanSession)
	if len(sessions) != 1 {
		t.Fatalf("session spans = %d, want 1", len(sessions))
	}
	session := sessions[0]
	if session.Parent().IsValid() {
		t.Errorf("session parent = %s, want root span", session.Parent().SpanID())
	}
	if session.SpanKind() != trace.SpanKindInternal {
		t.Errorf("session kind = %v", session.SpanKind())
	}
	if got, want := session.SpanContext().TraceID(), sessionTraceIDFor(t, sessionID); got != want {
		t.Errorf("session trace id = %s, want %s", got, want)
	}

	served := 0
	for _, span := range h.spans.Ended() {
		if span.SpanKind() != trace.SpanKindServer {
			continue
		}
		served++
		if span.Parent().SpanID() != session.SpanContext().SpanID() {
			t.Errorf("%s parent = %s, want the session span", span.Name(), span.Parent().SpanID())
		}
	}
	if served != 3 {
		t.Errorf("server spans = %d, want 3", served)
	}

	if got := mustAttr(t, session, semconv.SessionIDKey).AsString(); got != sessionID {
		t.Errorf("session.id = %q", got)
	}
	if got := mustAttr(t, session, AttrSessionRequestCount).AsInt64(); got != 3 {
		t.Errorf("request count = %d, want 3", got)
	}
	if got := mustAttr(t, session, AttrSessionModels).AsStringSlice(); len(got) != 2 || got[0] != "m1" || got[1] != "m2" {
		t.Errorf("models = %v, want [m1 m2] in first-seen order", got)
	}
	if got := mustAttr(t, session, AttrSessionEndReason).AsString(); got != SessionEndShutdown {
		t.Errorf("end reason = %q", got)
	}
	if got := mustAttr(t, session, semconv.UserAgentOriginalKey).AsString(); got != "harness/1.0" {
		t.Errorf("user agent = %q", got)
	}
}

func Test_アイドルのsession_spanが待機時間経過後に終了している(t *testing.T) {
	registry, rec := newSessionHarness(t, 20*time.Millisecond)
	const sessionID = "ses_idle"
	start := time.Now()
	_, release := registry.acquire(sessionID, sessionTraceIDFor(t, sessionID), start, "m1", "agent/1")
	if release == nil {
		t.Fatal("acquire returned no release")
	}
	last := start.Add(5 * time.Millisecond)
	release(last)

	waitFor(t, func() bool { return len(rec.Ended()) == 1 })
	session := rec.Ended()[0]
	if session.Name() != SpanSession {
		t.Errorf("name = %q", session.Name())
	}
	if got := mustAttr(t, session, AttrSessionEndReason).AsString(); got != SessionEndIdle {
		t.Errorf("end reason = %q", got)
	}
	if !session.EndTime().Equal(last) {
		t.Errorf("end time = %s, want the last activity %s", session.EndTime(), last)
	}
}

func Test_進行中のリクエストがある間はsession_spanが終了していない(t *testing.T) {
	registry, rec := newSessionHarness(t, 10*time.Millisecond)
	const sessionID = "ses_busy"
	traceID := sessionTraceIDFor(t, sessionID)
	now := time.Now()
	_, first := registry.acquire(sessionID, traceID, now, "m1", "agent/1")
	_, second := registry.acquire(sessionID, traceID, now, "m1", "agent/1")
	first(now.Add(time.Millisecond))

	time.Sleep(50 * time.Millisecond)
	if len(rec.Ended()) != 0 {
		t.Fatalf("session ended while a request was in flight: %v", rec.Ended())
	}
	second(now.Add(2 * time.Millisecond))
	waitFor(t, func() bool { return len(rec.Ended()) == 1 })
	if got := mustAttr(t, rec.Ended()[0], AttrSessionRequestCount).AsInt64(); got != 2 {
		t.Errorf("request count = %d, want 2", got)
	}
}

func Test_追跡セッション数の上限を超えた古いsessionが終了している(t *testing.T) {
	registry, rec := newSessionHarness(t, time.Hour)
	base := time.Now()
	for i := range maxTrackedSessions {
		id := "ses_" + strconv.Itoa(i)
		at := base.Add(time.Duration(i) * time.Millisecond)
		_, release := registry.acquire(id, sessionTraceIDFor(t, id), at, "m1", "agent/1")
		release(at)
	}
	if len(rec.Ended()) != 0 {
		t.Fatalf("evicted below the limit: %d", len(rec.Ended()))
	}

	const overflow = "ses_overflow"
	_, release := registry.acquire(overflow, sessionTraceIDFor(t, overflow), base.Add(time.Hour), "m1", "agent/1")
	release(base.Add(time.Hour))

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended = %d, want the least recently used session", len(ended))
	}
	if got := mustAttr(t, ended[0], AttrSessionEndReason).AsString(); got != SessionEndEvicted {
		t.Errorf("end reason = %q", got)
	}
	if got := mustAttr(t, ended[0], semconv.SessionIDKey).AsString(); got != "ses_0" {
		t.Errorf("evicted session = %q, want the oldest", got)
	}
}
