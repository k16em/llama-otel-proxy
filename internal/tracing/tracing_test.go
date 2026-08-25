package tracing

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/config"
	"github.com/k16em/llama-otel-proxy/internal/genai"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func Test_samplerがremote_parentのsampledを引き継ぎunsampledでは比率で決めている(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("11111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("2222222222222222")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		ratio  float64
		flags  trace.TraceFlags
		remote bool
		want   sdktrace.SamplingDecision
	}{
		{name: "ratio 1, no parent", ratio: 1, want: sdktrace.RecordAndSample},
		{name: "ratio 0, no parent", ratio: 0, want: sdktrace.Drop},
		{name: "ratio 1, remote parent sampled", ratio: 1, flags: trace.FlagsSampled, remote: true, want: sdktrace.RecordAndSample},
		{name: "ratio 1, remote parent NOT sampled", ratio: 1, remote: true, want: sdktrace.RecordAndSample},
		{name: "ratio 0, remote parent sampled", ratio: 0, flags: trace.FlagsSampled, remote: true, want: sdktrace.RecordAndSample},
		{name: "ratio 0, remote parent NOT sampled", ratio: 0, remote: true, want: sdktrace.Drop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			params := sdktrace.SamplingParameters{Name: "chat m1", Kind: trace.SpanKindServer}
			if tt.remote {
				sc := trace.NewSpanContext(trace.SpanContextConfig{
					TraceID:    traceID,
					SpanID:     spanID,
					TraceFlags: tt.flags,
					Remote:     true,
				})
				ctx = trace.ContextWithSpanContext(ctx, sc)
				params.ParentContext = ctx
			}
			got := sampler(tt.ratio).ShouldSample(params).Decision
			if got != tt.want {
				t.Errorf("decision = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_samplerがlocal_parentのsampling判断を引き継いでいる(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("33333333333333333333333333333333")
	spanID, _ := trace.SpanIDFromHex("4444444444444444")

	for _, tt := range []struct {
		name  string
		flags trace.TraceFlags
		want  sdktrace.SamplingDecision
	}{
		{name: "local parent sampled", flags: trace.FlagsSampled, want: sdktrace.RecordAndSample},
		{name: "local parent not sampled", want: sdktrace.Drop},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sc := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: tt.flags,
			})
			ctx := trace.ContextWithSpanContext(context.Background(), sc)

			got := sampler(0.5).ShouldSample(sdktrace.SamplingParameters{
				ParentContext: ctx,
				Name:          "time_to_first_token",
				Kind:          trace.SpanKindInternal,
			}).Decision
			if got != tt.want {
				t.Errorf("decision = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_contextで指定したtrace_IDを新しいroot_spanへ設定できる(t *testing.T) {
	want, err := trace.TraceIDFromHex("11111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	gen := NewIDGenerator()
	ctx := ContextWithTraceID(context.Background(), want)
	firstTraceID, firstSpanID := gen.NewIDs(ctx)
	secondTraceID, secondSpanID := gen.NewIDs(ctx)
	if firstTraceID != want || secondTraceID != want {
		t.Errorf("trace IDs = %s, %s, want %s", firstTraceID, secondTraceID, want)
	}
	if !firstSpanID.IsValid() || !secondSpanID.IsValid() || firstSpanID == secondSpanID {
		t.Errorf("span IDs = %s, %s", firstSpanID, secondSpanID)
	}
}

func Test_環境変数で明示したOpenTelemetry設定が上書きされていない(t *testing.T) {
	t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=from-env,injected=yes")
	t.Setenv("OTEL_SERVICE_NAME", "name-from-env")

	scrubResourceEnv(quietLogger())

	cfg := config.Defaults()
	cfg.OpenTelemetry.ServiceName = "name-from-config"
	cfg.OpenTelemetry.ResourceAttributes = map[string]string{"deployment.environment": "from-config"}

	opts, err := providerOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(append(opts, sdktrace.WithSpanProcessor(rec))...)

	_, span := tp.Tracer("test").Start(context.Background(), "chat m1")
	span.SetAttributes(attribute.String("gen_ai.provider.name", "llama.cpp"))
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("got %d spans", len(ended))
	}

	if len(ended[0].Attributes()) == 0 {
		t.Error("span attributes were dropped: OTEL_SPAN_* still applies")
	}

	res := ended[0].Resource()
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	if got["service.name"] != "name-from-config" {
		t.Errorf("service.name = %q, want the configured value", got["service.name"])
	}
	if got["deployment.environment"] != "from-config" {
		t.Errorf("deployment.environment = %q, want the configured value", got["deployment.environment"])
	}
	if _, ok := got["injected"]; ok {
		t.Error("OTEL_RESOURCE_ATTRIBUTES injected a label that no config file mentions")
	}
}

func Test_service_nameが空でも環境変数の値にfallbackしていない(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "name-from-env")
	scrubResourceEnv(quietLogger())

	cfg := config.Defaults()
	cfg.OpenTelemetry.ServiceName = ""
	res, err := buildResource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range res.Attributes() {
		if kv.Key == "service.name" && kv.Value.Emit() == "name-from-env" {
			t.Error("service.name came from the environment")
		}
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func Test_spanの上限値が環境変数に影響されていない(t *testing.T) {
	t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0")
	t.Setenv("OTEL_SPAN_EVENT_COUNT_LIMIT", "0")

	got := spanLimits()
	want := sdktrace.SpanLimits{
		AttributeValueLengthLimit:   sdktrace.DefaultAttributeValueLengthLimit,
		AttributeCountLimit:         spanAttributeCountLimit,
		EventCountLimit:             sdktrace.DefaultEventCountLimit,
		LinkCountLimit:              sdktrace.DefaultLinkCountLimit,
		AttributePerEventCountLimit: sdktrace.DefaultAttributePerEventCountLimit,
		AttributePerLinkCountLimit:  sdktrace.DefaultAttributePerLinkCountLimit,
	}
	if got != want {
		t.Errorf("spanLimits() = %+v, want %+v", got, want)
	}
}

// Attributes reach the collector as protobuf strings, which must be valid
// UTF-8. A message cut mid-rune fails to marshal and takes the whole batch with
// it, so this exports for real rather than only inspecting the attribute.
func Test_不正なUTF8を含むattributeがあってもspanがexportされている(t *testing.T) {
	var exported atomic.Int32
	var failed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			failed.Store(true)
		}
		exported.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		t.Errorf("SDK reported an export error: %v", err)
	}))

	cfg := config.Defaults()
	cfg.OpenTelemetry.Endpoint = srv.URL
	shutdown, err := Init(context.Background(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	_, span := otel.Tracer("test").Start(context.Background(), "chat m1")
	span.SetAttributes(
		attribute.String("llamaproxy.upstream_error", genai.Truncate(strings.Repeat("a", 255)+"€")),
		attribute.String("gen_ai.request.model", genai.Truncate(strings.Repeat("あ", 400))),
	)
	span.SetStatus(codes.Error, genai.Truncate(strings.Repeat("b", 250)+"🎉"))
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if exported.Load() == 0 {
		t.Fatal("nothing was exported")
	}
	if failed.Load() {
		t.Error("the exporter sent an empty body")
	}
}
