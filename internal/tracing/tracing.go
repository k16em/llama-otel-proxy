package tracing

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const ScopeName = "github.com/k16em/llama-otel-proxy"

const spanAttributeCountLimit = 1024

func Init(ctx context.Context, cfg config.Config, logger *slog.Logger) (shutdown func(context.Context) error, err error) {
	scrubResourceEnv(logger)
	restoreEnv, err := prepareExporterEnvironment(cfg)
	if err != nil {
		return nil, err
	}
	defer restoreEnv()

	exporter, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	tpOpts, err := providerOptions(cfg)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(append(tpOpts,
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxQueueSize(256),
			sdktrace.WithMaxExportBatchSize(32),

			sdktrace.WithExportTimeout(30*time.Second),
		),
	)...)
	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if ratio := cfg.SampleRatio(); ratio < 1 {

		logger.Warn("sampling below 100% skews spanmetrics: call counts and latency histograms are computed from sampled spans only; prefer sampling downstream of the spanmetrics connector",
			"sample_percent", cfg.SamplePercent)
	}

	return tp.Shutdown, nil
}

func newExporter(ctx context.Context, cfg config.Config) (sdktrace.SpanExporter, error) {
	if cfg.OpenTelemetry.Protocol == config.OTLPProtocolGRPC {
		options := []otlptracegrpc.Option{}
		if cfg.OpenTelemetry.Endpoint != "" {
			endpoint, err := grpcEndpoint(cfg.OpenTelemetry.Endpoint)
			if err != nil {
				return nil, err
			}
			options = append(options, otlptracegrpc.WithEndpointURL(endpoint))
		}
		if len(cfg.OpenTelemetry.Headers) > 0 {
			options = append(options, otlptracegrpc.WithHeaders(cfg.OpenTelemetry.Headers))
		}
		exporter, err := otlptracegrpc.New(ctx, options...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		return exporter, nil
	}

	options := []otlptracehttp.Option{}
	if cfg.OpenTelemetry.Endpoint != "" {
		endpoint, err := tracesEndpoint(cfg.OpenTelemetry.Endpoint)
		if err != nil {
			return nil, err
		}
		options = append(options, otlptracehttp.WithEndpointURL(endpoint))
	}
	if len(cfg.OpenTelemetry.Headers) > 0 {
		options = append(options, otlptracehttp.WithHeaders(cfg.OpenTelemetry.Headers))
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	return exporter, nil
}

func prepareExporterEnvironment(cfg config.Config) (func(), error) {
	var masked []string
	if cfg.OpenTelemetry.Endpoint != "" {
		masked = append(masked, "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	}
	if len(cfg.OpenTelemetry.Headers) > 0 {
		masked = append(masked, "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS")
	}
	restore, err := maskEnvironment(masked)
	if err != nil {
		return func() {}, err
	}
	if err := validateExporterEnvironment(cfg.OpenTelemetry.Protocol); err != nil {
		restore()
		return func() {}, err
	}
	return restore, nil
}

func maskEnvironment(names []string) (func(), error) {
	type savedValue struct {
		name  string
		value string
	}
	saved := make([]savedValue, 0, len(names))
	for _, name := range names {
		value, ok := os.LookupEnv(name)
		if !ok {
			continue
		}
		if err := os.Unsetenv(name); err != nil {
			for _, entry := range saved {
				_ = os.Setenv(entry.name, entry.value)
			}
			return func() {}, fmt.Errorf("%s: cannot ignore environment value", name)
		}
		saved = append(saved, savedValue{name: name, value: value})
	}
	return func() {
		for _, entry := range saved {
			_ = os.Setenv(entry.name, entry.value)
		}
	}, nil
}

func validateExporterEnvironment(protocol string) error {
	normalize := tracesEndpoint
	if protocol == config.OTLPProtocolGRPC {
		normalize = grpcEndpoint
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			if _, err := normalize(value); err != nil {
				return fmt.Errorf("%s: invalid URL", name)
			}
		}
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" && !validHeaderEnvironment(value) {
			return fmt.Errorf("%s: invalid header configuration", name)
		}
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_TIMEOUT", "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			if _, err := strconv.Atoi(value); err != nil {
				return fmt.Errorf("%s: invalid timeout", name)
			}
		}
	}
	for _, name := range []string{"OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" && value != "gzip" && value != "none" {
			return fmt.Errorf("%s: invalid compression", name)
		}
	}
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
	} {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			continue
		}
		if _, err := os.ReadFile(value); err != nil {
			return fmt.Errorf("%s: cannot read configured file", name)
		}
	}
	return nil
}

func validHeaderEnvironment(value string) bool {
	for _, pair := range strings.Split(value, ",") {
		name, encoded, ok := strings.Cut(pair, "=")
		if !ok || !config.ValidHeaderName(strings.TrimSpace(name)) {
			return false
		}
		decoded, err := url.PathUnescape(encoded)
		if err != nil || !config.ValidHeaderValue(decoded) {
			return false
		}
	}
	return true
}

func grpcEndpoint(raw string) (string, error) {
	u, err := parseExporterEndpoint(raw)
	if err != nil {
		return "", err
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("otel.endpoint: must not carry a path when otel.protocol is grpc")
	}
	u.Path = ""
	return u.String(), nil
}

func tracesEndpoint(raw string) (string, error) {
	u, err := parseExporterEndpoint(raw)
	if err != nil {
		return "", err
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	}
	return u.String(), nil
}

func parseExporterEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("otel.endpoint: invalid URL")
	}
	if u.User != nil {
		return nil, fmt.Errorf("otel.endpoint: must not contain credentials")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("otel.endpoint: scheme must be http or https")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("otel.endpoint: missing host")
	}
	if u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") {
		return nil, fmt.Errorf("otel.endpoint: must not contain a query or fragment")
	}
	return u, nil
}

func SamplerForRatio(ratio float64) sdktrace.Sampler { return sampler(ratio) }

func sampler(ratio float64) sdktrace.Sampler {
	ratioBased := sdktrace.TraceIDRatioBased(ratio)
	return sdktrace.ParentBased(ratioBased,
		sdktrace.WithRemoteParentNotSampled(ratioBased),
	)
}

func SpanLimits() sdktrace.SpanLimits { return spanLimits() }

func spanLimits() sdktrace.SpanLimits {
	return sdktrace.SpanLimits{
		AttributeValueLengthLimit:   sdktrace.DefaultAttributeValueLengthLimit,
		AttributeCountLimit:         spanAttributeCountLimit,
		EventCountLimit:             sdktrace.DefaultEventCountLimit,
		LinkCountLimit:              sdktrace.DefaultLinkCountLimit,
		AttributePerEventCountLimit: sdktrace.DefaultAttributePerEventCountLimit,
		AttributePerLinkCountLimit:  sdktrace.DefaultAttributePerLinkCountLimit,
	}
}

func providerOptions(cfg config.Config) ([]sdktrace.TracerProviderOption, error) {
	res, err := buildResource(cfg)
	if err != nil {
		return nil, err
	}
	return []sdktrace.TracerProviderOption{

		sdktrace.WithRawSpanLimits(spanLimits()),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler(cfg.SampleRatio())),
		sdktrace.WithIDGenerator(NewIDGenerator()),
	}, nil
}

type requestedTraceIDKey struct{}

type contextualIDGenerator struct{}

func ContextWithTraceID(ctx context.Context, traceID trace.TraceID) context.Context {
	return context.WithValue(ctx, requestedTraceIDKey{}, traceID)
}

func NewIDGenerator() sdktrace.IDGenerator {
	return contextualIDGenerator{}
}

func (contextualIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	traceID, _ := ctx.Value(requestedTraceIDKey{}).(trace.TraceID)
	if !traceID.IsValid() {
		traceID = randomTraceID()
	}
	return traceID, randomSpanID()
}

func (contextualIDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	return randomSpanID()
}

func randomTraceID() trace.TraceID {
	for {
		var traceID trace.TraceID
		binary.NativeEndian.PutUint64(traceID[:8], rand.Uint64())
		binary.NativeEndian.PutUint64(traceID[8:], rand.Uint64())
		if traceID.IsValid() {
			return traceID
		}
	}
}

func randomSpanID() trace.SpanID {
	for {
		var spanID trace.SpanID
		binary.NativeEndian.PutUint64(spanID[:], rand.Uint64())
		if spanID.IsValid() {
			return spanID
		}
	}
}

func scrubResourceEnv(logger *slog.Logger) {
	for _, name := range []string{"OTEL_RESOURCE_ATTRIBUTES", "OTEL_SERVICE_NAME"} {
		if _, ok := os.LookupEnv(name); ok {
			logger.Warn("ignoring environment variable; configure this in the config file instead",
				"variable", name, "config_key", "otel.service_name / otel.resource_attributes")
			os.Unsetenv(name)
		}
	}
}

func buildResource(cfg config.Config) (*resource.Resource, error) {
	serviceName := cfg.OpenTelemetry.ServiceName
	if strings.TrimSpace(serviceName) == "" {
		serviceName = config.Defaults().OpenTelemetry.ServiceName
	}
	attrs := make([]attribute.KeyValue, 0, len(cfg.OpenTelemetry.ResourceAttributes)+4)
	for k, v := range cfg.OpenTelemetry.ResourceAttributes {
		if k == "service.name" || strings.HasPrefix(k, "telemetry.sdk.") {
			continue
		}
		attrs = append(attrs, attribute.String(k, v))
	}
	attrs = append(attrs,
		semconv.ServiceName(serviceName),
		semconv.TelemetrySDKName("opentelemetry"),
		semconv.TelemetrySDKLanguageGo,
		semconv.TelemetrySDKVersion(otel.Version()),
	)
	return resource.NewWithAttributes(semconv.SchemaURL, attrs...), nil
}
