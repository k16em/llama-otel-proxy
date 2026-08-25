package tracing

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/k16em/llama-otel-proxy/internal/config"
)

func Test_不正なOpenTelemetryエンドポイントが秘密情報を含めず拒否されている(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		secret string
	}{
		{"malformed", "https://collector.example/%zz?token=malformed-secret", "malformed-secret"},
		{"userinfo", "https://user:userinfo-secret@collector.example", "userinfo-secret"},
		{"query", "https://collector.example/path?token=query-secret", "query-secret"},
		{"fragment", "https://collector.example/path#fragment-secret", "fragment-secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tracesEndpoint(tt.raw)
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), tt.secret) || strings.Contains(err.Error(), tt.raw) {
				t.Errorf("error exposed input: %v", err)
			}
		})
	}
}

func Test_不正なExporter環境変数のエラーに秘密情報が含まれていない(t *testing.T) {
	tests := []struct {
		name   string
		env    string
		value  string
		secret string
	}{
		{
			name:   "endpoint",
			env:    "OTEL_EXPORTER_OTLP_ENDPOINT",
			value:  "https://collector.example/%zz?token=environment-endpoint-secret",
			secret: "environment-endpoint-secret",
		},
		{
			name:   "headers",
			env:    "OTEL_EXPORTER_OTLP_HEADERS",
			value:  "authorization=Bearer%20ok,environment-header-secret",
			secret: "environment-header-secret",
		},
		{
			name:   "header value with leading newline",
			env:    "OTEL_EXPORTER_OTLP_HEADERS",
			value:  "authorization=%0Aenvironment-header-secret",
			secret: "environment-header-secret",
		},
		{
			name:   "header value with trailing carriage return",
			env:    "OTEL_EXPORTER_OTLP_HEADERS",
			value:  "authorization=environment-header-secret%0D",
			secret: "environment-header-secret",
		},
		{
			name:   "timeout",
			env:    "OTEL_EXPORTER_OTLP_TIMEOUT",
			value:  "environment-timeout-secret",
			secret: "environment-timeout-secret",
		},
		{
			name:   "compression",
			env:    "OTEL_EXPORTER_OTLP_COMPRESSION",
			value:  "environment-compression-secret",
			secret: "environment-compression-secret",
		},
		{
			name:   "certificate directory",
			env:    "OTEL_EXPORTER_OTLP_CERTIFICATE",
			value:  t.TempDir(),
			secret: "",
		},
	}

	for _, protocol := range []string{config.OTLPProtocolHTTP, config.OTLPProtocolGRPC} {
		for _, tt := range tests {
			t.Run(protocol+"/"+tt.name, func(t *testing.T) {
				t.Setenv(tt.env, tt.value)
				err := validateExporterEnvironment(protocol)
				if err == nil {
					t.Fatal("want an error")
				}
				if (tt.secret != "" && strings.Contains(err.Error(), tt.secret)) || strings.Contains(err.Error(), tt.value) {
					t.Errorf("error exposed input: %v", err)
				}
			})
		}
	}
}

func Test_Exporter設定を明示したとき不正な環境変数が参照されていない(t *testing.T) {
	endpointSecret := "https://collector.example/%zz?token=ignored-endpoint-secret"
	headerSecret := "ignored-header-secret"
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpointSecret)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", headerSecret)

	cfg := config.Defaults()
	cfg.OpenTelemetry.Headers = map[string]string{"authorization": "configured"}
	restore, err := prepareExporterEnvironment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		t.Error("endpoint environment was not masked")
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_HEADERS") != "" {
		t.Error("header environment was not masked")
	}
	restore()
	if got := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); got != endpointSecret {
		t.Errorf("endpoint environment was not restored: %q", got)
	}
	if got := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"); got != headerSecret {
		t.Errorf("header environment was not restored: %q", got)
	}
}

func Test_不正なfallback環境変数がExporter作成前に拒否されている(t *testing.T) {
	secret := "init-fallback-secret"
	value := "https://collector.example/%zz?token=" + secret
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", value)
	cfg := config.Defaults()
	cfg.OpenTelemetry.Endpoint = ""
	_, err := Init(context.Background(), cfg, quietLogger())
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), value) {
		t.Errorf("error exposed input: %v", err)
	}
}
