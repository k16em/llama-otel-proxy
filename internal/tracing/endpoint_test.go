package tracing

import (
	"context"
	"testing"

	"github.com/k16em/llama-otel-proxy/internal/config"
)

func Test_OpenTelemetryエンドポイントからtraces送信先URLが正規化されている(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:4318":             "http://127.0.0.1:4318/v1/traces",
		"http://127.0.0.1:4318/":            "http://127.0.0.1:4318/v1/traces",
		"https://collector.example":         "https://collector.example/v1/traces",
		"https://gw.example/otlp/v1/traces": "https://gw.example/otlp/v1/traces",
		"http://127.0.0.1:4318/custom":      "http://127.0.0.1:4318/custom",
	}
	for in, want := range tests {
		got, err := tracesEndpoint(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got != want {
			t.Errorf("tracesEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func Test_grpcのtraces送信先URLからパスが付与されていない(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:4317":     "http://127.0.0.1:4317",
		"http://127.0.0.1:4317/":    "http://127.0.0.1:4317",
		"https://collector.example": "https://collector.example",
	}
	for in, want := range tests {
		got, err := grpcEndpoint(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got != want {
			t.Errorf("grpcEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func Test_grpcの送信先にパスを指定したときエラーになっている(t *testing.T) {
	for _, in := range []string{
		"http://127.0.0.1:4317/v1/traces",
		"https://gw.example/otlp",
	} {
		if _, err := grpcEndpoint(in); err == nil {
			t.Errorf("grpcEndpoint(%q) should reject a path", in)
		}
	}
}

func Test_不正な送信先がプロトコルによらず拒否されている(t *testing.T) {
	for _, in := range []string{
		"ftp://collector.example",
		"http://",
		"https://user:pass@collector.example",
		"http://collector.example?token=x",
	} {
		if _, err := tracesEndpoint(in); err == nil {
			t.Errorf("tracesEndpoint(%q) should be rejected", in)
		}
		if _, err := grpcEndpoint(in); err == nil {
			t.Errorf("grpcEndpoint(%q) should be rejected", in)
		}
	}
}

func Test_grpcのendpointにパスがあるときInitがエラーになっている(t *testing.T) {
	cfg := config.Defaults()
	cfg.OpenTelemetry.Protocol = config.OTLPProtocolGRPC
	cfg.OpenTelemetry.Endpoint = "http://127.0.0.1:4317/v1/traces"
	if _, err := Init(context.Background(), cfg, quietLogger()); err == nil {
		t.Fatal("want an error naming otel.endpoint")
	}
}
