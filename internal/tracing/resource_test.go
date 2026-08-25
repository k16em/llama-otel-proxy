package tracing

import (
	"testing"

	"github.com/k16em/llama-otel-proxy/internal/config"
	"go.opentelemetry.io/otel"
)

func Test_予約済みresource属性が利用者の設定で上書きされていない(t *testing.T) {
	cfg := config.Defaults()
	cfg.OpenTelemetry.ServiceName = "configured-service"
	cfg.OpenTelemetry.ResourceAttributes = map[string]string{
		"service.name":           "attacker-service",
		"telemetry.sdk.name":     "attacker-sdk",
		"telemetry.sdk.language": "attacker-language",
		"telemetry.sdk.version":  "attacker-version",
		"telemetry.sdk.future":   "attacker-future",
		"deployment.region":      "local",
	}

	res, err := buildResource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}

	want := map[string]string{
		"service.name":           "configured-service",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.language": "go",
		"telemetry.sdk.version":  otel.Version(),
		"deployment.region":      "local",
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q", key, got[key], value)
		}
	}
	if _, exists := got["telemetry.sdk.future"]; exists {
		t.Error("telemetry.sdk.future was accepted")
	}
}

func Test_service_nameが空のとき安全なfallback値がresourceに設定されている(t *testing.T) {
	cfg := config.Defaults()
	cfg.OpenTelemetry.ServiceName = ""
	res, err := buildResource(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "service.name" {
			if got := kv.Value.Emit(); got != config.Defaults().OpenTelemetry.ServiceName {
				t.Errorf("service.name = %q", got)
			}
			return
		}
	}
	t.Fatal("service.name is missing")
}
