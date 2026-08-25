package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_URL検証エラーに秘密の値が含まれていない(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		raw    string
		secret string
	}{
		{"upstream malformed", "upstream", "https://example.com/%zz?token=upstream-malformed-secret", "upstream-malformed-secret"},
		{"upstream bad scheme and userinfo", "upstream", "ftp://user:upstream-userinfo-secret@example.com", "upstream-userinfo-secret"},
		{"upstream missing host", "upstream", "https:?token=upstream-host-secret", "upstream-host-secret"},
		{"upstream query", "upstream", "https://example.com/path?token=upstream-query-secret", "upstream-query-secret"},
		{"upstream fragment", "upstream", "https://example.com/path#upstream-fragment-secret", "upstream-fragment-secret"},
		{"otel malformed", "otel.endpoint", "https://collector.example/%zz?token=otel-malformed-secret", "otel-malformed-secret"},
		{"otel bad scheme and userinfo", "otel.endpoint", "ftp://user:otel-userinfo-secret@collector.example", "otel-userinfo-secret"},
		{"otel missing host", "otel.endpoint", "https:?token=otel-host-secret", "otel-host-secret"},
		{"otel query", "otel.endpoint", "https://collector.example/path?token=otel-query-secret", "otel-query-secret"},
		{"otel fragment", "otel.endpoint", "https://collector.example/path#otel-fragment-secret", "otel-fragment-secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Defaults()
			if tt.field == "upstream" {
				cfg.Upstream = tt.raw
			} else {
				cfg.OpenTelemetry.Endpoint = tt.raw
			}
			err := cfg.validate()
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tt.field) {
				t.Errorf("error does not identify %s: %v", tt.field, err)
			}
			if strings.Contains(err.Error(), tt.secret) || strings.Contains(err.Error(), tt.raw) {
				t.Errorf("error exposed input: %v", err)
			}
		})
	}
}

func Test_OpenTelemetryの予約済みリソース属性を指定したときエラーになっている(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		want    string
		notWant string
	}{
		{"empty service name", func(c *Config) { c.OpenTelemetry.ServiceName = "" }, "otel.service_name", ""},
		{"whitespace service name", func(c *Config) { c.OpenTelemetry.ServiceName = " \t" }, "otel.service_name", ""},
		{"service name resource", func(c *Config) { c.OpenTelemetry.ResourceAttributes = map[string]string{"service.name": "other"} }, "service.name", ""},
		{"sdk name resource", func(c *Config) { c.OpenTelemetry.ResourceAttributes = map[string]string{"telemetry.sdk.name": "other"} }, "telemetry.sdk.*", "telemetry.sdk.name"},
		{"sdk future resource", func(c *Config) {
			c.OpenTelemetry.ResourceAttributes = map[string]string{"telemetry.sdk.secret": "other"}
		}, "telemetry.sdk.*", "telemetry.sdk.secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Defaults()
			tt.mutate(&cfg)
			err := cfg.validate()
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error does not mention %q: %v", tt.want, err)
			}
			if tt.notWant != "" && strings.Contains(err.Error(), tt.notWant) {
				t.Errorf("the key comes from the file and must not be echoed: %v", err)
			}
		})
	}
}

func Test_OpenTelemetryの通常のリソース属性が受け入れられている(t *testing.T) {
	cfg := Defaults()
	cfg.OpenTelemetry.ResourceAttributes = map[string]string{
		"deployment.environment.name": "prod",
		"service.namespace":           "local-ai",
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
}

func Test_YAMLの型エラーに秘密の値が含まれていない(t *testing.T) {
	const secret = "sk-live-0123456789abcdef"
	cases := map[string]string{
		"headers as a scalar":   "otel:\n  headers: " + secret + "\n",
		"endpoint as a list":    "otel:\n  endpoint: [" + secret + "]\n",
		"sample_percent as map": "sample_percent:\n  " + secret + ": 1\n",

		"backtick inside the value": "sample_percent: \"a`" + secret + "\"\n",

		"unknown alias": "sample_percent: *" + secret + "\n",
		"duplicate key": "listen: " + secret + "\nlisten: " + secret + "\n",
		"bad map key":   "otel:\n  resource_attributes:\n    ? [" + secret + "]\n    : 1\n",

		"custom tag":        "sample_percent: !!" + secret + " 1\n",
		"custom tag one !":  "sample_percent: !" + secret + " [1]\n",
		"custom tag on map": "otel:\n  headers: !!" + secret + " 1\n",

		"unknown key":             secret + ": 1\n",
		"unknown key with dashes": strings.ReplaceAll(secret, "-", "_") + "-x: 1\n",
		"unknown key under otel":  "otel:\n  " + secret + ": 1\n",
		"block scalar as key":     "? |\n  " + secret + "\n  " + secret + "\n: 1\n",
		"unterminated flow seq":   "listen: [" + secret + "\n",
		"invalid utf8 value":      "listen: \xff" + secret + "\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error exposes the value: %v", err)
			}
			for i := 4; i <= len(secret); i++ {
				if strings.Contains(err.Error(), secret[:i]) {
					t.Errorf("error exposes a %d-byte prefix of the value: %v", i, err)
					break
				}
			}
		})
	}
}

func Test_YAMLエラーを秘匿化しても診断情報が保持されている(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("otel:\n  headers: plain\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"line 2", "map[string]string"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lost %q", err, want)
		}
	}
}

func Test_標準YAMLタグを含むエラーが秘匿化後も判別できている(t *testing.T) {
	cases := map[string]string{
		"sample_percent: abc\n":     "!!str",
		"otel:\n  endpoint: [a]\n":  "!!seq",
		"sample_percent:\n  a: 1\n": "!!map",
	}
	for body, want := range cases {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path)
		if err == nil {
			t.Fatalf("%q: want an error", body)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q: error %q lost %q", body, err, want)
		}
	}
}

func Test_未知のキーのエラーに値を含めず位置情報が記録されている(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("listen: 127.0.0.1:1\nnope: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), "nope") {
		t.Errorf("the key comes from the file and must not be echoed: %v", err)
	}
	for _, want := range []string{"line 2", "config.Config"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lost %q", err, want)
		}
	}
}
