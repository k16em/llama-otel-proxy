package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func isolate(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)

	oldLocal, oldSystem := LocalPath, SystemPath
	SystemPath = filepath.Join(dir, "etc", "config.yaml")
	t.Cleanup(func() { LocalPath, SystemPath = oldLocal, oldSystem })
	if err := os.MkdirAll(filepath.Dir(SystemPath), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func Test_設定ファイルが存在しないときデフォルト設定が使われている(t *testing.T) {
	isolate(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Path != "" {
		t.Errorf("Path = %q, want empty", cfg.Path)
	}
	def := Defaults()
	if cfg.Listen != def.Listen || cfg.Upstream != def.Upstream ||
		cfg.SamplePercent != def.SamplePercent || cfg.ModelInSpanName {
		t.Errorf("got %+v", cfg)
	}
	if cfg.UpstreamURL == nil || cfg.UpstreamURL.Host != "127.0.0.1:8080" {
		t.Errorf("UpstreamURL = %v", cfg.UpstreamURL)
	}
}

func Test_明示した設定ファイルがデフォルトの探索先より優先されている(t *testing.T) {
	dir := isolate(t)
	explicit := filepath.Join(dir, "explicit.yaml")
	write(t, explicit, "listen: :1111\n")
	write(t, LocalPath, "listen: :2222\n")
	write(t, SystemPath, "listen: :3333\n")

	cfg, err := Load(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":1111" {
		t.Errorf("explicit path should win, got %q", cfg.Listen)
	}
	if cfg.Path != explicit {
		t.Errorf("Path = %q", cfg.Path)
	}

	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":2222" {
		t.Errorf("working directory should win over /etc, got %q", cfg.Listen)
	}

	if err := os.Remove(LocalPath); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":3333" {
		t.Errorf("should fall back to the system path, got %q", cfg.Listen)
	}
	if cfg.Path != SystemPath {
		t.Errorf("Path = %q", cfg.Path)
	}
}

func Test_明示した設定ファイルが存在しないときエラーになっている(t *testing.T) {
	dir := isolate(t)
	write(t, LocalPath, "listen: :2222\n")
	_, err := Load(filepath.Join(dir, "nope.yaml"))
	if err == nil {
		t.Fatal("want an error for a missing explicit path")
	}
	if !strings.Contains(err.Error(), "nope.yaml") {
		t.Errorf("error should name the path: %v", err)
	}
}

func Test_すべての設定項目が読み込まれている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, `
listen: 127.0.0.1:9090
upstream: https://swap.example:8443
sample_percent: 2.5
model_in_span_name: true
session_idle_timeout: 10m
otel:
  service_name: proxy-a
  protocol: grpc
  endpoint: http://collector:4317
  headers:
    authorization: Bearer xyz
  resource_attributes:
    deployment.environment: prod
`)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:9090" || cfg.Upstream != "https://swap.example:8443" {
		t.Errorf("got %+v", cfg)
	}
	if cfg.SamplePercent != 2.5 || cfg.SampleRatio() != 0.025 {
		t.Errorf("sample = %v", cfg.SamplePercent)
	}
	if !cfg.ModelInSpanName {
		t.Error("model_in_span_name should be true")
	}
	if cfg.SessionIdleTimeout != 10*time.Minute {
		t.Errorf("session_idle_timeout = %v", cfg.SessionIdleTimeout)
	}
	if cfg.OpenTelemetry.ServiceName != "proxy-a" || cfg.OpenTelemetry.Endpoint != "http://collector:4317" {
		t.Errorf("otel = %+v", cfg.OpenTelemetry)
	}
	if cfg.OpenTelemetry.Protocol != OTLPProtocolGRPC {
		t.Errorf("otel.protocol = %q", cfg.OpenTelemetry.Protocol)
	}
	if cfg.OpenTelemetry.Headers["authorization"] != "Bearer xyz" {
		t.Errorf("headers = %v", cfg.OpenTelemetry.Headers)
	}
	if cfg.OpenTelemetry.ResourceAttributes["deployment.environment"] != "prod" {
		t.Errorf("resource_attributes = %v", cfg.OpenTelemetry.ResourceAttributes)
	}
}

func Test_不正なOpenTelemetryヘッダーを指定したとき値を露出せずエラーになっている(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "empty name", header: `"": secret`, want: "invalid header name"},
		{name: "invalid name", header: `"bad name": secret`, want: "invalid header name"},
		{name: "newline in value", header: `authorization: "Bearer secret\nsecond"`, want: "invalid header value"},
		{name: "delete in value", header: `authorization: "Bearer secret\u007f"`, want: "invalid header value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolate(t)
			write(t, LocalPath, "otel:\n  headers:\n    "+tt.header+"\n")
			_, err := Load("")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("Load error exposed header contents: %v", err)
			}
		})
	}
}

func Test_一部の設定だけ指定したとき未指定項目にデフォルト値が保持されている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "listen: :9999\n")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Upstream != Defaults().Upstream {
		t.Errorf("upstream = %q, want the default", cfg.Upstream)
	}
	if cfg.SamplePercent != 100 || cfg.ModelInSpanName {
		t.Errorf("got %+v", cfg)
	}
}

func Test_0とfalseを明示したとき指定値が保持されている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "sample_percent: 0\ntrust_incoming_trace_context: false\n")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SamplePercent != 0 || cfg.SampleRatio() != 0 {
		t.Errorf("sample_percent = %v, want 0", cfg.SamplePercent)
	}
	if cfg.TrustTraceContext {
		t.Error("trust_incoming_trace_context = true, want false")
	}
}

func Test_空の設定ファイルを読み込んだときデフォルト設定が使われている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != Defaults().Listen {
		t.Errorf("an empty file should mean defaults, got %+v", cfg)
	}
}

func Test_未知の設定キーを指定したときエラーになっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "listen: :8081\nsample_percentage: 50\n")
	_, err := Load("")
	if err == nil {
		t.Fatal("want an error for an unknown key")
	}
	if strings.Contains(err.Error(), "sample_percentage") {
		t.Errorf("the key comes from the file and must not be echoed: %v", err)
	}
	for _, want := range []string{"line 2", "config.Config"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}
}

func Test_不正なYAMLを読み込んだとき設定ファイルを示すエラーになっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "listen: [unclosed\n")
	_, err := Load("")
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), LocalPath) {
		t.Errorf("error should name the file: %v", err)
	}
}

func Test_不正な設定値を読み込んだとき該当項目を示すエラーになっている(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"sample too low", "sample_percent: -1\n", "sample_percent"},
		{"sample too high", "sample_percent: 100.1\n", "sample_percent"},
		{"empty listen", "listen: \"\"\n", "listen"},
		{"empty upstream", "upstream: \"\"\n", "upstream"},
		{"upstream without scheme", "upstream: 127.0.0.1:8080\n", "upstream"},
		{"upstream bad scheme", "upstream: ftp://host\n", "upstream"},
		{"upstream no host", "upstream: http://\n", "upstream"},
		{"otel endpoint bad scheme", "otel:\n  endpoint: ftp://host\n", "otel.endpoint"},
		{"otel endpoint no host", "otel:\n  endpoint: http://\n", "otel.endpoint"},
		{"session idle timeout zero", "session_idle_timeout: 0s\n", "session_idle_timeout"},
		{"session idle timeout negative", "session_idle_timeout: -1s\n", "session_idle_timeout"},
		{"unknown otel protocol", "otel:\n  protocol: thrift\n", "otel.protocol"},
		{"grpc endpoint with a path", "otel:\n  protocol: grpc\n  endpoint: http://collector:4317/v1/traces\n", "otel.endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolate(t)
			write(t, LocalPath, tt.yaml)
			_, err := Load("")
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error should mention %q: %v", tt.want, err)
			}
			if !strings.Contains(err.Error(), LocalPath) {
				t.Errorf("error should name the file: %v", err)
			}
		})
	}
}

func Test_型が異なる設定値を読み込んだとき値を露出せずエラーになっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "sample_percent: abc\n")
	_, err := Load("")
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{LocalPath, "line 1", "float64"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}

	if strings.Contains(err.Error(), "abc") {
		t.Errorf("error echoes the offending value: %v", err)
	}
}

func Test_有効なサンプリング率が受け入れられている(t *testing.T) {
	for _, v := range []string{"0", "50", "100", "2.5"} {
		isolate(t)
		write(t, LocalPath, "sample_percent: "+v+"\n")
		if _, err := Load(""); err != nil {
			t.Errorf("sample_percent %s rejected: %v", v, err)
		}
	}
}

func Test_OpenTelemetryエンドポイントが空でも設定が受け入れられている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "otel:\n  endpoint: \"\"\n")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpenTelemetry.Endpoint != "" {
		t.Errorf("endpoint = %q", cfg.OpenTelemetry.Endpoint)
	}
}

func Test_デフォルトの待受アドレスがループバックになっている(t *testing.T) {
	isolate(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:8081" {
		t.Errorf("default listen = %q, want 127.0.0.1:8081", cfg.Listen)
	}
}

func Test_サンプリング率にNaNかInfを指定したときエラーになっている(t *testing.T) {
	for _, v := range []string{".nan", ".inf", "-.inf"} {
		t.Run(v, func(t *testing.T) {
			isolate(t)
			write(t, LocalPath, "sample_percent: "+v+"\n")
			_, err := Load("")
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), "sample_percent") {
				t.Errorf("error should name the field: %v", err)
			}
		})
	}
}

func Test_複数のYAMLドキュメントを指定したときエラーになっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "listen: :8081\n---\nlisten: :9090\nbogus: true\n")
	_, err := Load("")
	if err == nil {
		t.Fatal("want an error for a second document")
	}
	if !strings.Contains(err.Error(), "one YAML document") {
		t.Errorf("unexpected error: %v", err)
	}
}

func Test_UpstreamURLに認証情報を含めたとき値を露出せずエラーになっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "upstream: http://user:secret@127.0.0.1:8080\n")
	_, err := Load("")
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("the error itself leaked the credential: %v", err)
	}
}

func Test_秘密情報を含む設定ファイルの権限が安全でないとき露出状態になっている(t *testing.T) {
	tests := []struct {
		name        string
		mode        os.FileMode
		headers     bool
		wantExposed bool
	}{
		{name: "0600 with headers", mode: 0o600, headers: true},
		{name: "0640 with headers", mode: 0o640, headers: true, wantExposed: true},
		{name: "0644 with headers", mode: 0o644, headers: true, wantExposed: true},
		{name: "0644 without headers", mode: 0o644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolate(t)
			body := "listen: :8081\n"
			if tt.headers {
				body += "otel:\n  headers:\n    authorization: Bearer secret\n"
			}
			write(t, LocalPath, body)
			if err := os.Chmod(LocalPath, tt.mode); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load("")
			if err != nil {
				t.Fatal(err)
			}
			mode, exposed := cfg.SecretsExposed()
			if exposed != tt.wantExposed {
				t.Errorf("exposed = %v (mode %04o), want %v", exposed, mode, tt.wantExposed)
			}
		})
	}
}

func Test_OTLPプロトコルの表記ゆれが正規化されている(t *testing.T) {
	tests := map[string]string{
		"":              OTLPProtocolHTTP,
		"  ":            OTLPProtocolHTTP,
		"http":          OTLPProtocolHTTP,
		"https":         OTLPProtocolHTTP,
		"http/protobuf": OTLPProtocolHTTP,
		"HTTP/protobuf": OTLPProtocolHTTP,
		"grpc":          OTLPProtocolGRPC,
		"gRPC":          OTLPProtocolGRPC,
		" grpc ":        OTLPProtocolGRPC,
		"thrift":        "thrift",
	}
	for in, want := range tests {
		if got := NormalizeOTLPProtocol(in); got != want {
			t.Errorf("NormalizeOTLPProtocol(%q) = %q, want %q", in, got, want)
		}
	}
}

func Test_OTLPプロトコル未指定のときHTTPが既定になっている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "otel:\n  service_name: proxy-b\n")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OpenTelemetry.Protocol != OTLPProtocolHTTP {
		t.Errorf("otel.protocol = %q, want %q", cfg.OpenTelemetry.Protocol, OTLPProtocolHTTP)
	}
}

func Test_grpcのendpointにパスがなければ受け入れられている(t *testing.T) {
	isolate(t)
	write(t, LocalPath, "otel:\n  protocol: grpc\n  endpoint: http://collector:4317\n")
	if _, err := Load(""); err != nil {
		t.Fatal(err)
	}
}
