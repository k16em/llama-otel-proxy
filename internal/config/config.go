package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

var (
	LocalPath = "config.yaml"

	SystemPath = "/etc/llama-otel-proxy/config.yaml"
)

type Config struct {
	Listen                string  `yaml:"listen"`
	Upstream              string  `yaml:"upstream"`
	SamplePercent         float64 `yaml:"sample_percent"`
	ModelInSpanName       bool    `yaml:"model_in_span_name"`
	TrustTraceContext     bool    `yaml:"trust_incoming_trace_context"`
	MaxConcurrentRequests int     `yaml:"max_concurrent_requests"`

	MaxConcurrentPassthroughRequests int `yaml:"max_concurrent_passthrough_requests"`

	RequestBodyLimitMiB int `yaml:"request_body_limit_mib"`

	RequestReadTimeout time.Duration `yaml:"request_read_timeout"`
	SessionIdleTimeout time.Duration `yaml:"session_idle_timeout"`
	OpenTelemetry      OpenTelemetry `yaml:"otel"`

	UpstreamURL *url.URL `yaml:"-"`

	Path string `yaml:"-"`
}

type OpenTelemetry struct {
	ServiceName        string            `yaml:"service_name"`
	Protocol           string            `yaml:"protocol"`
	Endpoint           string            `yaml:"endpoint"`
	Headers            map[string]string `yaml:"headers"`
	ResourceAttributes map[string]string `yaml:"resource_attributes"`
}

func Defaults() Config {
	return Config{

		Listen:                "127.0.0.1:8081",
		Upstream:              "http://127.0.0.1:8080",
		SamplePercent:         100,
		ModelInSpanName:       false,
		TrustTraceContext:     true,
		MaxConcurrentRequests: 16,

		MaxConcurrentPassthroughRequests: 128,

		RequestBodyLimitMiB: 4,

		RequestReadTimeout: 30 * time.Second,
		SessionIdleTimeout: 5 * time.Minute,
		OpenTelemetry: OpenTelemetry{
			ServiceName: "llamaproxy",
			Protocol:    OTLPProtocolHTTP,
			Endpoint:    "http://127.0.0.1:4318",
		},
	}
}

func Load(explicitPath string) (Config, error) {
	path, err := find(explicitPath)
	if err != nil {
		return Config{}, err
	}

	cfg := Defaults()
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return Config{}, fmt.Errorf("config %s: %w", path, err)
		}
		defer f.Close()
		if cfg, err = parse(f, cfg); err != nil {
			return Config{}, fmt.Errorf("config %s: %w", path, err)
		}
		cfg.Path = path
	}

	cfg.OpenTelemetry.Protocol = NormalizeOTLPProtocol(cfg.OpenTelemetry.Protocol)

	if err := cfg.validate(); err != nil {
		if path != "" {
			return Config{}, fmt.Errorf("config %s: %w", path, err)
		}
		return Config{}, err
	}
	return cfg, nil
}

func find(explicitPath string) (string, error) {
	if explicitPath != "" {
		if _, err := os.Stat(explicitPath); err != nil {
			return "", fmt.Errorf("config %s: %w", explicitPath, err)
		}
		return explicitPath, nil
	}
	for _, p := range []string{LocalPath, SystemPath} {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		} else if !errors.Is(err, os.ErrNotExist) {

			return "", fmt.Errorf("config %s: %w", p, err)
		}
	}
	return "", nil
}

func parse(r io.Reader, base Config) (Config, error) {
	dec := yaml.NewDecoder(r)

	dec.KnownFields(true)
	if err := dec.Decode(&base); err != nil {
		if errors.Is(err, io.EOF) {
			return base, nil
		}
		return Config{}, redactYAMLError(err)
	}

	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return Config{}, redactYAMLError(err)
		}
		return Config{}, errors.New("file contains more than one YAML document")
	}
	return base, nil
}

var (
	yamlLinePrefix   = regexp.MustCompile(`^line (\d+): (.*)$`)
	yamlCannotType   = regexp.MustCompile(`^cannot unmarshal (\S+)(?: .*)? into (\S+)$`)
	yamlUnknownField = regexp.MustCompile(`^field \S+ not found in type (\S+)$`)
)

var yamlStandardTags = map[string]bool{
	"!!str": true, "!!int": true, "!!float": true, "!!bool": true,
	"!!null": true, "!!seq": true, "!!map": true, "!!binary": true,
	"!!timestamp": true, "!!merge": true,
}

func redactYAMLError(err error) error {
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) {
		lines := make([]string, len(typeErr.Errors))
		for i, line := range typeErr.Errors {
			lines[i] = safeYAMLMessage(line)
		}
		return fmt.Errorf("yaml: %s", strings.Join(lines, "; "))
	}
	return fmt.Errorf("yaml: %s", safeYAMLMessage(strings.TrimPrefix(err.Error(), "yaml: ")))
}

func safeYAMLMessage(msg string) string {
	prefix := ""
	if m := yamlLinePrefix.FindStringSubmatch(msg); m != nil {
		prefix, msg = "line "+m[1]+": ", m[2]
	}
	if m := yamlCannotType.FindStringSubmatch(msg); m != nil {
		if yamlStandardTags[m[1]] {
			return prefix + "cannot unmarshal " + m[1] + " into " + m[2]
		}
		return prefix + "cannot unmarshal into " + m[2]
	}
	if m := yamlUnknownField.FindStringSubmatch(msg); m != nil {
		return prefix + "unknown key in type " + m[1]
	}
	return prefix + "invalid YAML"
}

func (c *Config) validate() error {
	if c.Listen == "" {
		return errors.New("listen: must not be empty")
	}

	if c.Upstream == "" {
		return errors.New("upstream: must not be empty")
	}
	u, err := validateHTTPURL("upstream", c.Upstream)
	if err != nil {
		return err
	}
	c.UpstreamURL = u

	if math.IsNaN(c.SamplePercent) || math.IsInf(c.SamplePercent, 0) {
		return fmt.Errorf("sample_percent: must be a finite number, got %v", c.SamplePercent)
	}
	if c.SamplePercent < 0 || c.SamplePercent > 100 {
		return fmt.Errorf("sample_percent: must be within 0..100, got %v", c.SamplePercent)
	}
	if c.MaxConcurrentRequests <= 0 {
		return errors.New("max_concurrent_requests: must be greater than zero")
	}
	if c.MaxConcurrentPassthroughRequests <= 0 {
		return errors.New("max_concurrent_passthrough_requests: must be greater than zero")
	}
	if c.RequestBodyLimitMiB <= 0 {
		return errors.New("request_body_limit_mib: must be greater than zero")
	}
	if c.RequestBodyLimitMiB > 512 {
		return fmt.Errorf("request_body_limit_mib: must be at most 512, got %d", c.RequestBodyLimitMiB)
	}
	if c.RequestReadTimeout <= 0 {
		return errors.New("request_read_timeout: must be greater than zero")
	}
	if c.SessionIdleTimeout <= 0 {
		return errors.New("session_idle_timeout: must be greater than zero")
	}
	if strings.TrimSpace(c.OpenTelemetry.ServiceName) == "" {
		return errors.New("otel.service_name: must not be empty")
	}
	if c.OpenTelemetry.Protocol != OTLPProtocolHTTP && c.OpenTelemetry.Protocol != OTLPProtocolGRPC {
		return fmt.Errorf("otel.protocol: must be %q or %q", OTLPProtocolHTTP, OTLPProtocolGRPC)
	}

	if c.OpenTelemetry.Endpoint != "" {
		u, err := validateHTTPURL("otel.endpoint", c.OpenTelemetry.Endpoint)
		if err != nil {
			return err
		}
		if c.OpenTelemetry.Protocol == OTLPProtocolGRPC && u.Path != "" && u.Path != "/" {
			return errors.New("otel.endpoint: must not carry a path when otel.protocol is grpc")
		}
	}
	for name, value := range c.OpenTelemetry.Headers {
		if !ValidHeaderName(name) {
			return errors.New("otel.headers: contains an invalid header name")
		}
		if !ValidHeaderValue(value) {
			return errors.New("otel.headers: contains an invalid header value")
		}
	}
	for key := range c.OpenTelemetry.ResourceAttributes {
		if key == "service.name" {
			return errors.New("otel.resource_attributes: service.name is reserved; set it through otel.service_name")
		}
		if strings.HasPrefix(key, "telemetry.sdk.") {
			return errors.New("otel.resource_attributes: telemetry.sdk.* is reserved for the SDK")
		}
	}
	return nil
}

func ValidHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	return true
}

func ValidHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] != '\t' && (value[i] < ' ' || value[i] == 0x7f) {
			return false
		}
	}
	return true
}

func validateHTTPURL(field, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid URL", field)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%s: must not contain credentials", field)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("%s: scheme must be http or https", field)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("%s: missing host", field)
	}
	if u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") {
		return nil, fmt.Errorf("%s: must not contain a query or fragment", field)
	}
	return u, nil
}

const (
	OTLPProtocolHTTP = "http/protobuf"
	OTLPProtocolGRPC = "grpc"
)

func NormalizeOTLPProtocol(protocol string) string {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "":
		return OTLPProtocolHTTP
	case "http", "https", "http/protobuf":
		return OTLPProtocolHTTP
	case "grpc":
		return OTLPProtocolGRPC
	default:
		return strings.TrimSpace(protocol)
	}
}

func (c Config) SampleRatio() float64 { return c.SamplePercent / 100 }

func (c Config) RequestBodyLimit() int64 { return int64(c.RequestBodyLimitMiB) << 20 }

func (c Config) SecretsExposed() (mode os.FileMode, exposed bool) {
	if c.Path == "" || !c.carriesSecrets() {
		return 0, false
	}
	info, err := os.Stat(c.Path)
	if err != nil {
		return 0, false
	}
	perm := info.Mode().Perm()
	return perm, perm&0o077 != 0
}

func (c Config) carriesSecrets() bool {
	return len(c.OpenTelemetry.Headers) > 0
}
