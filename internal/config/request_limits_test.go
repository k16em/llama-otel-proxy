package config

import (
	"strings"
	"testing"
	"time"
)

func Test_リクエスト上限にデフォルト値が設定されている(t *testing.T) {
	cfg := Defaults()
	if cfg.MaxConcurrentRequests != 16 {
		t.Errorf("max_concurrent_requests = %d", cfg.MaxConcurrentRequests)
	}
	if cfg.MaxConcurrentPassthroughRequests != 128 {
		t.Errorf("max_concurrent_passthrough_requests = %d", cfg.MaxConcurrentPassthroughRequests)
	}
	if cfg.RequestReadTimeout != 30*time.Second {
		t.Errorf("request_read_timeout = %s", cfg.RequestReadTimeout)
	}
	if cfg.RequestBodyLimitMiB != 4 || cfg.RequestBodyLimit() != 4<<20 {
		t.Errorf("request_body_limit_mib = %d (%d bytes)", cfg.RequestBodyLimitMiB, cfg.RequestBodyLimit())
	}
}

func Test_リクエスト本文上限の設定値が読み込まれている(t *testing.T) {
	cfg, err := parse(strings.NewReader("request_body_limit_mib: 32\n"), Defaults())
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.RequestBodyLimit() != 32<<20 {
		t.Errorf("request body limit = %d bytes", cfg.RequestBodyLimit())
	}
}

func Test_リクエスト本文上限が範囲外のとき起動が失敗している(t *testing.T) {
	for _, value := range []string{"0", "-1", "513"} {
		cfg, err := parse(strings.NewReader("request_body_limit_mib: "+value+"\n"), Defaults())
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.validate(); err == nil {
			t.Errorf("request_body_limit_mib: %s was accepted", value)
		} else if !strings.Contains(err.Error(), "request_body_limit_mib") {
			t.Errorf("error should name the key: %v", err)
		}
	}
}

func Test_リクエスト上限の設定値が読み込まれている(t *testing.T) {
	cfg, err := parse(strings.NewReader("max_concurrent_requests: 17\nmax_concurrent_passthrough_requests: 33\nrequest_read_timeout: 750ms\n"), Defaults())
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConcurrentRequests != 17 {
		t.Errorf("max_concurrent_requests = %d", cfg.MaxConcurrentRequests)
	}
	if cfg.MaxConcurrentPassthroughRequests != 33 {
		t.Errorf("max_concurrent_passthrough_requests = %d", cfg.MaxConcurrentPassthroughRequests)
	}
	if cfg.RequestReadTimeout != 750*time.Millisecond {
		t.Errorf("request_read_timeout = %s", cfg.RequestReadTimeout)
	}
}

func Test_リクエスト上限に0以下を指定したときエラーになっている(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		field string
	}{
		{"zero concurrency", "max_concurrent_requests: 0\n", "max_concurrent_requests"},
		{"negative concurrency", "max_concurrent_requests: -1\n", "max_concurrent_requests"},
		{"zero passthrough concurrency", "max_concurrent_passthrough_requests: 0\n", "max_concurrent_passthrough_requests"},
		{"negative passthrough concurrency", "max_concurrent_passthrough_requests: -1\n", "max_concurrent_passthrough_requests"},
		{"zero timeout", "request_read_timeout: 0s\n", "request_read_timeout"},
		{"negative timeout", "request_read_timeout: -1s\n", "request_read_timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parse(strings.NewReader(tt.body), Defaults())
			if err != nil {
				t.Fatal(err)
			}
			err = cfg.validate()
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tt.field) {
				t.Errorf("error does not mention %q: %v", tt.field, err)
			}
		})
	}
}
