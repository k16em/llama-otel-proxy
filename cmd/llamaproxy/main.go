package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/config"
	"github.com/k16em/llama-otel-proxy/internal/proxy"
	"github.com/k16em/llama-otel-proxy/internal/tracing"
)

const (
	drainTimeout = 15 * time.Second

	unwindTimeout = 5 * time.Second
	flushTimeout  = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {

	configPath := flag.String("config", "", "path to the configuration file")
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "llamaproxy - OpenTelemetry-instrumented reverse proxy for llama-swap\n\n")
		fmt.Fprintf(out, "Usage:\n  llamaproxy [--config PATH]\n\n")
		fmt.Fprintf(out, "Options:\n")
		fmt.Fprintf(out, "  --config PATH\n")
		fmt.Fprintf(out, "        path to the configuration file.\n")
		fmt.Fprintf(out, "        Without it: ./%s, then %s.\n", config.LocalPath, config.SystemPath)
	}
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Path == "" {
		logger.Warn("no configuration file found, using defaults",
			"searched", []string{config.LocalPath, config.SystemPath})
	} else {
		logger.Info("configuration loaded", "path", cfg.Path)
	}
	if mode, exposed := cfg.SecretsExposed(); exposed {
		logger.Warn("configuration file holds otel.headers but is readable by other local users",
			"path", cfg.Path, "mode", fmt.Sprintf("%04o", mode), "want", "0600")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, cfg, logger)
	if err != nil {
		return err
	}

	handler := proxy.New(proxy.Options{
		Upstream:              cfg.UpstreamURL,
		Logger:                logger,
		ModelInSpanName:       cfg.ModelInSpanName,
		TrustTraceContext:     cfg.TrustTraceContext,
		SessionTraceIDRoots:   true,
		SessionIdleTimeout:    cfg.SessionIdleTimeout,
		MaxConcurrentRequests: cfg.MaxConcurrentRequests,
		RequestBodyLimit:      cfg.RequestBodyLimit(),

		MaxConcurrentPassthroughRequests: cfg.MaxConcurrentPassthroughRequests,
	})

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: handler,

		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       cfg.RequestReadTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	logger.Info("llamaproxy listening",
		"listen", cfg.Listen,
		"upstream", cfg.UpstreamURL.Redacted(),
		"sample_percent", cfg.SamplePercent,
		"model_in_span_name", cfg.ModelInSpanName,
		"max_concurrent_requests", cfg.MaxConcurrentRequests,
		"max_concurrent_passthrough_requests", cfg.MaxConcurrentPassthroughRequests,
		"request_body_limit_mib", cfg.RequestBodyLimitMiB,
		"request_read_timeout", cfg.RequestReadTimeout,
		"session_idle_timeout", cfg.SessionIdleTimeout,
		"otel_protocol", cfg.OpenTelemetry.Protocol,
	)

	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case err := <-errc:
		if err != nil {
			handler.CloseSessions()
			flushCtx, cancelFlush := context.WithTimeout(context.Background(), flushTimeout)
			shutdownErr := shutdownTracing(flushCtx)
			cancelFlush()
			if shutdownErr != nil {
				return errors.Join(err, fmt.Errorf("tracer shutdown: %w", shutdownErr))
			}
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
	}
	stop()

	shutdownServer(srv, handler, shutdownTracing, logger)
	logger.Info("stopped")
	return nil
}

func shutdownServer(srv server, h drainer, flush func(context.Context) error, logger *slog.Logger) {

	h.BeginDrain()

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), drainTimeout)
	defer cancelDrain()
	drainErr := srv.Shutdown(drainCtx)
	if drainErr == nil {
		drainErr = h.WaitIdle(drainCtx)
	}
	if drainErr != nil {

		logger.Warn("http drain did not finish, closing remaining connections", "err", drainErr)
		h.BeginForcedClose()
		if err := srv.Close(); err != nil {
			logger.Warn("http close", "err", err)
		}

		unwindCtx, cancelUnwind := context.WithTimeout(context.Background(), unwindTimeout)
		defer cancelUnwind()
		if err := h.WaitIdle(unwindCtx); err != nil {
			logger.Warn("handlers did not finish unwinding; some spans may be lost", "err", err)
		}
	}

	h.CloseSessions()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), flushTimeout)
	defer cancelFlush()
	if err := flush(flushCtx); err != nil {
		logger.Warn("tracer shutdown", "err", err)
	}
}

type server interface {
	Shutdown(context.Context) error
	Close() error
}

type drainer interface {
	BeginDrain()
	BeginForcedClose()
	WaitIdle(context.Context) error
	CloseSessions()
}
