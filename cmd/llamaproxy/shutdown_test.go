package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeServer struct {
	drainErr   error
	drainDelay time.Duration
	closed     bool

	drainCtxDeadline time.Duration
}

func (f *fakeServer) Shutdown(ctx context.Context) error {
	if dl, ok := ctx.Deadline(); ok {
		f.drainCtxDeadline = time.Until(dl)
	}
	if f.drainDelay > 0 {
		select {
		case <-time.After(f.drainDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.drainErr
}

func (f *fakeServer) Close() error {
	f.closed = true
	return nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeHandler struct {
	drainCalled    bool
	forcedCalled   bool
	sessionsClosed bool
	closedAt       time.Time
	release        chan struct{}
	waitedAt       time.Time
	waitErr        error
}

func (f *fakeHandler) BeginDrain() { f.drainCalled = true }

func (f *fakeHandler) CloseSessions() {
	f.sessionsClosed = true
	f.closedAt = time.Now()
}

func (f *fakeHandler) BeginForcedClose() { f.forcedCalled = true }

func (f *fakeHandler) WaitIdle(ctx context.Context) error {
	if f.release == nil {
		f.waitedAt = time.Now()
		return nil
	}
	select {
	case <-f.release:
		f.waitedAt = time.Now()
		return nil
	case <-ctx.Done():
		f.waitErr = ctx.Err()
		return ctx.Err()
	}
}

func Test_TracerProviderのflushに専用のタイムアウトが割り当てられている(t *testing.T) {
	srv := &fakeServer{drainErr: context.DeadlineExceeded}
	var flushDeadline time.Duration
	var flushExpired bool

	shutdownServer(srv, &fakeHandler{}, func(ctx context.Context) error {
		if dl, ok := ctx.Deadline(); ok {
			flushDeadline = time.Until(dl)
		}
		flushExpired = ctx.Err() != nil
		return nil
	}, quiet())

	if flushExpired {
		t.Fatal("the flush was handed an already-expired context")
	}
	if flushDeadline < flushTimeout/2 {
		t.Errorf("flush budget = %v, want close to %v", flushDeadline, flushTimeout)
	}
	if srv.drainCtxDeadline < drainTimeout/2 {
		t.Errorf("drain budget = %v, want close to %v", srv.drainCtxDeadline, drainTimeout)
	}
}

func Test_drainがタイムアウトしたとき強制終了が開始されている(t *testing.T) {
	srv := &fakeServer{drainErr: context.DeadlineExceeded}
	var flushed bool
	h := &fakeHandler{}
	shutdownServer(srv, h, func(context.Context) error { flushed = true; return nil }, quiet())
	if !h.drainCalled {
		t.Error("want BeginDrain before the drain starts")
	}
	if !h.forcedCalled {
		t.Error("want BeginForcedClose before connections are closed by force")
	}

	if !srv.closed {
		t.Error("want Close() after a drain timeout")
	}
	if !flushed {
		t.Error("want the flush to run even after a drain timeout")
	}
}

func Test_drainが正常完了したとき強制終了が開始されていない(t *testing.T) {
	srv := &fakeServer{}
	h := &fakeHandler{}
	shutdownServer(srv, h, func(context.Context) error { return nil }, quiet())
	if srv.closed {
		t.Error("Close() must not be called when the drain succeeded")
	}
	if h.forcedCalled {
		t.Error("nothing was closed by force, so nothing should be attributed to one")
	}
}

func Test_Serverのshutdown完了後もhandlerの終了が待機されている(t *testing.T) {
	srv := &fakeServer{}
	h := &fakeHandler{release: make(chan struct{})}
	flushed := make(chan struct{})
	done := make(chan struct{})
	go func() {
		shutdownServer(srv, h, func(context.Context) error {
			close(flushed)
			return nil
		}, quiet())
		close(done)
	}()

	select {
	case <-flushed:
		t.Fatal("tracing was flushed while a hijacked handler was still active")
	case <-time.After(50 * time.Millisecond):
	}
	close(h.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after the handler became idle")
	}
	if srv.closed {
		t.Error("Close() must not be called after handlers finish within the drain budget")
	}
	if h.forcedCalled {
		t.Error("a completed handler must not be attributed to forced close")
	}
}

func Test_TracerProviderのflushが失敗してもshutdown処理が完了している(t *testing.T) {
	srv := &fakeServer{}
	h := &fakeHandler{}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	flushed := false
	shutdownServer(srv, h, func(ctx context.Context) error {
		flushed = true
		if _, ok := ctx.Deadline(); !ok {
			t.Error("flush must run under a deadline")
		}
		return errors.New("exporter down")
	}, logger)

	if !flushed {
		t.Error("the exporter was never flushed")
	}
	if !h.drainCalled {
		t.Error("the handler was not drained")
	}
	if h.forcedCalled || srv.closed {
		t.Error("a clean drain must not force connections closed")
	}
	if !strings.Contains(buf.String(), "exporter down") {
		t.Errorf("the flush failure was not logged: %q", buf.String())
	}
}

func Test_handlerの終了後にTracerProviderがflushされている(t *testing.T) {
	srv := &fakeServer{drainErr: context.DeadlineExceeded}
	h := &fakeHandler{release: make(chan struct{})}

	var flushedAt time.Time
	done := make(chan struct{})
	go func() {
		shutdownServer(srv, h, func(context.Context) error {
			flushedAt = time.Now()
			return nil
		}, quiet())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("the flush ran before the handlers had unwound")
	default:
	}
	close(h.release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not complete")
	}
	if flushedAt.Before(h.waitedAt) {
		t.Errorf("flush at %v ran before handlers finished at %v", flushedAt, h.waitedAt)
	}
}

func Test_handlerの終了待機時間に上限が設定されている(t *testing.T) {
	srv := &fakeServer{drainErr: context.DeadlineExceeded}
	h := &fakeHandler{release: make(chan struct{})}
	var flushed bool

	done := make(chan struct{})
	go func() {
		shutdownServer(srv, h, func(context.Context) error { flushed = true; return nil }, quiet())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(unwindTimeout + 5*time.Second):
		t.Fatal("shutdown blocked past the unwind budget")
	}
	if !flushed {
		t.Error("the flush must still run after the unwind budget expires")
	}
	if h.waitErr == nil {
		t.Error("want the bounded wait to report its timeout")
	}
}

func Test_session_spanがhandlerの停止後flushの前に閉じられている(t *testing.T) {
	srv := &fakeServer{}
	h := &fakeHandler{}
	var flushedAt time.Time
	shutdownServer(srv, h, func(context.Context) error {
		flushedAt = time.Now()
		return nil
	}, quiet())

	if !h.sessionsClosed {
		t.Fatal("want CloseSessions so open session spans are exported")
	}
	if h.closedAt.Before(h.waitedAt) {
		t.Error("session spans were closed before in-flight requests finished")
	}
	if flushedAt.Before(h.closedAt) {
		t.Error("the flush ran before session spans were closed; they would be lost")
	}
}

func Test_強制終了のあともsession_spanが閉じられている(t *testing.T) {
	srv := &fakeServer{drainErr: context.DeadlineExceeded}
	h := &fakeHandler{}
	shutdownServer(srv, h, func(context.Context) error { return nil }, quiet())
	if !h.forcedCalled {
		t.Fatal("want BeginForcedClose on a drain timeout")
	}
	if !h.sessionsClosed {
		t.Error("want CloseSessions even after a forced close")
	}
}
