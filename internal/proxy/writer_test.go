package proxy

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

type flushErrorResponseWriter struct {
	header http.Header
	err    error
	calls  int
}

func (w *flushErrorResponseWriter) Header() http.Header {
	return w.header
}

func (*flushErrorResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*flushErrorResponseWriter) WriteHeader(int) {}

func (w *flushErrorResponseWriter) FlushError() error {
	w.calls++
	return w.err
}

func Test_FlushErrorが返したエラーがobservedWriterに記録されている(t *testing.T) {
	want := errors.New("flush failed")
	underlying := &flushErrorResponseWriter{header: make(http.Header), err: want}
	observed, wrapped := newObservedWriter(underlying)
	w, ok := wrapped.(interface{ FlushError() error })
	if !ok {
		t.Fatal("flush-capable writer lost FlushError")
	}

	if got := w.FlushError(); !errors.Is(got, want) {
		t.Fatalf("FlushError() = %v, want %v", got, want)
	}
	if got := observed.writeErr(); !errors.Is(got, want) {
		t.Errorf("writeErr() = %v, want %v", got, want)
	}
	if underlying.calls != 1 {
		t.Errorf("FlushError calls = %d, want 1", underlying.calls)
	}
}

func Test_Flush経由でunderlyingのエラーがobservedWriterに記録されている(t *testing.T) {
	want := errors.New("flush failed")
	underlying := &flushErrorResponseWriter{header: make(http.Header), err: want}
	observed, wrapped := newObservedWriter(underlying)
	w, ok := wrapped.(http.Flusher)
	if !ok {
		t.Fatal("flush-capable writer lost Flusher")
	}

	w.Flush()

	if got := observed.writeErr(); !errors.Is(got, want) {
		t.Errorf("writeErr() = %v, want %v", got, want)
	}
	if underlying.calls != 1 {
		t.Errorf("FlushError calls = %d, want 1", underlying.calls)
	}
}

func Test_underlyingがflush未対応のときラッパーもflush未対応になっている(t *testing.T) {
	observed, wrapped := newObservedWriter(&plainResponseWriter{header: make(http.Header)})
	if _, ok := wrapped.(http.Flusher); ok {
		t.Fatal("plain writer unexpectedly gained Flusher")
	}
	if _, ok := wrapped.(interface{ FlushError() error }); ok {
		t.Fatal("plain writer unexpectedly gained FlushError")
	}
	if got := observed.writeErr(); got != nil {
		t.Errorf("writeErr() = %v, want nil", got)
	}
}

func Test_応答のheaderとbodyがsnapshotに保存されている(t *testing.T) {
	underlying := &plainResponseWriter{header: make(http.Header)}
	underlying.header.Add("X-Response-Value", "first")
	underlying.header.Add("X-Response-Value", "second")
	observed, wrapped := newObservedWriter(underlying)
	wrapped.WriteHeader(http.StatusCreated)
	if _, err := wrapped.Write([]byte("response body")); err != nil {
		t.Fatal(err)
	}
	snapshot := observed.snapshot()
	if got := snapshot.Header.Values("X-Response-Value"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("header = %v", got)
	}
	if got := string(snapshot.Body); got != "response body" {
		t.Errorf("body = %q", got)
	}
	if snapshot.BodyTruncated {
		t.Error("body was unexpectedly truncated")
	}
}

func Test_応答bodyの保存上限を超えても転送量が変わっていない(t *testing.T) {
	underlying := &plainResponseWriter{header: make(http.Header)}
	observed, wrapped := newObservedWriter(underlying)
	body := strings.Repeat("x", maxTracedBodySize+1)
	n, err := wrapped.Write([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(body) {
		t.Errorf("written = %d, want %d", n, len(body))
	}
	snapshot := observed.snapshot()
	if len(snapshot.Body) != maxTracedBodySize || !snapshot.BodyTruncated {
		t.Errorf("captured = %d, truncated = %v", len(snapshot.Body), snapshot.BodyTruncated)
	}
}

func Test_spanを記録しないとき応答のtrace用copyが作られていない(t *testing.T) {
	underlying := &plainResponseWriter{header: make(http.Header)}
	observed, wrapped := newObservedWriter(underlying)
	observed.setCapture(false)
	underlying.header.Set("X-Response-Value", "value")
	if _, err := wrapped.Write([]byte("response body")); err != nil {
		t.Fatal(err)
	}
	snapshot := observed.snapshot()
	if len(snapshot.Header) != 0 || len(snapshot.Body) != 0 {
		t.Errorf("snapshot = %+v", snapshot)
	}
}

func Test_body保存を無効にしても応答headerがsnapshotに保存されている(t *testing.T) {
	underlying := &plainResponseWriter{header: make(http.Header)}
	observed, wrapped := newObservedWriter(underlying)
	observed.disableBodyCapture()
	underlying.header.Set("X-Response-Value", "value")
	if _, err := wrapped.Write([]byte("response body")); err != nil {
		t.Fatal(err)
	}
	snapshot := observed.snapshot()
	if got := snapshot.Header.Get("X-Response-Value"); got != "value" {
		t.Errorf("header = %q", got)
	}
	if len(snapshot.Body) != 0 || snapshot.BodyTruncated {
		t.Errorf("body = %q, truncated = %v", snapshot.Body, snapshot.BodyTruncated)
	}
}

type plainResponseWriter struct {
	header http.Header
}

func (w *plainResponseWriter) Header() http.Header {
	return w.header
}

func (*plainResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*plainResponseWriter) WriteHeader(int) {}
