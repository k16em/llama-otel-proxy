package proxy

import (
	"net/http"
	"sync"
)

type observedWriter struct {
	http.ResponseWriter

	mu            sync.Mutex
	err           error
	header        http.Header
	body          []byte
	bodyTruncated bool
	capture       bool
	bodyCapture   bool
}

type responseSnapshot struct {
	Header        http.Header
	Body          []byte
	BodyTruncated bool
}

type flushObservedWriter struct {
	*observedWriter
}

func newObservedWriter(underlying http.ResponseWriter) (*observedWriter, http.ResponseWriter) {
	observed := &observedWriter{ResponseWriter: underlying, capture: true, bodyCapture: true}
	if supportsFlush(underlying) {
		return observed, &flushObservedWriter{observedWriter: observed}
	}
	return observed, observed
}

func (w *observedWriter) Write(p []byte) (int, error) {
	w.captureHeader()
	n, err := w.ResponseWriter.Write(p)
	w.mu.Lock()
	if n > 0 {
		w.captureBody(p[:n])
	}
	if err != nil && w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
	return n, err
}

func (w *observedWriter) WriteHeader(statusCode int) {
	if statusCode < 100 || statusCode >= 200 {
		w.captureHeader()
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *flushObservedWriter) Flush() {
	_ = w.FlushError()
}

func (w *flushObservedWriter) FlushError() error {
	w.captureHeader()
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.recordError(err)
	return err
}

func (w *observedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *observedWriter) writeErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *observedWriter) snapshot() responseSnapshot {
	w.captureHeader()
	w.mu.Lock()
	defer w.mu.Unlock()
	return responseSnapshot{
		Header:        w.header.Clone(),
		Body:          append([]byte(nil), w.body...),
		BodyTruncated: w.bodyTruncated,
	}
}

func (w *observedWriter) setCapture(enabled bool) {
	w.mu.Lock()
	w.capture = enabled
	w.mu.Unlock()
}

func (w *observedWriter) disableBodyCapture() {
	w.mu.Lock()
	w.bodyCapture = false
	w.body = nil
	w.bodyTruncated = false
	w.mu.Unlock()
}

func (w *observedWriter) captureHeader() {
	w.mu.Lock()
	if w.capture && w.header == nil {
		w.header = w.ResponseWriter.Header().Clone()
	}
	w.mu.Unlock()
}

func (w *observedWriter) captureBody(p []byte) {
	if !w.capture || !w.bodyCapture || w.bodyTruncated {
		return
	}
	remaining := maxTracedBodySize - len(w.body)
	if len(p) > remaining {
		w.body = append(w.body, p[:remaining]...)
		w.bodyTruncated = true
		return
	}
	w.body = append(w.body, p...)
}

func (w *observedWriter) recordError(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

func supportsFlush(w http.ResponseWriter) bool {
	if _, ok := w.(interface{ FlushError() error }); ok {
		return true
	}
	if _, ok := w.(http.Flusher); ok {
		return true
	}
	unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
	return ok && supportsFlush(unwrapper.Unwrap())
}
