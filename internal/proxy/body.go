package proxy

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/k16em/llama-otel-proxy/internal/genai"
	"github.com/k16em/llama-otel-proxy/internal/serversentevents"
)

const maxCollectedBodySize = 1 << 20

type bodyResult struct {
	FirstByte time.Time
	LastByte  time.Time
	Payload   []byte
	Chunks    int
	Dropped   int
	Truncated bool

	Err error

	SawDone bool

	FirstToken    time.Time
	StreamOutputs []genai.StreamOutput
	StreamLimited bool
}

type bodyObserver struct {
	inner  io.ReadCloser
	stream bool
	opaque bool

	parser            serversentevents.Parser
	streamAccumulator *genai.StreamAccumulator
	buf               []byte

	first     time.Time
	last      time.Time
	now       time.Time
	truncated bool

	onTimeToFirstToken func(time.Time)
	onStreamOutput     func(genai.StreamOutput)
	onDone             func(bodyResult)
	once               sync.Once

	sawToken   bool
	firstToken time.Time
}

func newBodyObserver(inner io.ReadCloser, stream bool, onDone func(bodyResult)) *bodyObserver {
	b := &bodyObserver{
		inner:  inner,
		stream: stream,
		onDone: onDone,
	}
	if stream {
		b.streamAccumulator = genai.NewStreamAccumulator(maxTracedBodySize)
		b.parser.OnEvent = b.observeStreamPayload
	}
	return b
}

func (b *bodyObserver) observeStreamPayload(payload []byte, terminal bool) {
	if b.parser.Dropped() > 0 {
		b.streamAccumulator.MarkLimited()
	}
	at := b.now
	if at.IsZero() {
		at = time.Now()
	}
	outputs := b.streamAccumulator.Observe(payload, terminal, at)
	b.checkFirstToken(outputs, at)
	for _, output := range outputs {
		if b.onStreamOutput != nil {
			b.onStreamOutput(output)
		}
	}
}

func (b *bodyObserver) checkFirstToken(outputs []genai.StreamOutput, at time.Time) {
	if b.sawToken || b.parser.Dropped() > 0 || b.streamAccumulator.ObservationGap() {
		return
	}
	if b.streamAccumulator.ContainsToken() {
		b.markFirstToken(at)
		return
	}
	for _, output := range outputs {
		if output.ContainsToken() {
			b.markFirstToken(at)
			return
		}
	}
}

func (b *bodyObserver) markFirstToken(at time.Time) {
	if b.sawToken {
		return
	}
	b.sawToken = true
	b.firstToken = at
	if b.onTimeToFirstToken != nil {
		b.onTimeToFirstToken(at)
	}
}

func (b *bodyObserver) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	if n > 0 {
		now := time.Now()
		first := b.first.IsZero()
		if first {
			b.first = now
		}
		b.last = now
		b.now = now
		if first && !b.stream && !b.opaque {
			b.markFirstToken(now)
		}
		b.observe(p[:n])
	}
	if err != nil {

		if errors.Is(err, io.EOF) {
			if b.stream {
				b.parser.Finish()
			}
			b.done(nil)
		} else {
			b.done(err)
		}
	}
	return n, err
}

func (b *bodyObserver) Close() error {
	b.done(nil)
	return b.inner.Close()
}

func (b *bodyObserver) observe(p []byte) {
	if b.opaque {
		return
	}
	if b.stream {
		b.parser.Feed(p)
		return
	}
	if b.truncated {
		return
	}
	if len(b.buf)+len(p) > maxCollectedBodySize {
		b.truncated = true
		b.buf = nil
		return
	}
	b.buf = append(b.buf, p...)
}

func (b *bodyObserver) done(err error) {
	b.once.Do(func() {
		res := bodyResult{
			FirstByte:  b.first,
			LastByte:   b.last,
			FirstToken: b.firstToken,
			Truncated:  b.truncated,
			Err:        err,
		}
		if b.stream {
			if b.parser.Dropped() > 0 {
				b.streamAccumulator.MarkLimited()
			}
			res.StreamOutputs = b.streamAccumulator.Finish(time.Now())
			res.StreamLimited = b.streamAccumulator.Limited()
			res.Payload = b.parser.Last()
			res.Chunks = b.parser.Chunks()
			res.Dropped = b.parser.Dropped()
			res.SawDone = b.parser.SawDone() || b.streamAccumulator.TerminalObserved()
		} else {
			res.Payload = b.buf
		}
		if b.onDone != nil {
			b.onDone(res)
		}
	})
}
