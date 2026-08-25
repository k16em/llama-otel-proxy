package serversentevents

import "bytes"

const MaxLine = 1 << 20

var (
	dataPrefix = []byte("data:")
	byteOrder  = []byte{0xEF, 0xBB, 0xBF}
)

type Parser struct {
	carry     []byte
	pendingLF bool
	lineSeen  bool

	data        []byte
	dataSeen    bool
	dataDropped bool

	last     []byte
	chunks   int
	dropping bool
	dropped  int
	sawDone  bool

	OnEvent func(payload []byte, terminal bool)
}

func (p *Parser) Feed(b []byte) {
	for len(b) > 0 {
		if p.pendingLF {
			p.pendingLF = false
			if b[0] == '\n' {
				b = b[1:]
				continue
			}
		}

		if p.dropping {
			_, rest, ok := p.splitLine(b)
			if !ok {
				return
			}
			b = rest
			p.dropping = false
			continue
		}

		line, rest, ok := p.splitLine(b)
		if !ok {
			if len(p.carry)+len(b) > MaxLine {
				p.carry = p.carry[:0]
				p.dropping = true
				p.dropLine()
				return
			}
			p.carry = append(p.carry, b...)
			return
		}
		b = rest

		if len(p.carry)+len(line) > MaxLine {
			p.carry = p.carry[:0]
			p.dropLine()
			continue
		}
		if len(p.carry) > 0 {
			p.carry = append(p.carry, line...)
			line = p.carry
		}
		p.processLine(line)
		p.carry = p.carry[:0]
	}
}

func (p *Parser) Finish() {
	p.pendingLF = false
	if p.dropping {
		p.dropping = false
		p.carry = p.carry[:0]
	} else if len(p.carry) > 0 {
		p.processLine(p.carry)
		p.carry = p.carry[:0]
	}
	p.dispatchEvent()
}

func (p *Parser) splitLine(b []byte) (line, rest []byte, ok bool) {
	i := indexEOL(b)
	if i < 0 {
		return nil, b, false
	}
	next := i + 1
	if b[i] == '\r' {
		if i+1 < len(b) {
			if b[i+1] == '\n' {
				next = i + 2
			}
		} else {
			p.pendingLF = true
		}
	}
	return b[:i], b[next:], true
}

func indexEOL(b []byte) int {
	lf := bytes.IndexByte(b, '\n')
	cr := bytes.IndexByte(b, '\r')
	switch {
	case lf < 0:
		return cr
	case cr < 0:
		return lf
	case cr < lf:
		return cr
	default:
		return lf
	}
}

func (p *Parser) dropLine() {
	p.dropped++
	p.data = p.data[:0]
	p.dataSeen = false
	p.dataDropped = true
}

func (p *Parser) processLine(line []byte) {
	if !p.lineSeen {
		p.lineSeen = true
		line = bytes.TrimPrefix(line, byteOrder)
	}
	if len(line) == 0 {
		p.dispatchEvent()
		return
	}
	if !bytes.HasPrefix(line, dataPrefix) || p.dataDropped {
		return
	}
	value := line[len(dataPrefix):]
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	extra := len(value)
	if p.dataSeen {
		extra++
	}
	if len(p.data)+extra > MaxLine {
		p.dropLine()
		return
	}
	if p.dataSeen {
		p.data = append(p.data, '\n')
	}
	p.data = append(p.data, value...)
	p.dataSeen = true
}

func (p *Parser) dispatchEvent() {
	data := p.data
	seen := p.dataSeen
	dropped := p.dataDropped
	p.data = p.data[:0]
	p.dataSeen = false
	p.dataDropped = false

	if dropped || !seen || len(data) == 0 {
		return
	}
	if bytes.Equal(data, []byte("[DONE]")) {
		p.sawDone = true
		if p.OnEvent != nil {
			p.OnEvent(data, true)
		}
		return
	}
	p.chunks++
	p.last = append(p.last[:0], data...)
	if p.OnEvent != nil {
		p.OnEvent(p.last, false)
	}
}

func (p *Parser) Chunks() int { return p.chunks }

func (p *Parser) Last() []byte { return p.last }

func (p *Parser) Dropped() int { return p.dropped }

func (p *Parser) SawDone() bool { return p.sawDone }
