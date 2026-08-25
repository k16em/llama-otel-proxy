package serversentevents

import (
	"strings"
	"testing"
)

const stream = "data: {\"n\":1}\n\n" +
	"data: {\"n\":2}\n\n" +
	"data: {\"n\":3,\"usage\":{\"prompt_tokens\":7}}\n\n" +
	"data: [DONE]\n\n"

func Test_完全なServerSentEventsを一度にFeedしたときpayloadと終端が取得されている(t *testing.T) {
	var p Parser
	p.Feed([]byte(stream))
	if p.Chunks() != 3 {
		t.Errorf("Chunks = %d, want 3", p.Chunks())
	}
	if got := string(p.Last()); got != `{"n":3,"usage":{"prompt_tokens":7}}` {
		t.Errorf("Last = %q", got)
	}
}

func Test_ServerSentEventsをすべての位置で分割してFeedしても同じ結果になっている(t *testing.T) {
	for cut := 0; cut <= len(stream); cut++ {
		var p Parser
		p.Feed([]byte(stream[:cut]))
		p.Feed([]byte(stream[cut:]))
		if p.Chunks() != 3 {
			t.Fatalf("cut %d: Chunks = %d, want 3", cut, p.Chunks())
		}
		if got := string(p.Last()); got != `{"n":3,"usage":{"prompt_tokens":7}}` {
			t.Fatalf("cut %d: Last = %q", cut, got)
		}
	}
}

func Test_ServerSentEventsを1byteずつFeedしても同じ結果になっている(t *testing.T) {
	var p Parser
	for i := 0; i < len(stream); i++ {
		p.Feed([]byte{stream[i]})
	}
	if p.Chunks() != 3 {
		t.Errorf("Chunks = %d, want 3", p.Chunks())
	}
	if got := string(p.Last()); got != `{"n":3,"usage":{"prompt_tokens":7}}` {
		t.Errorf("Last = %q", got)
	}
}

func Test_CRLF区切りのServerSentEventsが解析されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\r\n\r\ndata: [DONE]\r\n\r\n"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
}

func Test_data以外のServerSentEventsフィールドが無視されている(t *testing.T) {
	var p Parser
	p.Feed([]byte(": keep-alive\nevent: ping\nid: 4\ndata: {\"n\":1}\n\n"))
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1", p.Chunks())
	}
}

func Test_上限を超える行が破棄されている(t *testing.T) {
	long := "data: " + strings.Repeat("x", MaxLine+10) + "\n\n"
	var p Parser
	p.Feed([]byte(long))
	p.Feed([]byte("data: {\"n\":9}\n\ndata: [DONE]\n\n"))
	if p.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", p.Dropped())
	}

	if p.Chunks() != 1 || string(p.Last()) != `{"n":9}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
}

func Test_分割された行が上限を超えたとき破棄されている(t *testing.T) {
	var p Parser
	for i := 0; i < 4; i++ {
		p.Feed([]byte(strings.Repeat("y", MaxLine/2)))
	}
	p.Feed([]byte("\n\ndata: {\"n\":1}\n\n"))
	if p.Dropped() == 0 {
		t.Error("want the over-long line dropped")
	}
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
}

func Test_破棄されたdata行を含むイベント全体が無効になっている(t *testing.T) {
	body := "data: {\"stop\":true,\n" +
		"data: " + strings.Repeat("x", MaxLine+10) + "\n" +
		"\n" +
		"data: {\"n\":1}\n\n"

	for _, cut := range []int{0, 10, len("data: {\"stop\":true,\n") + 100, MaxLine, len(body)} {
		var p Parser
		p.Feed([]byte(body[:cut]))
		p.Feed([]byte(body[cut:]))
		if p.Dropped() == 0 {
			t.Errorf("cut %d: the over-long line was not reported", cut)
		}
		if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
			t.Errorf("cut %d: chunks=%d last=%q, want only the intact event", cut, p.Chunks(), p.Last())
		}
	}
}

func Test_EOF時に破棄中のdata行を含むイベントが無効になっている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"stop\":true,\n"))
	p.Feed([]byte("data: " + strings.Repeat("x", MaxLine+10)))
	p.Finish()
	if p.Chunks() != 0 {
		t.Errorf("Chunks = %d, want 0", p.Chunks())
	}
	if p.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", p.Dropped())
	}
}

func Test_空のdataフィールドがpayloadとして数えられていない(t *testing.T) {
	var p Parser
	p.Feed([]byte("data:\n\ndata: \n\n"))
	if p.Chunks() != 0 {
		t.Errorf("Chunks = %d, want 0", p.Chunks())
	}
}

func Test_末尾に改行がない行がFinishで処理されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}"))
	if p.Chunks() != 0 {
		t.Error("an unterminated line must not count until its newline arrives")
	}
	p.Feed([]byte("\n"))
	if p.Chunks() != 0 {
		t.Error("an event is not dispatched until its blank line arrives")
	}
	p.Feed([]byte("\n"))
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1", p.Chunks())
	}
}

func Test_Finishで終端されていない行が処理されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\n\ndata: {\"stop\":true}"))
	if p.Chunks() != 1 {
		t.Fatalf("Chunks = %d before Finish, want 1", p.Chunks())
	}
	p.Finish()
	if p.Chunks() != 2 {
		t.Errorf("Chunks = %d, want 2", p.Chunks())
	}
	if got := string(p.Last()); got != `{"stop":true}` {
		t.Errorf("Last = %q, want the final event", got)
	}
}

func Test_Finishで空行のないイベントがdispatchされている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: [DONE]\n"))
	if p.SawDone() {
		t.Fatal("[DONE] must not count before its event is dispatched")
	}
	p.Finish()
	if !p.SawDone() {
		t.Error("Finish must dispatch the pending event")
	}
}

func Test_空のParserをFinishしてもpayloadが生成されていない(t *testing.T) {
	var p Parser
	p.Feed([]byte(stream))
	p.Finish()
	if p.Chunks() != 3 {
		t.Errorf("Chunks = %d, want 3; Finish must not invent an event", p.Chunks())
	}
}

func Test_Finish後も不完全なpayloadと完全なpayloadが区別されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\n\ndata: {\"n\":2,\"usa"))
	p.Finish()
	if got := string(p.Last()); got != `{"n":2,"usa` {
		t.Errorf("Last = %q", got)
	}
	if p.SawDone() {
		t.Error("a truncated event is not a terminator")
	}
}

func Test_複数のdata行が改行付きの1イベントに結合されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"stop\":true,\ndata: \"usage\":{\"n\":3}}\n\n"))
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1", p.Chunks())
	}
	if got := string(p.Last()); got != "{\"stop\":true,\n\"usage\":{\"n\":3}}" {
		t.Errorf("Last = %q", got)
	}
}

func Test_2つのイベントが2つのchunkとして数えられている(t *testing.T) {
	var p Parser
	p.Feed([]byte("event: delta\ndata: {\"a\":1}\n\n: ping\ndata: {\"b\":2}\n\n"))
	if p.Chunks() != 2 {
		t.Errorf("Chunks = %d, want 2", p.Chunks())
	}
	if got := string(p.Last()); got != `{"b":2}` {
		t.Errorf("Last = %q", got)
	}
}

func Test_結合後に上限を超えたイベントが破棄されている(t *testing.T) {
	var p Parser
	chunk := "data: " + strings.Repeat("z", MaxLine/4) + "\n"
	for i := 0; i < 8; i++ {
		p.Feed([]byte(chunk))
	}
	p.Feed([]byte("\n"))
	if p.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", p.Dropped())
	}
	if p.Chunks() != 0 {
		t.Errorf("Chunks = %d, want 0; a dropped event must not be emitted", p.Chunks())
	}
	p.Feed([]byte("data: {\"n\":1}\n\n"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("the parser did not resume: chunks=%d last=%q", p.Chunks(), p.Last())
	}
}

func Test_OnEventにはDONEを含む各dataイベントが通知されている(t *testing.T) {
	var payloads []string
	var terminals []bool
	p := Parser{}
	p.OnEvent = func(payload []byte, terminal bool) {
		payloads = append(payloads, string(payload))
		terminals = append(terminals, terminal)
	}
	p.Feed([]byte("data: {\"n\":1}\n\ndata: {\"n\":2}\n\ndata: [DONE]\n\n"))

	if got := strings.Join(payloads, "|"); got != `{"n":1}|{"n":2}|[DONE]` {
		t.Errorf("payloads = %q", got)
	}
	if len(terminals) != 3 || terminals[0] || terminals[1] || !terminals[2] {
		t.Errorf("terminals = %v", terminals)
	}
	if p.Chunks() != 2 || string(p.Last()) != `{"n":2}` || !p.SawDone() {
		t.Errorf("chunks=%d last=%q sawDone=%v", p.Chunks(), p.Last(), p.SawDone())
	}
}

func Test_上限内の大きな終端イベントが保持されている(t *testing.T) {
	payload := `{"type":"response.completed","response":{"output":"` +
		strings.Repeat("x", 200<<10) + `","usage":{"input_tokens":41}}}`
	var p Parser
	p.Feed([]byte("data: " + payload + "\n\n"))

	if p.Dropped() != 0 {
		t.Fatalf("dropped %d lines; a 200 KiB terminal event must survive", p.Dropped())
	}
	if string(p.Last()) != payload {
		t.Error("the terminal payload was not retained intact")
	}
}

// A terminal event larger than the line limit is dropped, and the caller has to
// be able to tell that apart from a stream that really stopped early.
func Test_上限を超えた終端イベントが破棄として記録されている(t *testing.T) {
	payload := `{"type":"response.completed","response":{"output":"` +
		strings.Repeat("x", MaxLine) + `","usage":{"input_tokens":41}}}`
	var p Parser
	p.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
	p.Feed([]byte("data: " + payload + "\n\n"))

	if p.Dropped() != 1 {
		t.Fatalf("Dropped = %d, want 1", p.Dropped())
	}
	if p.SawDone() {
		t.Error("no [DONE] was sent")
	}
	if string(p.Last()) == payload {
		t.Error("an over-limit line must not be buffered whole")
	}
}

// Just under the limit still gets through, so the boundary is where it is
// documented to be.
func Test_上限直前のイベントが保持されている(t *testing.T) {
	payload := `{"type":"response.completed","response":{"usage":{"input_tokens":41}},"pad":"` +
		strings.Repeat("x", MaxLine-200) + `"}`
	var p Parser
	p.Feed([]byte("data: " + payload + "\n\n"))
	if p.Dropped() != 0 {
		t.Fatalf("dropped a line of %d bytes, under the %d limit", len(payload), MaxLine)
	}
	if string(p.Last()) != payload {
		t.Error("payload not retained intact")
	}
}

func Test_行を破棄中にFinishしてもpayloadが生成されていない(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\n"))
	p.Feed([]byte("data: " + strings.Repeat("q", MaxLine+10)))
	p.Finish()
	if p.Chunks() != 0 {
		t.Errorf("Chunks = %d, want 0", p.Chunks())
	}
	if p.Dropped() != 1 {
		t.Errorf("Dropped = %d, want 1", p.Dropped())
	}
	p.Feed([]byte("data: {\"n\":2}\n\n"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":2}` {
		t.Errorf("the parser did not reset: chunks=%d last=%q", p.Chunks(), p.Last())
	}
}

func Test_Finishを複数回呼んでも結果が変化していない(t *testing.T) {
	var p Parser
	var events int
	p.OnEvent = func([]byte, bool) { events++ }
	p.Feed([]byte("data: {\"n\":1}"))
	p.Finish()
	p.Finish()
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1", p.Chunks())
	}
	if events != 1 {
		t.Errorf("events = %d, want 1", events)
	}
}

func Test_dataフィールド直後の空白が1つだけ除去されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\n\ndata:  [DONE]\n\n"))
	if p.SawDone() {
		t.Error("\" [DONE]\" is data, not a terminator")
	}
	if p.Chunks() != 2 {
		t.Errorf("Chunks = %d, want 2", p.Chunks())
	}
	if got := string(p.Last()); got != " [DONE]" {
		t.Errorf("Last = %q, want the payload with its leading space intact", got)
	}
}

func Test_dataフィールド直後に空白がなくても解析されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data:{\"n\":1}\n\ndata:[DONE]\n\n"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
	if !p.SawDone() {
		t.Error("want [DONE] recognized without a separating space")
	}
}

func Test_先頭のBOMが無視されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("\xef\xbb\xbfdata: {\"n\":1}\n\ndata: [DONE]\n\n"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
	if !p.SawDone() {
		t.Error("the rest of the stream must parse normally")
	}
	if p.Dropped() != 0 {
		t.Errorf("Dropped = %d, want 0", p.Dropped())
	}
}

func Test_分割してFeedされた先頭のBOMが無視されている(t *testing.T) {
	body := "\xef\xbb\xbfdata: {\"n\":1}\n\n"
	for cut := 0; cut <= len(body); cut++ {
		var p Parser
		p.Feed([]byte(body[:cut]))
		p.Feed([]byte(body[cut:]))
		if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
			t.Fatalf("cut %d: chunks=%d last=%q", cut, p.Chunks(), p.Last())
		}
	}
}

func Test_先頭以外のBOMが除去されていない(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\n\n\xef\xbb\xbfdata: {\"n\":2}\n\n"))
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1; a mid-stream BOM is not a data field", p.Chunks())
	}
}

func Test_CRだけの改行でServerSentEventsが解析されている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\r\rdata: [DONE]\r\r"))
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Errorf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
	if !p.SawDone() {
		t.Error("want [DONE] recognized with CR-only terminators")
	}
}

func Test_異なる改行コードが混在しても同じpayloadになっている(t *testing.T) {
	bodies := map[string]string{
		"lf":   "data: {\"n\":1}\n\ndata: [DONE]\n\n",
		"crlf": "data: {\"n\":1}\r\n\r\ndata: [DONE]\r\n\r\n",
		"cr":   "data: {\"n\":1}\r\rdata: [DONE]\r\r",
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			for cut := 0; cut <= len(body); cut++ {
				var p Parser
				p.Feed([]byte(body[:cut]))
				p.Feed([]byte(body[cut:]))
				p.Finish()
				if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` || !p.SawDone() {
					t.Fatalf("cut %d: chunks=%d last=%q done=%v",
						cut, p.Chunks(), p.Last(), p.SawDone())
				}
			}
		})
	}
}

func Test_FeedをまたぐCRLFが1つの改行として扱われている(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"a\":1}\r"))
	p.Feed([]byte("\ndata: {\"b\":2}\r\n\r\n"))
	if p.Chunks() != 1 {
		t.Fatalf("Chunks = %d, want 1", p.Chunks())
	}
	if got := string(p.Last()); got != "{\"a\":1}\n{\"b\":2}" {
		t.Errorf("Last = %q, want the two data lines joined as one event", got)
	}
}

func Test_Feedの分割位置にかかわらず解析結果が一致している(t *testing.T) {
	type result struct {
		chunks  int
		dropped int
		done    bool
		last    string
	}
	feed := func(body string, cuts []int, finish bool) result {
		var p Parser
		prev := 0
		for _, c := range cuts {
			p.Feed([]byte(body[prev:c]))
			prev = c
		}
		p.Feed([]byte(body[prev:]))
		if finish {
			p.Finish()
		}
		return result{p.Chunks(), p.Dropped(), p.SawDone(), string(p.Last())}
	}

	bodies := []string{
		"data: {\"n\":1}\n\ndata: [DONE]\n\n",
		"data: {\"n\":1}\r\n\r\ndata: [DONE]\r\n\r\n",
		"data: {\"n\":1}\r\rdata: [DONE]\r\r",
		"\xef\xbb\xbfdata: {\"n\":1}\r\n\r\n",
		"data: {\"a\":1}\r\ndata: {\"b\":2}\n\n: ping\r\ndata: [DONE]\r",
		"event: x\rid: 3\r\ndata: {\"n\":1}\n\ndata: {\"stop\":true}",
		"data:  [DONE]\n\ndata:{\"n\":9}\r\n\r\n",
		"data:\n\ndata: \r\n\r\ndata: {\"n\":1}\r",
		"data: {\"partial\":true,\ndata: \"more\":1}\r\n\r\n",
	}

	for i, body := range bodies {
		for _, finish := range []bool{false, true} {
			want := feed(body, nil, finish)

			every := make([]int, 0, len(body))
			for c := 1; c < len(body); c++ {
				every = append(every, c)
			}
			if got := feed(body, every, finish); got != want {
				t.Errorf("body %d finish=%v: byte-by-byte %+v, whole %+v", i, finish, got, want)
			}

			for c := 0; c <= len(body); c++ {
				if got := feed(body, []int{c}, finish); got != want {
					t.Fatalf("body %d finish=%v cut %d: %+v, whole %+v", i, finish, c, got, want)
				}
			}
		}
	}

	long := "data: {\"n\":1}\r\n\r\ndata: " + strings.Repeat("v", MaxLine+3) + "\r\ndata: {\"n\":2}\r\n\r\n"
	head := len("data: {\"n\":1}\r\n\r\ndata: ")
	for _, finish := range []bool{false, true} {
		want := feed(long, nil, finish)
		for _, c := range []int{1, head - 1, head, head + 1, MaxLine, MaxLine + head,
			len(long) - 20, len(long) - 1, len(long)} {
			if got := feed(long, []int{c}, finish); got != want {
				t.Errorf("over-long finish=%v cut %d: %+v, whole %+v", finish, c, got, want)
			}
		}
	}
}

func Test_Finish後に保留中のCRが次のFeedへ影響していない(t *testing.T) {
	var p Parser
	p.Feed([]byte("data: {\"n\":1}\r"))
	p.Finish()
	if p.Chunks() != 1 || string(p.Last()) != `{"n":1}` {
		t.Fatalf("chunks=%d last=%q", p.Chunks(), p.Last())
	}
	p.Feed([]byte("\ndata: [DONE]\n\n"))
	if !p.SawDone() {
		t.Error("the parser did not resume cleanly after Finish")
	}
	if p.Chunks() != 1 {
		t.Errorf("Chunks = %d, want 1", p.Chunks())
	}
}
