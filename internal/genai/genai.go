package genai

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const DefaultRequestLimit = 4 << 20

type Request struct {
	Model         string
	ModelKnown    bool
	Stream        bool
	StreamKnown   bool
	Body          []byte
	BodyTruncated bool
	Parameters    RequestParameters
}

type RequestParameters struct {
	MaxTokens        *int64
	Temperature      *float64
	TopP             *float64
	TopK             *int64
	FrequencyPenalty *float64
	PresencePenalty  *float64
	Seed             *int64
	ChoiceCount      *int64
	StopSequences    []string
	OutputType       string
}

type requestPayload struct {
	Model               string          `json:"model"`
	Stream              *bool           `json:"stream"`
	MaxTokens           *int64          `json:"max_tokens"`
	MaxCompletionTokens *int64          `json:"max_completion_tokens"`
	MaxOutputTokens     *int64          `json:"max_output_tokens"`
	NPredict            *int64          `json:"n_predict"`
	Temperature         *float64        `json:"temperature"`
	TopP                *float64        `json:"top_p"`
	TopK                *int64          `json:"top_k"`
	FrequencyPenalty    *float64        `json:"frequency_penalty"`
	PresencePenalty     *float64        `json:"presence_penalty"`
	Seed                *int64          `json:"seed"`
	ChoiceCount         *int64          `json:"n"`
	Stop                json.RawMessage `json:"stop"`
	ResponseFormat      *struct {
		Type string `json:"type"`
	} `json:"response_format"`
	Text *struct {
		Format *struct {
			Type string `json:"type"`
		} `json:"format"`
	} `json:"text"`
}

const maxStopSequences = 8

func (p requestPayload) parameters() RequestParameters {
	params := RequestParameters{
		MaxTokens:        firstNonNil(p.MaxTokens, p.MaxCompletionTokens, p.MaxOutputTokens, p.NPredict),
		Temperature:      p.Temperature,
		TopP:             p.TopP,
		TopK:             p.TopK,
		FrequencyPenalty: p.FrequencyPenalty,
		PresencePenalty:  p.PresencePenalty,
		Seed:             p.Seed,
		ChoiceCount:      p.ChoiceCount,
		StopSequences:    stopSequences(p.Stop),
	}
	format := ""
	if p.ResponseFormat != nil {
		format = p.ResponseFormat.Type
	}
	if format == "" && p.Text != nil && p.Text.Format != nil {
		format = p.Text.Format.Type
	}
	params.OutputType = outputType(format)
	return params
}

func outputType(format string) string {
	switch format {
	case "json_object", "json_schema", "json":
		return "json"
	case "text":
		return "text"
	default:
		return ""
	}
}

func stopSequences(raw json.RawMessage) []string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		if single == "" {
			return nil
		}
		return []string{Truncate(single)}
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil
	}
	sequences := make([]string, 0, len(many))
	for _, sequence := range many {
		if len(sequences) >= maxStopSequences {
			break
		}
		sequences = append(sequences, Truncate(sequence))
	}
	if len(sequences) == 0 {
		return nil
	}
	return sequences
}

func PeekRequest(r *http.Request, limit int64) (Request, bool) {
	req := Request{}
	if r.Body == nil || r.Body == http.NoBody {
		return req, false
	}

	head, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(head)) > limit || err != nil {
		r.Body = &splicedBody{Reader: io.MultiReader(bytes.NewReader(head), r.Body), closer: r.Body}
		req.Body = head
		if int64(len(req.Body)) > limit {
			req.Body = req.Body[:limit]
		}
		req.BodyTruncated = true
		peekTruncatedRequest(&req)
		return req, false
	}
	req.Body = head

	r.Body = &splicedBody{Reader: bytes.NewReader(head), closer: r.Body}
	r.ContentLength = int64(len(head))
	r.TransferEncoding = nil
	r.Header.Del("Content-Length")
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(head)), nil
	}

	var p requestPayload
	if err := json.Unmarshal(head, &p); err != nil {
		return req, false
	}
	if p.Model != "" {
		req.Model, req.ModelKnown = p.Model, true
	}
	if p.Stream != nil {
		req.Stream, req.StreamKnown = *p.Stream, true
	} else {

		req.StreamKnown = true
	}
	req.Parameters = p.parameters()
	return req, true
}

func peekTruncatedRequest(req *Request) {
	decoder := json.NewDecoder(bytes.NewReader(req.Body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			return
		}
		name, ok := token.(string)
		if !ok {
			return
		}
		value, err := decoder.Token()
		if err != nil {
			return
		}
		if _, nested := value.(json.Delim); nested {
			if !skipTruncatedValue(decoder) {
				return
			}
			continue
		}
		switch name {
		case "model":
			if model, ok := value.(string); ok && model != "" {
				req.Model, req.ModelKnown = model, true
			}
		case "stream":
			if stream, ok := value.(bool); ok {
				req.Stream, req.StreamKnown = stream, true
			}
		}
		if req.ModelKnown && req.StreamKnown {
			return
		}
	}
}

func skipTruncatedValue(decoder *json.Decoder) bool {
	depth := 1
	for depth > 0 {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			default:
				depth--
			}
		}
	}
	return true
}

type splicedBody struct {
	io.Reader
	closer io.Closer
}

func (b *splicedBody) Close() error { return b.closer.Close() }

type Timings struct {
	PromptMS           float64
	PredictedMS        float64
	PredictedPerSecond float64
	CacheN             int64

	HasPromptMS           bool
	HasPredictedMS        bool
	HasPredictedPerSecond bool
	HasCacheN             bool
}

type Response struct {
	Model string
	ID    string

	FinishReasons []string

	InputTokens     int64
	HasInputTokens  bool
	OutputTokens    int64
	HasOutputTokens bool

	Terminal   bool
	Incomplete bool

	Failed  bool
	Message string

	Timings *Timings
}

type usagePayload struct {
	PromptTokens     *int64 `json:"prompt_tokens"`
	CompletionTokens *int64 `json:"completion_tokens"`

	InputTokens  *int64 `json:"input_tokens"`
	OutputTokens *int64 `json:"output_tokens"`

	Timings *timingsPayload `json:"timings"`
}

type responsePayload struct {
	Type    string `json:"type"`
	Model   string `json:"model"`
	ID      string `json:"id"`
	Choices []struct {
		FinishReason  *string `json:"finish_reason"`
		StopReason    *string `json:"stop_reason"`
		NativeStopped *bool   `json:"stopped_eos"`
	} `json:"choices"`
	StopType string          `json:"stop_type"`
	Message  string          `json:"message"`
	Code     json.RawMessage `json:"code"`
	Usage    *usagePayload   `json:"usage"`
	Timings  *timingsPayload `json:"timings"`
	Stop     *bool           `json:"stop"`
	Error    json.RawMessage `json:"error"`

	Response *struct {
		Model  string          `json:"model"`
		ID     string          `json:"id"`
		Status string          `json:"status"`
		Usage  *usagePayload   `json:"usage"`
		Error  json.RawMessage `json:"error"`
	} `json:"response"`
}

type timingsPayload struct {
	PromptMS           *float64 `json:"prompt_ms"`
	PredictedMS        *float64 `json:"predicted_ms"`
	PredictedPerSecond *float64 `json:"predicted_per_second"`
	CacheN             *int64   `json:"cache_n"`
}

func ParseResponse(body []byte) (Response, bool) {
	var p responsePayload
	if err := json.Unmarshal(body, &p); err != nil {
		return Response{}, false
	}

	res := Response{Model: p.Model, ID: p.ID}
	res.FinishReasons = responseFinishReasons(p)
	res.Terminal = p.Type == "response.completed" || p.Type == "response.failed" || p.Type == "response.incomplete" || p.Type == "error" || (p.Stop != nil && *p.Stop)
	res.Incomplete = p.Type == "response.incomplete"
	if len(p.Error) > 0 && !bytes.Equal(p.Error, []byte("null")) {
		res.Failed = true
		res.Message = errorMessage(p.Error)

		res.Terminal = true
	}
	usage := p.Usage
	if p.Response != nil {
		if res.Model == "" {
			res.Model = p.Response.Model
		}
		if res.ID == "" {
			res.ID = p.Response.ID
		}
		if len(res.FinishReasons) == 0 && p.Response.Status != "" {
			res.FinishReasons = []string{Truncate(p.Response.Status)}
		}
		if usage == nil {
			usage = p.Response.Usage
		}
		if len(p.Response.Error) > 0 && !bytes.Equal(bytes.TrimSpace(p.Response.Error), []byte("null")) {
			res.Failed = true
			res.Message = errorMessage(p.Response.Error)
			res.Terminal = true
		}
	}
	if p.Type == "response.failed" {
		res.Failed = true
		if res.Message == "" {
			res.Message = "upstream reported an error"
		}
	}
	if p.Type == "error" {
		res.Failed = true
		if p.Message != "" {
			res.Message = Truncate(p.Message)
		}
		if res.Message == "" {
			res.Message = rawErrorCode(p.Code)
		}
		if res.Message == "" {
			res.Message = "upstream reported an error"
		}
	}

	if usage != nil {
		if in := firstNonNil(usage.PromptTokens, usage.InputTokens); in != nil {
			res.InputTokens, res.HasInputTokens = *in, true
		}
		if out := firstNonNil(usage.CompletionTokens, usage.OutputTokens); out != nil {
			res.OutputTokens, res.HasOutputTokens = *out, true
		}
	}

	tp := p.Timings
	if tp == nil && usage != nil {
		tp = usage.Timings
	}
	if tp != nil {
		t := &Timings{}
		if tp.PromptMS != nil {
			t.PromptMS, t.HasPromptMS = *tp.PromptMS, true
		}
		if tp.PredictedMS != nil {
			t.PredictedMS, t.HasPredictedMS = *tp.PredictedMS, true
		}
		if tp.PredictedPerSecond != nil {
			t.PredictedPerSecond, t.HasPredictedPerSecond = *tp.PredictedPerSecond, true
		}
		if tp.CacheN != nil {
			t.CacheN, t.HasCacheN = *tp.CacheN, true
		}
		if t.HasPromptMS || t.HasPredictedMS || t.HasPredictedPerSecond || t.HasCacheN {
			res.Timings = t
		}
	}
	return res, true
}

func responseFinishReasons(p responsePayload) []string {
	reasons := make([]string, 0, len(p.Choices))
	for _, choice := range p.Choices {
		reason := ""
		switch {
		case choice.FinishReason != nil && *choice.FinishReason != "":
			reason = *choice.FinishReason
		case choice.StopReason != nil && *choice.StopReason != "":
			reason = *choice.StopReason
		}
		if reason == "" {
			continue
		}
		reasons = append(reasons, Truncate(reason))
	}
	if len(reasons) > 0 {
		return reasons
	}
	if p.StopType != "" {
		return []string{Truncate(p.StopType)}
	}
	return nil
}

func errorMessage(raw json.RawMessage) string {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return Truncate(asString)
	}
	var obj struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		switch {
		case obj.Message != "":
			return Truncate(obj.Message)
		case obj.Type != "":
			return Truncate(obj.Type)
		}
	}
	return "upstream reported an error"
}

func rawErrorCode(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return Truncate(value)
	}
	return Truncate(string(raw))
}

func Truncate(s string) string {
	const max = 256
	s = strings.ToValidUTF8(s, "\uFFFD")
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func FirstTokenIn(payload []byte) bool {
	accumulator := NewStreamAccumulator(maxStreamMetadataSize)
	for _, output := range accumulator.Observe(payload, false, time.Time{}) {
		if output.ContainsToken() {
			return true
		}
	}
	return accumulator.ContainsToken()
}

type functionPart struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type StreamOutputKind uint8

const (
	StreamOutputReasoning StreamOutputKind = iota
	StreamOutputResponse
	StreamOutputToolCall
)

type StreamOutput struct {
	Kind StreamOutputKind

	ChoiceIndex      int
	HasChoiceIndex   bool
	OutputIndex      int
	HasOutputIndex   bool
	ContentIndex     int
	HasContentIndex  bool
	ToolCallIndex    int
	HasToolCallIndex bool

	ToolCallID string
	ItemID     string
	ToolType   string
	Name       string

	Reasoning    string
	Content      string
	Refusal      string
	Arguments    string
	FinishReason string

	Start time.Time
	End   time.Time

	Truncated          bool
	Complete           bool
	Failed             bool
	ObservationLimited bool
	ProtocolIncomplete bool
	ErrorMessage       string
}

func (o StreamOutput) ContainsToken() bool {
	return streamOutputHasToken(o)
}

type streamOutputSource uint8

const (
	streamSourceChat streamOutputSource = iota
	streamSourceResponses
	streamSourceNative
)

const (
	maxStreamOutputStates = 64
	maxStreamOutputParts  = 128
	maxStreamMetadataSize = 4 << 10
)

type streamOutputKey struct {
	source       streamOutputSource
	kind         StreamOutputKind
	choiceIndex  int
	outputIndex  int
	contentIndex int
	toolIndex    int
	itemID       string
}

type streamOutputGroup struct {
	source      streamOutputSource
	kind        StreamOutputKind
	choiceIndex int
	outputIndex int
	toolIndex   int
}

func (key streamOutputKey) group() streamOutputGroup {
	return streamOutputGroup{
		source:      key.source,
		kind:        key.kind,
		choiceIndex: key.choiceIndex,
		outputIndex: key.outputIndex,
		toolIndex:   key.toolIndex,
	}
}

type limitedStreamText struct {
	value     strings.Builder
	truncated bool
}

func (t *limitedStreamText) append(value string, limit int) {
	value = strings.ToValidUTF8(value, "\uFFFD")
	remaining := limit - t.value.Len()
	if remaining <= 0 {
		if value != "" {
			t.truncated = true
		}
		return
	}
	if len(value) <= remaining {
		t.value.WriteString(value)
		return
	}
	cut := remaining
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	t.value.WriteString(value[:cut])
	t.truncated = true
}

func (t *limitedStreamText) set(value string, limit int) {
	t.value.Reset()
	t.truncated = false
	t.append(value, limit)
}

func (t *limitedStreamText) string() string {
	return t.value.String()
}

type streamOutputState struct {
	output StreamOutput

	reasoning     limitedStreamText
	content       limitedStreamText
	refusal       limitedStreamText
	arguments     limitedStreamText
	name          limitedStreamText
	toolType      limitedStreamText
	toolID        limitedStreamText
	itemID        limitedStreamText
	finishReason  limitedStreamText
	shellCommands map[int]*limitedStreamText

	started                bool
	argumentsAuthoritative bool
	completed              bool
	incomplete             bool
	failed                 bool
	errorMessage           string
}

func (s *streamOutputState) touch(at time.Time) {
	if !s.started {
		s.started = true
		s.output.Start = at
	}
}

func (s *streamOutputState) hasData() bool {
	switch s.output.Kind {
	case StreamOutputReasoning:
		return s.reasoning.value.Len() > 0 || s.reasoning.truncated
	case StreamOutputResponse:
		return s.content.value.Len() > 0 || s.refusal.value.Len() > 0 || s.content.truncated || s.refusal.truncated
	case StreamOutputToolCall:
		return s.toolID.value.Len() > 0 || s.toolType.value.Len() > 0 || s.name.value.Len() > 0 || s.arguments.value.Len() > 0 ||
			s.toolID.truncated || s.toolType.truncated || s.name.truncated || s.arguments.truncated || len(s.shellCommands) > 0
	default:
		return false
	}
}

func streamStateHasToken(state *streamOutputState) bool {
	if state == nil {
		return false
	}
	switch state.output.Kind {
	case StreamOutputReasoning:
		return state.reasoning.value.Len() > 0 || state.reasoning.truncated
	case StreamOutputResponse:
		return state.content.value.Len() > 0 || state.refusal.value.Len() > 0 || state.content.truncated || state.refusal.truncated
	case StreamOutputToolCall:
		return streamToolHasToken(state.toolType.string(), state.toolID.string(), state.name.string(), state.arguments.string()) ||
			state.toolType.truncated || state.toolID.truncated || state.name.truncated || state.arguments.truncated
	default:
		return false
	}
}

func streamOutputHasToken(output StreamOutput) bool {
	switch output.Kind {
	case StreamOutputReasoning:
		return output.Reasoning != "" || output.Truncated
	case StreamOutputResponse:
		return output.Content != "" || output.Refusal != "" || output.Truncated
	case StreamOutputToolCall:
		return streamToolHasToken(output.ToolType, output.ToolCallID, output.Name, output.Arguments) || output.Truncated
	default:
		return false
	}
}

func streamToolHasToken(toolType, toolID, name, arguments string) bool {
	if toolID != "" || name != "" || arguments != "" {
		return true
	}
	return toolType != "" && toolType != "function" && toolType != "function_call" && toolType != "custom_tool_call"
}

func (s *streamOutputState) finish(at time.Time, complete, failed bool, message string, limit int) StreamOutput {
	if s.failed {
		complete = false
		failed = true
		message = preferredStreamErrorMessage(s.errorMessage, message)
	} else if s.incomplete {
		complete = false
	} else if s.completed {
		complete = true
		failed = false
		message = ""
	}
	out := s.output
	out.End = at
	out.Complete = complete
	out.Failed = failed
	out.ProtocolIncomplete = s.incomplete
	out.ErrorMessage = message
	out.Reasoning = s.reasoning.string()
	out.Content = s.content.string()
	out.Refusal = s.refusal.string()
	out.Arguments = s.arguments.string()
	shellTruncated := false
	if len(s.shellCommands) > 0 && !s.argumentsAuthoritative {
		out.Arguments, shellTruncated = s.assembledShellCommands(limit)
	}
	out.Name = s.name.string()
	out.ToolType = s.toolType.string()
	out.ToolCallID = s.toolID.string()
	out.ItemID = s.itemID.string()
	out.FinishReason = s.finishReason.string()
	out.Truncated = s.reasoning.truncated || s.content.truncated || s.refusal.truncated ||
		s.arguments.truncated || s.name.truncated || s.toolType.truncated || s.toolID.truncated ||
		s.itemID.truncated || s.finishReason.truncated || shellTruncated
	return out
}

func (s *streamOutputState) assembledShellCommands(limit int) (string, bool) {
	indexes := make([]int, 0, len(s.shellCommands))
	for index := range s.shellCommands {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	commands := make([]string, 0, len(indexes))
	truncated := false
	for _, index := range indexes {
		command := s.shellCommands[index]
		commands = append(commands, command.string())
		truncated = truncated || command.truncated
	}
	raw, _ := json.Marshal(struct {
		Commands []string `json:"commands"`
	}{Commands: commands})
	var assembled limitedStreamText
	assembled.set(string(raw), limit)
	return assembled.string(), truncated || assembled.truncated
}

type StreamAccumulator struct {
	limit int

	states             map[streamOutputKey]*streamOutputState
	finished           map[streamOutputKey]struct{}
	groups             map[streamOutputGroup]struct{}
	finishedGroups     map[streamOutputGroup]struct{}
	finishedChoices    map[[2]int]struct{}
	createdParts       int
	limited            bool
	decodeFailed       bool
	terminalIncomplete bool
	terminalObserved   bool
	closed             bool
}

func NewStreamAccumulator(limit int) *StreamAccumulator {
	if limit <= 0 {
		limit = 1
	}
	return &StreamAccumulator{
		limit:           limit,
		states:          make(map[streamOutputKey]*streamOutputState),
		finished:        make(map[streamOutputKey]struct{}),
		groups:          make(map[streamOutputGroup]struct{}),
		finishedGroups:  make(map[streamOutputGroup]struct{}),
		finishedChoices: make(map[[2]int]struct{}),
	}
}

func (a *StreamAccumulator) Limited() bool {
	return a.limited
}

func (a *StreamAccumulator) ObservationGap() bool {
	return a != nil && (a.limited || a.decodeFailed)
}

func (a *StreamAccumulator) ContainsToken() bool {
	if a == nil {
		return false
	}
	for _, state := range a.states {
		if streamStateHasToken(state) {
			return true
		}
	}
	return false
}

func (a *StreamAccumulator) TerminalObserved() bool {
	return a != nil && a.terminalObserved
}

func (a *StreamAccumulator) MarkLimited() {
	if a != nil {
		a.limited = true
	}
}

func (a *StreamAccumulator) Observe(payload []byte, protocolTerminal bool, at time.Time) []StreamOutput {
	if a == nil || a.closed {
		return nil
	}
	if protocolTerminal {
		if a.decodeFailed {
			a.limited = true
		}
		a.closed = true
		a.terminalObserved = true
		return a.finishAll(at, true, false, "")
	}

	var event streamEventPayload
	if err := json.Unmarshal(payload, &event); err != nil {
		a.decodeFailed = true
		if json.Valid(payload) {
			a.limited = true
		}
		terminal, incomplete, failed, message := decodeStreamTerminal(payload)
		if terminal {
			a.limited = true
			a.terminalIncomplete = incomplete
			a.closed = true
			a.terminalObserved = true
			return a.finishAll(at, !incomplete && !failed, failed, message)
		}
		return nil
	}

	failed := streamEventFailed(event)
	outputs := a.observeChat(event, at, !failed)
	outputs = append(outputs, a.observeNative(event, at, !failed)...)
	outputs = append(outputs, a.observeResponses(event, at)...)

	nativeTerminal := event.Stop != nil && *event.Stop
	if a.decodeFailed && (failed || nativeTerminal || event.Type == "response.incomplete" || event.Type == "response.completed") {
		a.limited = true
	}
	switch {
	case failed:
		a.closed = true
		a.terminalObserved = true
		outputs = append(outputs, a.finishAll(at, false, true, streamEventErrorMessage(event))...)
	case event.Type == "response.incomplete":
		a.terminalIncomplete = true
		a.closed = true
		a.terminalObserved = true
		outputs = append(outputs, a.finishAll(at, false, false, "")...)
	case event.Type == "response.completed":
		a.closed = true
		a.terminalObserved = true
		outputs = append(outputs, a.finishAll(at, true, false, "")...)
	case nativeTerminal:
		a.closed = true
		a.terminalObserved = true
		outputs = append(outputs, a.finishAll(at, true, false, "")...)
	}
	return outputs
}

func (a *StreamAccumulator) Finish(at time.Time) []StreamOutput {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	return a.finishAll(at, false, false, "")
}

func (a *StreamAccumulator) state(key streamOutputKey, output StreamOutput) *streamOutputState {
	if _, done := a.finished[key]; done {
		return nil
	}
	group := key.group()
	if _, done := a.finishedGroups[group]; done {
		return nil
	}
	if state := a.states[key]; state != nil {
		return state
	}
	if a.createdParts >= maxStreamOutputParts {
		a.limited = true
		return nil
	}
	if _, exists := a.groups[group]; !exists {
		if len(a.groups) >= maxStreamOutputStates {
			a.limited = true
			return nil
		}
		a.groups[group] = struct{}{}
	}
	a.createdParts++
	state := &streamOutputState{output: output}
	a.states[key] = state
	return state
}

func (a *StreamAccumulator) finishKey(key streamOutputKey, at time.Time, complete, failed bool, message string) (StreamOutput, bool) {
	state := a.states[key]
	if state == nil {
		return StreamOutput{}, false
	}
	delete(a.states, key)
	if !state.hasData() {
		return StreamOutput{}, false
	}
	a.finished[key] = struct{}{}
	a.finishedGroups[key.group()] = struct{}{}
	output := state.finish(at, complete, failed, message, a.limit)
	output.ObservationLimited = a.limited || a.decodeFailed && complete
	output.ProtocolIncomplete = output.ProtocolIncomplete || a.terminalIncomplete && !output.Complete
	return output, true
}

func (a *StreamAccumulator) finishMatching(match func(streamOutputKey) bool, at time.Time, complete, failed bool, message string) []StreamOutput {
	keys := make([]streamOutputKey, 0, len(a.states))
	for key := range a.states {
		if match(key) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := keys[i], keys[j]
		if left.source != right.source {
			return left.source < right.source
		}
		if left.choiceIndex != right.choiceIndex {
			return left.choiceIndex < right.choiceIndex
		}
		if left.outputIndex != right.outputIndex {
			return left.outputIndex < right.outputIndex
		}
		if left.contentIndex != right.contentIndex {
			return left.contentIndex < right.contentIndex
		}
		if left.kind != right.kind {
			return left.kind < right.kind
		}
		if left.toolIndex != right.toolIndex {
			return left.toolIndex < right.toolIndex
		}
		return left.itemID < right.itemID
	})
	outputs := make([]StreamOutput, 0, len(keys))
	for _, key := range keys {
		if output, ok := a.finishKey(key, at, complete, failed, message); ok {
			outputs = append(outputs, output)
		}
	}
	return outputs
}

func (a *StreamAccumulator) finishAll(at time.Time, complete, failed bool, message string) []StreamOutput {
	type responseGroup struct {
		outputIndex int
		kind        StreamOutputKind
	}
	groups := make(map[responseGroup]struct{})
	for key := range a.states {
		if key.source == streamSourceResponses {
			groups[responseGroup{outputIndex: key.outputIndex, kind: key.kind}] = struct{}{}
		}
	}
	ordered := make([]responseGroup, 0, len(groups))
	for group := range groups {
		ordered = append(ordered, group)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].outputIndex != ordered[j].outputIndex {
			return ordered[i].outputIndex < ordered[j].outputIndex
		}
		return ordered[i].kind < ordered[j].kind
	})
	var outputs []StreamOutput
	for _, group := range ordered {
		outputs = append(outputs, a.finishResponseOutputKind(group.outputIndex, group.kind, at, complete, failed, message)...)
	}
	outputs = append(outputs, a.finishMatching(func(key streamOutputKey) bool {
		return key.source != streamSourceResponses
	}, at, complete, failed, message)...)
	return outputs
}

func (a *StreamAccumulator) setChoiceFinishReason(source streamOutputSource, index int, reason string) {
	for key, state := range a.states {
		if key.source == source && key.choiceIndex == index {
			setStreamMetadata(&state.finishReason, reason, a.limit)
		}
	}
}

func (a *StreamAccumulator) finishChoice(source streamOutputSource, index int, at time.Time, reason string) []StreamOutput {
	choiceKey := [2]int{int(source), index}
	if _, done := a.finishedChoices[choiceKey]; done {
		return nil
	}
	if len(a.finishedChoices) >= maxStreamOutputStates {
		a.limited = true
		return nil
	}
	a.finishedChoices[choiceKey] = struct{}{}
	a.setChoiceFinishReason(source, index, reason)
	return a.finishMatching(func(key streamOutputKey) bool {
		return key.source == source && key.choiceIndex == index
	}, at, true, false, "")
}

type streamEventPayload struct {
	Type             string          `json:"type"`
	Delta            json.RawMessage `json:"delta"`
	Content          json.RawMessage `json:"content"`
	Reasoning        string          `json:"reasoning"`
	ReasoningContent string          `json:"reasoning_content"`
	Refusal          string          `json:"refusal"`
	Text             string          `json:"text"`
	Arguments        string          `json:"arguments"`
	Input            string          `json:"input"`
	Name             string          `json:"name"`
	Message          string          `json:"message"`
	Status           string          `json:"status"`
	Code             json.RawMessage `json:"code"`
	Command          string          `json:"command"`
	ItemID           string          `json:"item_id"`
	OutputIndex      *int            `json:"output_index"`
	ContentIndex     *int            `json:"content_index"`
	SummaryIndex     *int            `json:"summary_index"`
	CommandIndex     *int            `json:"command_index"`
	Stop             *bool           `json:"stop"`
	StopType         string          `json:"stop_type"`
	Error            json.RawMessage `json:"error"`
	Choices          []streamChoice  `json:"choices"`
	Item             *streamItem     `json:"item"`
	Part             *streamPart     `json:"part"`
	Response         *struct {
		Output []streamItem    `json:"output"`
		Error  json.RawMessage `json:"error"`
	} `json:"response"`
}

type streamChoice struct {
	Index        *int         `json:"index"`
	Text         string       `json:"text"`
	FinishReason *string      `json:"finish_reason"`
	Delta        *streamDelta `json:"delta"`
}

type streamDelta struct {
	Content          string           `json:"content"`
	Reasoning        string           `json:"reasoning"`
	ReasoningContent string           `json:"reasoning_content"`
	Refusal          string           `json:"refusal"`
	ToolCalls        []streamToolPart `json:"tool_calls"`
	FunctionCall     *functionPart    `json:"function_call"`
}

type streamToolPart struct {
	Index    *int          `json:"index"`
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Function *functionPart `json:"function"`
}

type streamItem struct {
	ID        string          `json:"id"`
	CallID    string          `json:"call_id"`
	Type      string          `json:"type"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	Arguments json.RawMessage `json:"arguments"`
	Input     json.RawMessage `json:"input"`
	Code      json.RawMessage `json:"code"`
	Action    json.RawMessage `json:"action"`
	Actions   json.RawMessage `json:"actions"`
	Queries   json.RawMessage `json:"queries"`
	Operation json.RawMessage `json:"operation"`
	Error     json.RawMessage `json:"error"`
	Content   []streamPart    `json:"content"`
	Summary   []streamPart    `json:"summary"`
}

type streamPart struct {
	Type    string          `json:"type"`
	Text    string          `json:"text"`
	Refusal string          `json:"refusal"`
	Status  string          `json:"status"`
	Error   json.RawMessage `json:"error"`
}

func (a *StreamAccumulator) observeChat(event streamEventPayload, at time.Time, finishOutputs bool) []StreamOutput {
	var outputs []StreamOutput
	for choicePosition, choice := range event.Choices {
		choiceIndex := streamIndex(choice.Index, choicePosition)
		if _, done := a.finishedChoices[[2]int{int(streamSourceChat), choiceIndex}]; done {
			continue
		}

		delta := choice.Delta
		if delta != nil {
			reasoning := delta.ReasoningContent
			if reasoning == "" {
				reasoning = delta.Reasoning
			}
			if reasoning != "" {
				state := a.chatState(StreamOutputReasoning, choiceIndex)
				if state != nil {
					state.touch(at)
					state.reasoning.append(reasoning, a.limit)
				}
			}

			if delta.Content != "" || delta.Refusal != "" {
				state := a.chatState(StreamOutputResponse, choiceIndex)
				if state != nil {
					state.touch(at)
					state.content.append(delta.Content, a.limit)
					state.refusal.append(delta.Refusal, a.limit)
				}
			}

			for toolPosition, tool := range delta.ToolCalls {
				toolIndex := streamIndex(tool.Index, toolPosition)
				state := a.chatToolState(choiceIndex, toolIndex)
				if state == nil {
					continue
				}
				name, arguments := "", ""
				if tool.Function != nil {
					name = tool.Function.Name
					arguments = tool.Function.Arguments
				}
				if tool.ID == "" && tool.Type == "" && name == "" && arguments == "" {
					continue
				}
				state.touch(at)
				setStreamMetadata(&state.toolID, tool.ID, a.limit)
				setStreamMetadata(&state.toolType, tool.Type, a.limit)
				appendStreamMetadata(&state.name, name, a.limit)
				state.arguments.append(arguments, a.limit)
			}

			if delta.FunctionCall != nil {
				call := delta.FunctionCall
				if call.Name != "" || call.Arguments != "" {
					state := a.chatToolState(choiceIndex, 0)
					if state != nil {
						state.touch(at)
						setStreamMetadata(&state.toolType, "function", a.limit)
						appendStreamMetadata(&state.name, call.Name, a.limit)
						state.arguments.append(call.Arguments, a.limit)
					}
				}
			}
		}

		if choice.Text != "" {
			state := a.chatState(StreamOutputResponse, choiceIndex)
			if state != nil {
				state.touch(at)
				state.content.append(choice.Text, a.limit)
			}
		}
		if choice.FinishReason != nil {
			if finishOutputs {
				outputs = append(outputs, a.finishChoice(streamSourceChat, choiceIndex, at, *choice.FinishReason)...)
			} else {
				a.setChoiceFinishReason(streamSourceChat, choiceIndex, *choice.FinishReason)
			}
		}
	}
	return outputs
}

func (a *StreamAccumulator) observeNative(event streamEventPayload, at time.Time, finishOutputs bool) []StreamOutput {
	if event.Type != "" || len(event.Choices) != 0 {
		return nil
	}
	reasoning := event.ReasoningContent
	if reasoning == "" {
		reasoning = event.Reasoning
	}
	if reasoning != "" {
		state := a.nativeState(StreamOutputReasoning)
		if state != nil {
			state.touch(at)
			state.reasoning.append(reasoning, a.limit)
		}
	}
	if content := streamJSONString(event.Content); content != "" {
		state := a.nativeState(StreamOutputResponse)
		if state != nil {
			state.touch(at)
			state.content.append(content, a.limit)
		}
	}
	if event.Stop != nil && *event.Stop {
		reason := event.StopType
		if reason == "" {
			reason = "stop"
		}
		if finishOutputs {
			return a.finishChoice(streamSourceNative, 0, at, reason)
		}
		a.setChoiceFinishReason(streamSourceNative, 0, reason)
	}
	return nil
}

func (a *StreamAccumulator) observeResponses(event streamEventPayload, at time.Time) []StreamOutput {
	if event.Type == "" {
		return nil
	}
	outputIndex := streamIndex(event.OutputIndex, 0)
	contentIndex := streamIndex(event.ContentIndex, 0)
	itemID := event.ItemID
	var outputs []StreamOutput

	switch event.Type {
	case "response.output_text.delta", "response.text.delta":
		a.appendResponseText(outputIndex, contentIndex, itemID, streamJSONString(event.Delta), "", at)
	case "response.refusal.delta":
		a.appendResponseText(outputIndex, contentIndex, itemID, "", streamJSONString(event.Delta), at)
	case "response.reasoning_text.delta":
		a.appendResponseReasoning(outputIndex, contentIndex, itemID, streamJSONString(event.Delta), at)
	case "response.reasoning_summary_text.delta":
		a.appendResponseSummary(outputIndex, streamIndex(event.SummaryIndex, 0), itemID, streamJSONString(event.Delta), at)
	case "response.function_call_arguments.delta":
		a.appendResponseTool(outputIndex, itemID, "function_call", "", streamJSONString(event.Delta), "", at)
	case "response.custom_tool_call_input.delta":
		a.appendResponseTool(outputIndex, itemID, "custom_tool_call", "", "", streamJSONString(event.Delta), at)
	case "response.mcp_call_arguments.delta":
		a.appendResponseTool(outputIndex, itemID, "mcp_call", event.Name, streamJSONString(event.Delta), "", at)
	case "response.code_interpreter_call_code.delta":
		a.appendResponseTool(outputIndex, itemID, "code_interpreter_call", "", streamJSONString(event.Delta), "", at)
	case "response.shell_call_command.added":
		a.observeResponseShellCommand(outputIndex, streamIndex(event.CommandIndex, 0), itemID, event.Command, at, true)
	case "response.shell_call_command.delta":
		a.observeResponseShellCommand(outputIndex, streamIndex(event.CommandIndex, 0), itemID, streamJSONString(event.Delta), at, false)
	case "response.shell_call_command.done":
		a.observeResponseShellCommand(outputIndex, streamIndex(event.CommandIndex, 0), itemID, event.Command, at, true)
	case "response.output_text.done", "response.text.done":
		state, _ := a.responseTextState(StreamOutputResponse, outputIndex, contentIndex, itemID)
		if state != nil {
			state.touch(at)
			state.content.set(event.Text, a.limit)
		}
	case "response.refusal.done":
		state, _ := a.responseTextState(StreamOutputResponse, outputIndex, contentIndex, itemID)
		if state != nil {
			state.touch(at)
			value := event.Refusal
			if value == "" {
				value = event.Text
			}
			state.refusal.set(value, a.limit)
		}
	case "response.reasoning_text.done":
		state, _ := a.responseTextState(StreamOutputReasoning, outputIndex, contentIndex, itemID)
		if state != nil {
			state.touch(at)
			state.reasoning.set(event.Text, a.limit)
		}
	case "response.reasoning_summary_text.done":
		summaryIndex := streamIndex(event.SummaryIndex, 0)
		state, _ := a.responseSummaryState(outputIndex, summaryIndex, itemID)
		if state != nil {
			state.touch(at)
			state.reasoning.set(event.Text, a.limit)
		}
	case "response.function_call_arguments.done":
		state, _ := a.responseToolState(outputIndex, itemID)
		if state != nil {
			state.touch(at)
			setStreamMetadata(&state.toolType, "function_call", a.limit)
			setStreamMetadata(&state.name, event.Name, a.limit)
			state.arguments.set(event.Arguments, a.limit)
		}
	case "response.custom_tool_call_input.done":
		state, _ := a.responseToolState(outputIndex, itemID)
		if state != nil {
			state.touch(at)
			setStreamMetadata(&state.toolType, "custom_tool_call", a.limit)
			setStreamMetadata(&state.name, event.Name, a.limit)
			state.arguments.set(event.Input, a.limit)
		}
	case "response.mcp_call_arguments.done":
		state, _ := a.responseToolState(outputIndex, itemID)
		if state != nil {
			state.touch(at)
			setStreamMetadata(&state.toolType, "mcp_call", a.limit)
			setStreamMetadata(&state.name, event.Name, a.limit)
			state.arguments.set(event.Arguments, a.limit)
		}
	case "response.code_interpreter_call_code.done":
		state, _ := a.responseToolState(outputIndex, itemID)
		if state != nil {
			state.touch(at)
			setStreamMetadata(&state.toolType, "code_interpreter_call", a.limit)
			state.arguments.set(streamJSONString(event.Code), a.limit)
		}
	case "response.output_item.added":
		if event.Item != nil {
			a.observeResponseItem(*event.Item, outputIndex, at, false)
		}
	case "response.output_item.done":
		if event.Item != nil {
			a.observeResponseItem(*event.Item, outputIndex, at, true)
			a.markResponseOutputStatus(outputIndex, event.Item.Status, event.Item.Error)
			complete, failed, message := streamStatus(event.Item.Status, event.Item.Error)
			outputs = append(outputs, a.finishResponseOutput(outputIndex, at, complete, failed, message)...)
		} else {
			outputs = append(outputs, a.finishResponseOutput(outputIndex, at, true, false, "")...)
		}
	case "response.content_part.added":
		if event.Part != nil {
			a.observeResponsePart(*event.Part, outputIndex, contentIndex, itemID, at, false)
		}
	case "response.content_part.done":
		if event.Part != nil {
			part := streamPartWithEventStatus(*event.Part, event.Status, event.Error)
			a.observeResponsePart(part, outputIndex, contentIndex, itemID, at, true)
		}
	case "response.reasoning_summary_part.added":
		if event.Part != nil {
			a.observeResponseSummaryPart(*event.Part, outputIndex, streamIndex(event.SummaryIndex, 0), itemID, at, false)
		}
	case "response.reasoning_summary_part.done":
		if event.Part != nil {
			part := streamPartWithEventStatus(*event.Part, event.Status, event.Error)
			a.observeResponseSummaryPart(part, outputIndex, streamIndex(event.SummaryIndex, 0), itemID, at, true)
		}
	}

	if toolType, status, ok := streamToolLifecycle(event.Type); ok {
		key := streamOutputKey{source: streamSourceResponses, kind: StreamOutputToolCall, choiceIndex: -1, outputIndex: outputIndex, contentIndex: -1, toolIndex: -1}
		state := a.states[key]
		if state != nil {
			setStreamMetadata(&state.itemID, itemID, a.limit)
			if state.toolID.string() == "" {
				setStreamMetadata(&state.toolID, itemID, a.limit)
			}
			setStreamMetadata(&state.toolType, toolType, a.limit)
			setStreamMetadata(&state.name, event.Name, a.limit)
			applyStreamStatus(state, status, event.Error)
			if state.failed && event.Message != "" {
				state.errorMessage = Truncate(event.Message)
			}
		}
	}

	if event.Response != nil && (event.Type == "response.completed" || event.Type == "response.failed" || event.Type == "response.incomplete") {
		for index, item := range event.Response.Output {
			a.observeResponseItem(item, index, at, true)
			a.markResponseOutputStatus(index, item.Status, item.Error)
		}
	}
	return outputs
}

func (a *StreamAccumulator) chatState(kind StreamOutputKind, choiceIndex int) *streamOutputState {
	key := streamOutputKey{source: streamSourceChat, kind: kind, choiceIndex: choiceIndex, outputIndex: -1, contentIndex: -1, toolIndex: -1}
	return a.state(key, StreamOutput{Kind: kind, ChoiceIndex: choiceIndex, HasChoiceIndex: true})
}

func (a *StreamAccumulator) chatToolState(choiceIndex, toolIndex int) *streamOutputState {
	key := streamOutputKey{source: streamSourceChat, kind: StreamOutputToolCall, choiceIndex: choiceIndex, outputIndex: -1, contentIndex: -1, toolIndex: toolIndex}
	return a.state(key, StreamOutput{Kind: StreamOutputToolCall, ChoiceIndex: choiceIndex, HasChoiceIndex: true, ToolCallIndex: toolIndex, HasToolCallIndex: true})
}

func (a *StreamAccumulator) nativeState(kind StreamOutputKind) *streamOutputState {
	key := streamOutputKey{source: streamSourceNative, kind: kind, choiceIndex: 0, outputIndex: -1, contentIndex: -1, toolIndex: -1}
	return a.state(key, StreamOutput{Kind: kind})
}

func (a *StreamAccumulator) responseTextState(kind StreamOutputKind, outputIndex, contentIndex int, itemID string) (*streamOutputState, streamOutputKey) {
	key := streamOutputKey{source: streamSourceResponses, kind: kind, choiceIndex: -1, outputIndex: outputIndex, contentIndex: contentIndex, toolIndex: -1}
	state := a.state(key, StreamOutput{Kind: kind, OutputIndex: outputIndex, HasOutputIndex: true, ContentIndex: contentIndex, HasContentIndex: true})
	if state != nil {
		setStreamMetadata(&state.itemID, itemID, a.limit)
	}
	return state, key
}

func (a *StreamAccumulator) responseToolState(outputIndex int, itemID string) (*streamOutputState, streamOutputKey) {
	key := streamOutputKey{source: streamSourceResponses, kind: StreamOutputToolCall, choiceIndex: -1, outputIndex: outputIndex, contentIndex: -1, toolIndex: -1}
	state := a.state(key, StreamOutput{Kind: StreamOutputToolCall, OutputIndex: outputIndex, HasOutputIndex: true})
	if state != nil {
		setStreamMetadata(&state.itemID, itemID, a.limit)
		if state.toolID.string() == "" {
			setStreamMetadata(&state.toolID, itemID, a.limit)
		}
	}
	return state, key
}

func (a *StreamAccumulator) responseSummaryState(outputIndex, summaryIndex int, itemID string) (*streamOutputState, streamOutputKey) {
	key := streamOutputKey{source: streamSourceResponses, kind: StreamOutputReasoning, choiceIndex: -1, outputIndex: outputIndex, contentIndex: -2 - summaryIndex, toolIndex: -1}
	state := a.state(key, StreamOutput{Kind: StreamOutputReasoning, OutputIndex: outputIndex, HasOutputIndex: true, ContentIndex: summaryIndex, HasContentIndex: true})
	if state != nil {
		setStreamMetadata(&state.itemID, itemID, a.limit)
	}
	return state, key
}

func (a *StreamAccumulator) appendResponseText(outputIndex, contentIndex int, itemID, content, refusal string, at time.Time) {
	if content == "" && refusal == "" {
		return
	}
	state, _ := a.responseTextState(StreamOutputResponse, outputIndex, contentIndex, itemID)
	if state == nil {
		return
	}
	state.touch(at)
	state.content.append(content, a.limit)
	state.refusal.append(refusal, a.limit)
}

func (a *StreamAccumulator) appendResponseReasoning(outputIndex, contentIndex int, itemID, reasoning string, at time.Time) {
	if reasoning == "" {
		return
	}
	state, _ := a.responseTextState(StreamOutputReasoning, outputIndex, contentIndex, itemID)
	if state == nil {
		return
	}
	state.touch(at)
	state.reasoning.append(reasoning, a.limit)
}

func (a *StreamAccumulator) appendResponseSummary(outputIndex, summaryIndex int, itemID, reasoning string, at time.Time) {
	if reasoning == "" {
		return
	}
	state, _ := a.responseSummaryState(outputIndex, summaryIndex, itemID)
	if state == nil {
		return
	}
	state.touch(at)
	state.reasoning.append(reasoning, a.limit)
}

func (a *StreamAccumulator) appendResponseTool(outputIndex int, itemID, toolType, name, arguments, input string, at time.Time) {
	if itemID == "" && toolType == "" && name == "" && arguments == "" && input == "" {
		return
	}
	state, _ := a.responseToolState(outputIndex, itemID)
	if state == nil {
		return
	}
	state.touch(at)
	setStreamMetadata(&state.toolType, toolType, a.limit)
	setStreamMetadata(&state.name, name, a.limit)
	state.arguments.append(arguments, a.limit)
	state.arguments.append(input, a.limit)
}

func (a *StreamAccumulator) observeResponseShellCommand(outputIndex, commandIndex int, itemID, command string, at time.Time, authoritative bool) {
	state, _ := a.responseToolState(outputIndex, itemID)
	if state == nil {
		return
	}
	state.touch(at)
	setStreamMetadata(&state.toolType, "shell_call", a.limit)
	if state.shellCommands == nil {
		state.shellCommands = make(map[int]*limitedStreamText)
	}
	part := state.shellCommands[commandIndex]
	if part == nil {
		if a.createdParts >= maxStreamOutputParts {
			a.limited = true
			return
		}
		a.createdParts++
		part = &limitedStreamText{}
		state.shellCommands[commandIndex] = part
	}
	if authoritative {
		part.set(command, a.limit)
	} else {
		part.append(command, a.limit)
	}
}

func (a *StreamAccumulator) observeResponsePart(part streamPart, outputIndex, contentIndex int, itemID string, at time.Time, authoritative bool) {
	kind := StreamOutputResponse
	if strings.Contains(part.Type, "reasoning") || strings.Contains(part.Type, "summary") {
		kind = StreamOutputReasoning
	}
	value := part.Text
	if value == "" {
		value = part.Refusal
	}
	if value == "" {
		if !authoritative {
			return
		}
	}
	state, _ := a.responseTextState(kind, outputIndex, contentIndex, itemID)
	if state == nil {
		return
	}
	if value != "" {
		state.touch(at)
	}
	if authoritative {
		if kind == StreamOutputReasoning {
			state.reasoning.set(part.Text, a.limit)
		} else if part.Refusal != "" {
			state.refusal.set(part.Refusal, a.limit)
		} else {
			state.content.set(part.Text, a.limit)
		}
		applyStreamStatus(state, part.Status, part.Error)
	} else if kind == StreamOutputReasoning {
		state.reasoning.append(part.Text, a.limit)
	} else if part.Refusal != "" {
		state.refusal.append(part.Refusal, a.limit)
	} else {
		state.content.append(part.Text, a.limit)
	}
}

func (a *StreamAccumulator) observeResponseSummaryPart(part streamPart, outputIndex, summaryIndex int, itemID string, at time.Time, authoritative bool) {
	state, _ := a.responseSummaryState(outputIndex, summaryIndex, itemID)
	if state == nil {
		return
	}
	if part.Text != "" {
		state.touch(at)
		if authoritative {
			state.reasoning.set(part.Text, a.limit)
		} else {
			state.reasoning.append(part.Text, a.limit)
		}
	}
	if authoritative {
		applyStreamStatus(state, part.Status, part.Error)
	}
}

func (a *StreamAccumulator) observeResponseItem(item streamItem, outputIndex int, at time.Time, authoritative bool) {
	switch item.Type {
	case "message":
		for contentIndex, part := range item.Content {
			a.observeResponsePart(part, outputIndex, contentIndex, item.ID, at, authoritative)
		}
	case "reasoning":
		for contentIndex, part := range item.Content {
			a.observeResponsePart(part, outputIndex, contentIndex, item.ID, at, authoritative)
		}
		for summaryIndex, part := range item.Summary {
			a.observeResponseSummaryPart(part, outputIndex, summaryIndex, item.ID, at, authoritative)
		}
	default:
		if !strings.HasSuffix(item.Type, "_call") && item.Type != "program" {
			return
		}
		arguments := streamItemArguments(item)
		state, _ := a.responseToolState(outputIndex, item.ID)
		if state == nil {
			return
		}
		state.touch(at)
		toolID := item.CallID
		if toolID == "" {
			toolID = item.ID
		}
		setStreamMetadata(&state.toolID, toolID, a.limit)
		setStreamMetadata(&state.toolType, item.Type, a.limit)
		setStreamMetadata(&state.name, item.Name, a.limit)
		if authoritative {
			state.arguments.set(arguments, a.limit)
			state.argumentsAuthoritative = arguments != ""
			applyStreamOutputStatus(state, item.Status, item.Error)
			if item.Type == "program" {
				state.completed = true
			}
		} else {
			state.arguments.append(arguments, a.limit)
		}
	}
}

func (a *StreamAccumulator) markResponseOutputStatus(outputIndex int, status string, raw json.RawMessage) {
	for key, state := range a.states {
		if key.source == streamSourceResponses && key.outputIndex == outputIndex {
			applyStreamOutputStatus(state, status, raw)
		}
	}
}

func (a *StreamAccumulator) finishResponseOutput(outputIndex int, at time.Time, complete, failed bool, message string) []StreamOutput {
	var outputs []StreamOutput
	for _, kind := range []StreamOutputKind{StreamOutputReasoning, StreamOutputResponse, StreamOutputToolCall} {
		outputs = append(outputs, a.finishResponseOutputKind(outputIndex, kind, at, complete, failed, message)...)
	}
	return outputs
}

func (a *StreamAccumulator) finishResponseOutputKind(outputIndex int, kind StreamOutputKind, at time.Time, complete, failed bool, message string) []StreamOutput {
	keys := make([]streamOutputKey, 0)
	for key := range a.states {
		if key.source == streamSourceResponses && key.outputIndex == outputIndex && key.kind == kind {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := keys[i].contentIndex, keys[j].contentIndex
		if left >= 0 && right < 0 {
			return true
		}
		if left < 0 && right >= 0 {
			return false
		}
		if left < 0 {
			left = -2 - left
			right = -2 - right
		}
		return left < right
	})
	parts := make([]StreamOutput, 0, len(keys))
	for _, key := range keys {
		if output, ok := a.finishKey(key, at, complete, failed, message); ok {
			parts = append(parts, output)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return []StreamOutput{a.mergeResponseOutputs(parts, at, complete, failed, message)}
}

func (a *StreamAccumulator) mergeResponseOutputs(parts []StreamOutput, at time.Time, complete, failed bool, message string) StreamOutput {
	state := streamOutputState{output: parts[0], started: true}
	state.output.Start = parts[0].Start
	state.output.ContentIndex = parts[0].ContentIndex
	state.output.HasContentIndex = parts[0].HasContentIndex
	if len(parts) > 1 {
		state.output.HasContentIndex = false
	}
	truncated := false
	observationLimited := a.limited
	protocolIncomplete := false
	complete = true
	failed = false
	message = ""
	for _, part := range parts {
		if part.Start.Before(state.output.Start) {
			state.output.Start = part.Start
		}
		state.reasoning.append(part.Reasoning, a.limit)
		state.content.append(part.Content, a.limit)
		state.refusal.append(part.Refusal, a.limit)
		state.arguments.append(part.Arguments, a.limit)
		setStreamMetadata(&state.name, part.Name, a.limit)
		setStreamMetadata(&state.toolType, part.ToolType, a.limit)
		setStreamMetadata(&state.toolID, part.ToolCallID, a.limit)
		setStreamMetadata(&state.itemID, part.ItemID, a.limit)
		setStreamMetadata(&state.finishReason, part.FinishReason, a.limit)
		truncated = truncated || part.Truncated
		observationLimited = observationLimited || part.ObservationLimited
		protocolIncomplete = protocolIncomplete || part.ProtocolIncomplete
		if part.Failed {
			failed = true
			complete = false
			if part.ErrorMessage != "" {
				message = part.ErrorMessage
			}
		} else if !part.Complete {
			complete = false
		}
	}
	output := state.finish(at, complete, failed, message, a.limit)
	output.Truncated = output.Truncated || truncated
	output.ObservationLimited = observationLimited
	output.ProtocolIncomplete = protocolIncomplete
	return output
}

func setStreamMetadata(field *limitedStreamText, value string, limit int) {
	if value != "" {
		if limit > maxStreamMetadataSize {
			limit = maxStreamMetadataSize
		}
		field.set(value, limit)
	}
}

func appendStreamMetadata(field *limitedStreamText, value string, limit int) {
	if value != "" {
		if limit > maxStreamMetadataSize {
			limit = maxStreamMetadataSize
		}
		field.append(value, limit)
	}
}

func applyStreamStatus(state *streamOutputState, status string, raw json.RawMessage) {
	if state == nil {
		return
	}
	_, failed, message := streamStatus(status, raw)
	if failed {
		state.failed = true
		if state.errorMessage == "" || message != "upstream reported an error" {
			state.errorMessage = message
		}
	} else if status == "incomplete" || status == "cancelled" || status == "canceled" {
		state.incomplete = true
	}
}

func applyStreamOutputStatus(state *streamOutputState, status string, raw json.RawMessage) {
	applyStreamStatus(state, status, raw)
	if state != nil && status == "completed" {
		state.completed = true
	}
}

func streamStatus(status string, raw json.RawMessage) (bool, bool, string) {
	if len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, true, errorMessage(raw)
	}
	switch status {
	case "failed", "error":
		return false, true, "upstream reported an error"
	case "incomplete", "cancelled", "canceled", "in_progress", "queued":
		return false, false, ""
	default:
		return true, false, ""
	}
}

func streamToolLifecycle(eventType string) (string, string, bool) {
	name := strings.TrimPrefix(eventType, "response.")
	if name == eventType {
		return "", "", false
	}
	for _, status := range []string{"completed", "failed", "incomplete"} {
		suffix := "." + status
		if strings.HasSuffix(name, suffix) {
			toolType := strings.TrimSuffix(name, suffix)
			if strings.HasSuffix(toolType, "_call") {
				return toolType, status, true
			}
		}
	}
	return "", "", false
}

func streamEventFailed(event streamEventPayload) bool {
	if event.Type == "response.failed" || event.Type == "error" {
		return true
	}
	return event.Type == "" && len(event.Error) > 0 && !bytes.Equal(bytes.TrimSpace(event.Error), []byte("null"))
}

func streamEventErrorMessage(event streamEventPayload) string {
	if event.Message != "" {
		return Truncate(event.Message)
	}
	if len(event.Error) > 0 && !bytes.Equal(bytes.TrimSpace(event.Error), []byte("null")) {
		return errorMessage(event.Error)
	}
	if event.Response != nil && len(event.Response.Error) > 0 && !bytes.Equal(bytes.TrimSpace(event.Response.Error), []byte("null")) {
		return errorMessage(event.Response.Error)
	}
	if code := rawErrorCode(event.Code); code != "" {
		return code
	}
	return "upstream reported an error"
}

func decodeStreamTerminal(payload []byte) (bool, bool, bool, string) {
	var envelope struct {
		Type     string          `json:"type"`
		Stop     *bool           `json:"stop"`
		Message  string          `json:"message"`
		Code     json.RawMessage `json:"code"`
		Error    json.RawMessage `json:"error"`
		Response json.RawMessage `json:"response"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return false, false, false, ""
	}
	switch envelope.Type {
	case "response.completed":
		return true, false, false, ""
	case "response.incomplete":
		return true, true, false, ""
	case "response.failed", "error":
		return true, false, true, streamEnvelopeErrorMessage(envelope.Message, envelope.Code, envelope.Error, envelope.Response)
	}
	if envelope.Stop != nil && *envelope.Stop {
		return true, false, false, ""
	}
	if envelope.Type == "" && len(envelope.Error) > 0 && !bytes.Equal(bytes.TrimSpace(envelope.Error), []byte("null")) {
		return true, false, true, streamEnvelopeErrorMessage(envelope.Message, envelope.Code, envelope.Error, envelope.Response)
	}
	return false, false, false, ""
}

func streamEnvelopeErrorMessage(message string, code, raw, response json.RawMessage) string {
	if message != "" {
		return Truncate(message)
	}
	if len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errorMessage(raw)
	}
	var nested struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(response, &nested) == nil && len(nested.Error) > 0 && !bytes.Equal(bytes.TrimSpace(nested.Error), []byte("null")) {
		return errorMessage(nested.Error)
	}
	if value := rawErrorCode(code); value != "" {
		return value
	}
	return "upstream reported an error"
}

func streamIndex(value *int, fallback int) int {
	if value != nil && *value >= 0 {
		return *value
	}
	return fallback
}

func streamJSONString(raw json.RawMessage) string {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func streamItemArguments(item streamItem) string {
	for _, raw := range []json.RawMessage{item.Arguments, item.Input, item.Code, item.Action, item.Actions, item.Queries, item.Operation} {
		if value := streamRawValue(raw); value != "" {
			return value
		}
	}
	return ""
}

func preferredStreamErrorMessage(state, fallback string) string {
	const generic = "upstream reported an error"
	switch {
	case state != "" && state != generic:
		return state
	case fallback != "" && fallback != generic:
		return fallback
	case state != "":
		return state
	default:
		return fallback
	}
}

func streamRawValue(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var compact bytes.Buffer
	if json.Compact(&compact, raw) == nil {
		return compact.String()
	}
	return ""
}

func streamPartWithEventStatus(part streamPart, status string, raw json.RawMessage) streamPart {
	if status != "" {
		part.Status = status
	}
	if len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		part.Error = raw
	}
	return part
}

func nonEmptyJSONString(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return false
	}
	return s != ""
}

func firstNonNil(vals ...*int64) *int64 {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}
