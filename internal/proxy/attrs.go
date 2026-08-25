package proxy

import (
	"bytes"
	"encoding/json"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	AttrProviderName   = attribute.Key("gen_ai.provider.name")
	AttrOperationName  = attribute.Key("gen_ai.operation.name")
	AttrConversationID = attribute.Key("gen_ai.conversation.id")
	AttrRequestModel   = attribute.Key("gen_ai.request.model")
	AttrRequestStream  = attribute.Key("gen_ai.request.stream")
	AttrOutputType     = attribute.Key("gen_ai.output.type")
	AttrResponseModel  = attribute.Key("gen_ai.response.model")
	AttrResponseID     = attribute.Key("gen_ai.response.id")
	AttrInputTokens    = attribute.Key("gen_ai.usage.input_tokens")
	AttrOutputTokens   = attribute.Key("gen_ai.usage.output_tokens")

	AttrRequestMaxTokens        = attribute.Key("gen_ai.request.max_tokens")
	AttrRequestTemperature      = attribute.Key("gen_ai.request.temperature")
	AttrRequestTopP             = attribute.Key("gen_ai.request.top_p")
	AttrRequestTopK             = attribute.Key("gen_ai.request.top_k")
	AttrRequestFrequencyPenalty = attribute.Key("gen_ai.request.frequency_penalty")
	AttrRequestPresencePenalty  = attribute.Key("gen_ai.request.presence_penalty")
	AttrRequestSeed             = attribute.Key("gen_ai.request.seed")
	AttrRequestChoiceCount      = attribute.Key("gen_ai.request.choice.count")
	AttrRequestStopSequences    = attribute.Key("gen_ai.request.stop_sequences")
	AttrResponseFinishReasons   = attribute.Key("gen_ai.response.finish_reasons")

	AttrHTTPMethod   = attribute.Key("http.request.method")
	AttrURLPath      = attribute.Key("url.path")
	AttrStatusCode   = attribute.Key("http.response.status_code")
	AttrRequestBody  = attribute.Key("llamaproxy.request.body")
	AttrResponseBody = attribute.Key("llamaproxy.response.body")

	AttrTimingsPromptMS           = attribute.Key("llamacpp.timings.prompt_ms")
	AttrTimingsPredictedMS        = attribute.Key("llamacpp.timings.predicted_ms")
	AttrTimingsPredictedPerSecond = attribute.Key("llamacpp.timings.predicted_per_second")
	AttrTimingsCacheN             = attribute.Key("llamacpp.timings.cache_n")

	AttrServerSentEventsChunkCount       = attribute.Key("llamaproxy.server_sent_events_chunk_count")
	AttrClientDisconnected               = attribute.Key("llamaproxy.client_disconnected")
	AttrServerSentEventsDroppedLineCount = attribute.Key("llamaproxy.server_sent_events_dropped_line_count")
	AttrResponseIncomplete               = attribute.Key("llamaproxy.response_incomplete")
	AttrUpstreamError                    = attribute.Key("llamaproxy.upstream_error")
	AttrOutcome                          = attribute.Key("llamaproxy.outcome")
	AttrResponseEncoding                 = attribute.Key("llamaproxy.response_encoding")
	AttrErrorType                        = attribute.Key("error.type")
	AttrShutdownInterrupted              = attribute.Key("llamaproxy.shutdown_interrupted")
	AttrResponseBodyTruncated            = attribute.Key("llamaproxy.response_body_truncated")
	AttrTraceRequestBodyTruncated        = attribute.Key("llamaproxy.trace_request_body_truncated")
	AttrTraceResponseBodyTruncated       = attribute.Key("llamaproxy.trace_response_body_truncated")
	AttrTraceRequestBodySplitLimited     = attribute.Key("llamaproxy.trace_request_body_split_limited")
	AttrTraceResponseBodySplitLimited    = attribute.Key("llamaproxy.trace_response_body_split_limited")
	AttrTraceRequestHeaderLimited        = attribute.Key("llamaproxy.trace_request_header_limited")
	AttrTraceResponseHeaderLimited       = attribute.Key("llamaproxy.trace_response_header_limited")
	AttrSessionRequestCount              = attribute.Key("llamaproxy.session.request_count")
	AttrSessionModels                    = attribute.Key("llamaproxy.session.models")
	AttrSessionEndReason                 = attribute.Key("llamaproxy.session.end_reason")
	AttrChoiceIndex                      = attribute.Key("llamaproxy.choice_index")
	AttrOutputIndex                      = attribute.Key("llamaproxy.output_index")
	AttrContentIndex                     = attribute.Key("llamaproxy.content_index")
	AttrToolCallIndex                    = attribute.Key("llamaproxy.tool_call_index")
	AttrToolCallID                       = attribute.Key("llamaproxy.tool_call_id")
	AttrOutputItemID                     = attribute.Key("llamaproxy.output_item_id")
	AttrResponseBodyReasoningContent     = attribute.Key("llamaproxy.response.body.reasoning_content")
	AttrResponseBodyContent              = attribute.Key("llamaproxy.response.body.content")
	AttrResponseBodyRefusal              = attribute.Key("llamaproxy.response.body.refusal")
	AttrResponseBodyType                 = attribute.Key("llamaproxy.response.body.type")
	AttrResponseBodyName                 = attribute.Key("llamaproxy.response.body.name")
	AttrResponseBodyArguments            = attribute.Key("llamaproxy.response.body.arguments")
	AttrResponseBodyFinishReason         = attribute.Key("llamaproxy.response.body.finish_reason")
)

const System = "llama.cpp"

const (
	maxTracedBodySize           = 256 << 10
	maxSplitBodyFields          = 64
	maxSplitBodyFieldNameLength = 128
	maxSplitBodyDepth           = 8
	maxSplitBodyAttributes      = 256
	maxSplitBodyValues          = 256
	maxHeaderAttributes         = 64
	maxHeaderValues             = 32
)

const (
	SpanTimeToFirstToken = "time_to_first_token"
	SpanGeneration       = "generation"
	SpanReasoning        = "reasoning"
	SpanResponse         = "response"
	SpanToolCall         = "tool_call"
)

func responseModelIdentifier(model string) string {
	if cut := strings.LastIndexAny(model, `/\`); cut >= 0 {
		model = model[cut+1:]
	}
	return strings.TrimSuffix(model, ".gguf")
}

func requestHeaderAttributes(header http.Header) ([]attribute.KeyValue, bool) {
	return headerAttributes(header, semconv.HTTPRequestHeader)
}

func responseHeaderAttributes(header http.Header) ([]attribute.KeyValue, bool) {
	return headerAttributes(header, semconv.HTTPResponseHeader)
}

func headerAttributes(header http.Header, build func(string, ...string) attribute.KeyValue) ([]attribute.KeyValue, bool) {
	limited := false
	normalized := make(map[string][]string, len(header))
	for name, values := range header {
		key := strings.ToLower(name)
		for _, value := range values {
			if len(normalized[key]) >= maxHeaderValues {
				limited = true
				break
			}
			normalized[key] = append(normalized[key], strings.ToValidUTF8(value, "\uFFFD"))
		}
	}
	keys := make([]string, 0, len(normalized))
	for key := range normalized {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > maxHeaderAttributes {
		keys = keys[:maxHeaderAttributes]
		limited = true
	}
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		attrs = append(attrs, build(key, normalized[key]...))
	}
	return attrs, limited
}

func traceBody(body []byte) string {
	return strings.ToValidUTF8(string(body), "\uFFFD")
}

func traceBodyAttributes(key, limitKey attribute.Key, body []byte, truncated bool) []attribute.KeyValue {
	if !truncated {
		if attrs, ok := splitJSONBodyAttributes(key, limitKey, body); ok {
			return attrs
		}
	}
	return []attribute.KeyValue{key.String(traceBody(body))}
}

func splitJSONBodyAttributes(key, limitKey attribute.Key, body []byte) ([]attribute.KeyValue, bool) {
	root := bytes.TrimSpace(body)
	if len(root) == 0 || root[0] != '{' || !json.Valid(root) {
		return nil, false
	}
	names, fields, ok := jsonObjectFields(root)
	if !ok || len(names) == 0 || len(names) > maxSplitBodyFields {
		return nil, false
	}
	splitter := newBodySplitter()
	attrs := make([]attribute.KeyValue, 0, len(names))
	for _, field := range names {
		raw := bytes.TrimSpace(fields[field])
		if len(raw) == 0 {
			return nil, false
		}
		path := string(key) + "." + field
		switch {
		case field == messagesBodyField && raw[0] == '[':
			splitter.walkMessages(string(key)+"."+messageBodyPrefix, raw)
		case raw[0] == '{' || raw[0] == '[':
			splitter.walk(path, raw, 1)
		default:
			value, ok := jsonBodyFieldValue(raw)
			if !ok {
				return nil, false
			}
			attrs = append(attrs, attribute.KeyValue{Key: attribute.Key(path), Value: value})
		}
	}
	attrs = append(attrs, splitter.attributes()...)
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Key < attrs[j].Key })
	if splitter.limited {
		attrs = append(attrs, limitKey.Bool(true))
	}
	if len(attrs) == 0 {
		return nil, false
	}
	return attrs, true
}

const (
	messagesBodyField  = "messages"
	messageBodyPrefix  = "message"
	messageRoleField   = "role"
	defaultMessageRole = "default"
)

type bodySplitter struct {
	paths   []string
	values  map[string][]string
	limited bool
}

func newBodySplitter() *bodySplitter {
	return &bodySplitter{values: make(map[string][]string)}
}

func (b *bodySplitter) walkMessages(base string, raw json.RawMessage) {
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		b.emitRaw(base, raw)
		return
	}
	for _, element := range elements {
		element = bytes.TrimSpace(element)
		if len(element) == 0 {
			continue
		}
		if element[0] != '{' {
			b.walk(base+"."+defaultMessageRole, element, 2)
			continue
		}
		names, fields, ok := jsonObjectFields(element)
		if !ok {
			b.emitRaw(base+"."+defaultMessageRole, element)
			continue
		}
		role := defaultMessageRole
		if name, ok := jsonStringValue(fields[messageRoleField]); ok && validBodyFieldName(name) {
			role = name
		}
		path := base + "." + role
		sort.Strings(names)
		for _, name := range names {
			if name == messageRoleField {
				continue
			}
			b.walk(path+"."+name, fields[name], 2)
		}
	}
}

func (b *bodySplitter) walk(path string, raw json.RawMessage, depth int) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return
	}
	switch raw[0] {
	case '{':
		if depth > maxSplitBodyDepth {
			b.emitRaw(path, raw)
			return
		}
		names, fields, ok := jsonObjectFields(raw)
		if !ok {
			b.emitRaw(path, raw)
			return
		}
		sort.Strings(names)
		for _, name := range names {
			b.walk(path+"."+name, fields[name], depth+1)
		}
	case '[':
		if depth > maxSplitBodyDepth {
			b.emitRaw(path, raw)
			return
		}
		var elements []json.RawMessage
		if err := json.Unmarshal(raw, &elements); err != nil {
			b.emitRaw(path, raw)
			return
		}
		for _, element := range elements {
			b.walk(path, element, depth+1)
		}
	default:
		b.emit(path, jsonLeafString(raw))
	}
}

func (b *bodySplitter) emitRaw(path string, raw json.RawMessage) {
	b.limited = true
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return
	}
	b.emit(path, traceBody(compact.Bytes()))
}

func (b *bodySplitter) emit(path string, value string) {
	values, exists := b.values[path]
	if !exists {
		if len(b.values) >= maxSplitBodyAttributes {
			b.limited = true
			return
		}
		b.paths = append(b.paths, path)
	}
	if len(values) >= maxSplitBodyValues {
		b.limited = true
		return
	}
	b.values[path] = append(values, value)
}

func (b *bodySplitter) attributes() []attribute.KeyValue {
	sort.Strings(b.paths)
	attrs := make([]attribute.KeyValue, 0, len(b.paths))
	for _, path := range b.paths {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key(path),
			Value: attribute.StringSliceValue(b.values[path]),
		})
	}
	return attrs
}

func jsonObjectFields(raw json.RawMessage) ([]string, map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, nil, false
	}
	var names []string
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || !validBodyFieldName(name) {
			return nil, nil, false
		}
		if _, exists := fields[name]; exists {
			return nil, nil, false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, nil, false
		}
		names = append(names, name)
		fields[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, nil, false
	}
	return names, fields, true
}

func jsonStringValue(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

func jsonLeafString(raw json.RawMessage) string {
	if value, ok := jsonStringValue(raw); ok {
		return traceBody([]byte(value))
	}
	return string(raw)
}

func validBodyFieldName(field string) bool {
	if len(field) == 0 || len(field) > maxSplitBodyFieldNameLength {
		return false
	}
	for i := range len(field) {
		char := field[i]
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func jsonBodyFieldValue(raw json.RawMessage) (attribute.Value, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return attribute.Value{}, false
	}
	switch raw[0] {
	case '"':
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return attribute.Value{}, false
		}
		return attribute.StringValue(value), true
	case 't':
		return attribute.BoolValue(true), true
	case 'f':
		return attribute.BoolValue(false), true
	case 'n':
		return attribute.StringValue("null"), true
	case '[', '{':
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil {
			return attribute.Value{}, false
		}
		return attribute.StringValue(traceBody(compact.Bytes())), true
	default:
		return jsonNumberFieldValue(string(raw)), true
	}
}

func jsonNumberFieldValue(raw string) attribute.Value {
	if !strings.ContainsAny(raw, ".eE") {
		if integer, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return attribute.Int64Value(integer)
		}
		return attribute.StringValue(raw)
	}
	number, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return attribute.StringValue(raw)
	}
	original, originalOK := new(big.Rat).SetString(raw)
	represented, representedOK := new(big.Rat).SetString(strconv.FormatFloat(number, 'g', -1, 64))
	if !originalOK || !representedOK || original.Cmp(represented) != 0 {
		return attribute.StringValue(raw)
	}
	return attribute.Float64Value(number)
}

func boundedTraceBody(body []byte) ([]byte, bool) {
	if len(body) <= maxTracedBodySize {
		return body, false
	}
	cut := maxTracedBodySize
	for cut > 0 && !utf8.RuneStart(body[cut]) {
		cut--
	}
	return body[:cut], true
}
