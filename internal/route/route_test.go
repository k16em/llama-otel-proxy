package route

import "testing"

func Test_計装対象のパスに対応する操作名が定義されている(t *testing.T) {
	want := map[string]string{
		"/v1/chat/completions": "chat",
		"/v1/completions":      "text_completion",
		"/v1/responses":        "chat",
		"/v1/embeddings":       "embeddings",
		"/completion":          "text_completion",
		"/infill":              "text_completion",
		"/v1/rerank":           "rerank",
		"/v1/reranking":        "rerank",
		"/rerank":              "rerank",
	}
	for path, op := range want {
		got, ok := Operation(path)
		if !ok || got != op {
			t.Errorf("Operation(%q) = %q, %v; want %q, true", path, got, ok, op)
		}
	}
}

func Test_計装対象外のパスに操作名が定義されていない(t *testing.T) {

	for _, path := range []string{
		"/logs/stream",
		"/api/events",
		"/ui/index.html",
		"/v1/messages",
		"/v1/messages/count_tokens",
		"/v1/models",
		"/running",
		"/health",
		"/v1/chat/completions/",
		"/v1/chat",
		"",
		"/",
	} {
		if op, ok := Operation(path); ok {
			t.Errorf("Operation(%q) = %q, true; want not instrumented", path, op)
		}
	}
}
