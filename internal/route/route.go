package route

var operations = map[string]string{
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

func Operation(path string) (string, bool) {
	op, ok := operations[path]
	return op, ok
}
