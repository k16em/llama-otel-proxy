# llama-otel-proxy
## これはなんだ
OpenAI互換のAPIの前段に置いて、reasoningやtool_useなどをTraceとして記録するためのプロキシ  
認証などないので、ローカルで起動することを推奨

## ビルド
```sh
CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o llamaproxy ./cmd/llamaproxy
```

## 実行
`config/config.example.yaml` を参考にconfigファイルを作ってから  

```sh
llamaproxy --config ${configファイルの場所}
```

