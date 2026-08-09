# OMQ Go Binding

Idiomatic Go binding over `omq-tokio`. Rust owns OMQ contexts, sockets,
protocol state, compression, reconnect, and IO threads. Go exposes scalar
context-aware calls plus optional channel adapters.

Build native library first:

```sh
../../scripts/test-go.sh
```

Minimal use:

```go
ctx, _ := omq.Open(omq.Config{IOThreads: 2})
defer ctx.Close()

pull, _ := ctx.Socket(omq.Pull)
push, _ := ctx.Socket(omq.Push)
endpoint, _ := pull.Bind("inproc://example")
_ = push.Connect(endpoint)

_ = push.Send(context.Background(), omq.String("hello"))
msg, _ := pull.Recv(context.Background())
fmt.Println(msg.String())
```

Public calls are scalar. Native receive refills use OMQ batch APIs behind
the cgo boundary. Go does not expose public send or receive batch methods.
