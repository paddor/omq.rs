# OMQ Elixir

Elixir wrapper for OMQ.beam.

This package depends on the Erlang `omq` package, which owns the Rust NIF and
native OMQ runtime. The Elixir module keeps the same socket semantics while
presenting Elixir-friendly names and return values.

```elixir
{:ok, ctx} = OMQ.context()
{:ok, pull} = OMQ.socket(ctx, :pull)
{:ok, push} = OMQ.socket(ctx, :push)
{:ok, endpoint} = OMQ.bind(pull, "tcp://127.0.0.1:0")
:ok = OMQ.connect(push, endpoint)
:ok = OMQ.send(push, "hello")
{:ok, "hello"} = OMQ.recv_string(pull, 1000)
```

Development commands live in the parent OMQ.beam package.
