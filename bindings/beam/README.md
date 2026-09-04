# OMQ.beam

BEAM bindings for OMQ, backed by `omq-tokio`.

![OMQ.beam performance](https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/beam/doc/charts/bindings.svg)

Erlang is the base binding. Elixir and Gleam are thin wrappers over the same
Erlang module and NIF, so one Rust native library serves all three languages.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).
Benchmarks compare the three OMQ wrappers against `erlzmq` and `chumak`.
`exzmq` is not included because its current public API is CLIENT/SERVER-only.

## Install

Erlang:

```erlang
{deps, [{omq, "0.2.0"}]}.
```

Elixir:

```elixir
def deps do
  [{:omq_elixir, "~> 0.2"}]
end
```

Gleam:

```toml
[dependencies]
omq_gleam = ">= 0.2.0 and < 1.0.0"
```

## API Shape

- `omq:context(...)` owns native IO threads and creates sockets.
- Erlang is the base API. Elixir and Gleam wrappers keep the same native
  context and `inproc://` namespace through explicit share keys.
- Socket resources cover all 20 ZMQ socket types and use normal endpoint
  strings for bind/connect.
- Message calls support binaries, strings, Erlang terms, JSON, multipart, and
  nonblocking variants.
- Pub/sub controls, RADIO/DISH groups, polling, monitoring, proxies, and core
  libzmq-compatible options are exposed through the Erlang module.
- Treat each socket as owned by one BEAM process. This matches actor-style
  sequencing and keeps multipart frame state local to the caller.
- Create more sockets for more concurrent flows; share contexts when wrappers
  need the same `inproc://` namespace.
- PLAIN servers require a fixed credential allowlist through
  `omq:plain_server(Socket, [{Username, Password}, ...])` before bind. Bare
  `plain_server` option staging fails closed.

Example:

```erlang
{ok, Ctx} = omq:context(),
{ok, Pull} = omq:socket(Ctx, pull),
{ok, Push} = omq:socket(Ctx, push),
{ok, Endpoint} = omq:bind(Pull, <<"tcp://127.0.0.1:0">>),
ok = omq:connect(Push, Endpoint),
ok = omq:send(Push, <<"hello">>),
{ok, <<"hello">>} = omq:recv(Pull, 1000),
ok = omq:close(Push),
ok = omq:close(Pull),
ok = omq:term(Ctx).
```

Development commands: [`DEVELOPMENT.md`](DEVELOPMENT.md).
