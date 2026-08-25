# OMQ.beam

BEAM bindings for OMQ, backed by `omq-tokio`.

![OMQ.beam performance](https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/beam/doc/charts/bindings.svg)

Erlang is the base binding. Elixir and Gleam are thin wrappers over the same
Erlang module and NIF, so one Rust native library serves all three languages.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).
Benchmarks compare the three OMQ wrappers against `erlzmq` and `chumak`.
`exzmq` is not included because its current public API is CLIENT/SERVER-only.

## API Shape

- `omq:context/0,1` owns native IO threads.
- `omq:socket/2` creates Erlang socket resources for all 20 ZMQ socket types.
- `bind`, `bind_to_random_port`, `connect`, `unbind`, and `disconnect` use
  normal endpoint strings.
- `send`, `try_send`, `recv`, `recv_frame`, `try_recv`, `send_multipart`, and
  `recv_multipart` cover blocking and nonblocking message calls.
- `poll/2` and `select/4` provide libzmq-style readiness helpers.
- `setsockopt/3` and `getsockopt/2` cover core libzmq-compatible options and
  OMQ transport options.
- `subscribe`, `unsubscribe`, `join`, `leave`, and `send_group` expose
  pub/sub and RADIO/DISH controls.

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
