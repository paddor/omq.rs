# OMQ.beam

BEAM bindings for OMQ, backed by `omq-tokio`.

![OMQ.beam performance](https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/beam/doc/charts/bindings.svg)

Erlang is the base binding. Elixir and Gleam are thin wrappers over the same
Erlang module and NIF, so one Rust native library serves all three languages.

Architecture detail: [`doc/architecture.md`](doc/architecture.md).
Benchmarks compare the three OMQ wrappers against `erlzmq` and `chumak`.
`exzmq` is not included because its current public API is CLIENT/SERVER-only.

## API Shape

- `omq:context/0,1` owns native IO threads. `context_instance/0,1` and
  `instance/0,1` return a process-wide singleton.
- `term/1` and `destroy/1` close contexts. `backend_name/0`, `version/0`,
  `omq_version/0`, `omq_version_info/0`, `zmq_version/0`,
  `zmq_version_info/0`, and `strerror/1` expose metadata and compatibility
  helpers.
- `share_key/1` and `from_share_key/1` import an existing native context for
  cross-wrapper inproc use.
- `omq:socket/2` creates Erlang socket resources for all 20 ZMQ socket types.
- `bind`, `bind_to_random_port`, `connect`, `unbind`, and `disconnect` use
  normal endpoint strings.
- `send`, `send_string`, `send_json`, `send_term`, `try_send`, `recv`,
  `recv_string`, `recv_json`, `recv_term`, `recv_frame`, `try_recv`,
  `send_multipart`, and `recv_multipart` cover blocking and nonblocking
  message calls.
- `poll/2` and `select/4` provide libzmq-style readiness helpers.
- `monitor`, `connections`, and `connection_info` expose backend lifecycle
  state.
- `proxy/2,3`, `proxy_steerable/4`, and `device/3` forward between sockets,
  with optional capture and PAUSE/RESUME/TERMINATE control.
- `setsockopt/3` and `getsockopt/2` cover core libzmq-compatible options and
  OMQ transport options, plus `set/get`, `set_hwm/get_hwm`, string option
  aliases, device constants, mechanism constants, and poll constants.
- `curve_keypair/0`, `curve_public/1`, and `has/1` expose optional feature
  state.
- `socket_id/1`, `closed/1`, and `context_closed/1` expose wrapper state.
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
