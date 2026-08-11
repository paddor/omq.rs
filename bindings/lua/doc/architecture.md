# OMQ.lua Architecture

Lua 5.4 binding for `omq-libzmq`. The public module is `lua/omq.lua`.
The native module is `bindings/lua/native`, a Rust `cdylib` loaded as
`omq_native`.

## Source Layout

```text
lua/omq.lua           public Lua API, socket-name mapping, option helpers
native/src/lib.rs     mlua module, userdata, omq-libzmq calls
scripts/bench_peer.lua
                      benchmark peer process for OMQ.lua and lzmq
scripts/update_perf.py
                      append-only bench runner and SVG chart generator
tests/                Lua API and interop tests
```

`omq.lua` returns native socket userdata directly. The `omq_native` module is
an implementation detail used by the public wrapper and tests.

## Threading

Each Lua socket follows the libzmq rule: one socket, one application thread.
`SocketInner` stores the raw `omq-libzmq` socket pointer in an `Rc`, so the
socket handle is not `Send` or `Sync` at the Rust type level. `close()` swaps
the raw pointer to zero so drop and explicit close are idempotent.

`omq-libzmq` owns the OMQ context and IO threads. Lua caller threads do not own
transport, reconnect, ZMTP, compression, or routing state.

## Data Path

Send:

```text
Lua Socket:send
  -> mlua userdata method
  -> omq_native::NativeSocket::send
  -> zmq_send
  -> omq-libzmq send path
  -> omq-tokio socket/routing/send pipe
  -> IO thread encodes and writes transport
```

Receive:

```text
IO thread reads and decodes transport
  -> omq-libzmq receive queue
  -> zmq_msg_recv
  -> Lua string
```

Single-part small messages stay inline in OMQ message storage. The Lua binding
still allocates a Lua string for every received message, as lzmq does.

## Small-Message Cost

For 16 byte TCP PUSH/PULL, fixed overhead dominates. The current hot path pays:

- Lua VM method dispatch.
- mlua userdata type check and borrow.
- Rust callback and error conversion.
- `omq-libzmq` C ABI checks.
- OMQ routing, HWM, timeout, and closed-peer checks.
- Lua string allocation on receive.

lzmq is closer to:

```text
Lua C function -> libzmq -> lua_pushlstring
```

OMQ.lua is closer to:

```text
Lua -> mlua callback -> Rust userdata -> omq-libzmq -> omq-tokio -> mlua string
```

The extra safety and integration layers cost little in absolute time, but at
3M messages/s a 50 ns per-message delta is visible.

OPTIMIZATION: If small-message throughput becomes a focused target, add raw
Lua C API fast paths for `Socket:send(string[, flags])` and
`Socket:recv([max_size, flags])`. They can store the raw socket pointer in Lua
userdata, call `zmq_send`/`zmq_msg_recv` directly, and use `lua_pushlstring` for
receive. Keep setup, options, multipart, docs, and tests in the current mlua
layer. This narrows the gap to lzmq but adds unsafe Lua stack code and needs
dedicated regression and perf tests.
