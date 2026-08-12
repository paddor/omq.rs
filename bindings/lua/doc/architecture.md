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
  -> raw Lua C userdata method
  -> zmq_send
  -> omq-libzmq send path
  -> omq-tokio socket/routing/send pipe
  -> IO thread encodes and writes transport
```

Receive:

```text
IO thread reads and decodes transport
  -> omq-libzmq receive queue
  -> raw Lua C userdata method
  -> zmq_recv / zmq_msg_recv
  -> lua_pushlstring
```

Single-part messages up to OMQ's inline cutoff stay inline in OMQ message
storage. The Lua binding still allocates a Lua string for every received
message, as lzmq does.

## Small-Message Cost

For 16 byte TCP PUSH/PULL, fixed overhead dominates. The hot path still pays:

- Lua VM method dispatch.
- `omq-libzmq` C ABI checks.
- OMQ routing, HWM, timeout, and closed-peer checks.
- Lua string allocation on receive.

lzmq is closer to:

```text
Lua C function -> libzmq -> lua_pushlstring
```

OMQ.lua is closer to:

```text
Lua C userdata method -> omq-libzmq -> omq-tokio -> lua_pushlstring
```

The raw userdata keeps the mlua layer out of per-message send/recv. Setup,
socket options, helper peers, docs, and tests still live in the public Lua
wrapper and Rust module.
