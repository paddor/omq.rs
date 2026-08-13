# OMQ.cr

Crystal binding for OMQ backed by `omq-libzmq`.

This is a thin Crystal FFI binding. `omq-libzmq` owns contexts, sockets,
transports, reconnects, routing, compression, and `inproc://`; Crystal code
owns only the public wrapper and Crystal object lifecycle.

Architecture notes: [`doc/architecture.md`](doc/architecture.md).

## Shard Packaging

This shard currently lives under `bindings/crystal` in the OMQ.rs monorepo.
Crystal Shards still assumes a `shard.yml` at the repository root for Git
dependencies, so direct use from this repository depends on monorepo shard
support. Track that discussion in
[crystal-lang/shards#635](https://github.com/crystal-lang/shards/issues/635).

## Build And Test

Requires Crystal 1.21 or newer and a Rust toolchain.

```sh
./scripts/test-crystal.sh
```

Manual equivalent:

```sh
cargo build -p omq-libzmq
export LIBRARY_PATH="$PWD/target/debug:$LIBRARY_PATH"
export LD_LIBRARY_PATH="$PWD/target/debug:$LD_LIBRARY_PATH"
crystal spec bindings/crystal/spec --link-flags "-L$PWD/target/debug -Wl,-rpath,$PWD/target/debug"
```

## API Shape

- Socket constants: `PAIR`, `PUB`, `SUB`, `REQ`, `REP`, `DEALER`, `ROUTER`,
  `PULL`, `PUSH`, `XPUB`, `XSUB`, `STREAM`, `SERVER`, `CLIENT`, `RADIO`,
  `DISH`, `GATHER`, `SCATTER`, `DGRAM`, `PEER`, and `CHANNEL`.
- Flags and poll events: `DONTWAIT`, `NOBLOCK`, `SNDMORE`, `POLLIN`,
  `POLLOUT`, `POLLERR`, and `POLLPRI`.
- Context, socket option, monitor, message, mechanism, and draft constants
  match `omq-libzmq/include/zmq.h`.
- `OMQ::LibZMQ` declares the raw exported `zmq_*` and `omq_ctx_*` ABI for
  code that needs lower-level access.
- `OMQ.context(io_threads = 1)` creates a context.
- `OMQ.context_from_share_key(key)` imports a shared in-process OMQ context.
- `OMQ.version`, `OMQ.has(capability)`, `OMQ.curve_keypair`,
  `OMQ.curve_public(secret_key)`, `OMQ.z85_encode(bytes)`, and
  `OMQ.z85_decode(string)` expose libzmq utility APIs.
- `OMQ.poll(items, timeout_ms = -1)`, `OMQ::PollItem`, and `OMQ::Poller`
  expose `zmq_poll` and `zmq_poller_*`.
- `OMQ.proxy` and `OMQ.proxy_steerable` expose libzmq proxy helpers.
- `Context#socket("push", opts...)` creates a socket by name or numeric
  constant.
- Socket keyword options cover supported `omq-libzmq` options: linger,
  timeouts, HWM, identity/routing ID, reconnect, heartbeat, handshake,
  max message size, router options, TCP keepalive, buffers, XPUB flags,
  IPv6, immediate, PLAIN, CURVE, WSS, and `OMQ_ARENA_THRESHOLD`.
- `Context#term` closes the context. It errors while sockets are still live.
- `Context#close` aliases `#term`.
- `Context#set`, `Context#set_string`, `Context#set_bytes`, `Context#get`,
  `Context#get_ext_i32`, `Context#get_ext_string`, `Context#get_ext_bytes`,
  `Context#shutdown`, and `Context#share_key` expose native context APIs.
- `Socket#bind(endpoint)` binds and returns the resolved endpoint, including
  wildcard TCP ports when available.
- `Socket#connect(endpoint)` connects an endpoint.
- `Socket#unbind(endpoint)`, `Socket#disconnect(endpoint)`,
  `Socket#connect_peer(endpoint)`, and `Socket#disconnect_peer(routing_id)`
  expose endpoint lifecycle helpers.
- `Socket#join(group)`, `Socket#leave(group)`, and
  `Socket#send_group(group, payload)` support RADIO/DISH.
- `Socket#monitor`, `Socket#monitor_versioned`,
  `Socket#monitor_pipes_stats`, and `Socket#peer_state` expose monitor APIs.
- `Socket#close` closes the socket. It is idempotent.
- `Socket#send("bytes", flags = 0)` sends one frame.
- `Socket#send_const("bytes", flags = 0)` exposes `zmq_send_const`.
- `Socket#send_parts(["part1", "part2"], flags = 0)` sends multipart.
- `Socket#recv(max_size = nil, flags = 0)` receives one frame.
- `Socket#try_recv(max_size = nil)` is nonblocking and returns `nil` when no
  frame is ready.
- `Socket#recv_parts(max_size = nil, flags = 0)` receives all frames in one
  message.
- `Socket#subscribe(prefix)` and `Socket#unsubscribe(prefix)` manage SUB
  prefixes.
- `Socket#type`, `Socket#events`, `Socket#last_endpoint`,
  `Socket#get_option_i32`, `Socket#get_option_i64`,
  `Socket#get_option_string`, `Socket#get_option_bytes`, and matching
  `Socket#set_option_*` methods expose generic socket options.

Example:

```crystal
require "omq"

ctx = OMQ.context
pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
push = ctx.socket("push", linger: 0, send_timeout: 1000)

endpoint = pull.bind("tcp://127.0.0.1:*")
push.connect(endpoint)
push.send("hello")
puts pull.recv

push.close
pull.close
ctx.term
```

## Performance Chart

![OMQ.cr performance](https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/crystal/doc/charts/bindings.svg)

The comparison line uses `zeromq-crystal`'s `LibZMQ` FFI layer. Its
high-level `ZMQ::Socket` wrapper still references Crystal APIs removed before
Crystal 1.21.

Generate a quick local chart:

```sh
bindings/crystal/scripts/update_perf.py --quick
```

Benchmark rows append to:

```text
~/.cache/omq.cr/bindings.jsonl
```
