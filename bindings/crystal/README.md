# OMQ.cr

Crystal binding for OMQ backed by `omq-libzmq`.

This is a thin Crystal FFI binding. `omq-libzmq` owns contexts, sockets,
transports, reconnects, routing, compression, and `inproc://`; Crystal code
owns only the public wrapper and Crystal object lifecycle.

Architecture notes: [`doc/architecture.md`](doc/architecture.md).

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
  `PULL`, `PUSH`, `XPUB`, `XSUB`, `STREAM`.
- Flags: `DONTWAIT`, `SNDMORE`.
- `OMQ.context(io_threads = 1)` creates a context.
- `Context#socket("push", opts...)` creates a socket by name or numeric
  constant.
- `Context#term` closes the context. It errors while sockets are still live.
- `Context#close` aliases `#term`.
- `Socket#bind(endpoint)` binds and returns the resolved endpoint, including
  wildcard TCP ports when available.
- `Socket#connect(endpoint)` connects an endpoint.
- `Socket#close` closes the socket. It is idempotent.
- `Socket#send("bytes", flags = 0)` sends one frame.
- `Socket#send_parts(["part1", "part2"], flags = 0)` sends multipart.
- `Socket#recv(max_size = nil, flags = 0)` receives one frame.
- `Socket#try_recv(max_size = nil)` is nonblocking and returns `nil` when no
  frame is ready.
- `Socket#recv_parts(max_size = nil, flags = 0)` receives all frames in one
  message.
- `Socket#subscribe(prefix)` and `Socket#unsubscribe(prefix)` manage SUB
  prefixes.
- `Socket#set_linger(ms)`, `Socket#set_send_timeout(ms)`,
  `Socket#set_recv_timeout(ms)`, `Socket#set_send_hwm(value)`,
  `Socket#set_recv_hwm(value)`, `Socket#set_arena_threshold(bytes)`, and
  `Socket#get_arena_threshold` expose supported socket options.

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
