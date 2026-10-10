<img src="doc/omq-logo.svg" alt="OMQ" width="525" />

Connect threads, processes, hosts, and languages without a broker. OMQ gives
you the same small send/recv model across in-process queues, IPC, TCP,
QUIC, UDP, WebSocket, compressed links, and language boundaries.

Your app shouldn't have to care about the network. Queues all the way!

OMQ follows [ZeroMQ](https://zeromq.org): same socket patterns, compatible
wire protocol, and libzmq-style APIs. The core is memory-safe Rust and does
not depend on libzmq, libsodium, or a C compiler.

- Messaging patterns for pipelines, publish/subscribe, request/reply,
  routed services, exclusive peers, and raw streams.
- Transports for threads, processes, hosts, browsers, and compressed links:
  inproc, IPC, TCP, UDP/QUIC/DART, WebSocket,
  `lz4+tcp://`, `lz4+ws://`, and `zstd+tcp://`.
- Security for open, password-authenticated, and encrypted connections:
  NULL, PLAIN, CURVE, and verified TLS for QUIC and secure WebSocket.
- Near-linear I/O scalability with OMQ-owned background threads on Linux,
  macOS, and Windows.
- No C compiler, no libzmq, no libsodium.
- Native bindings and compatibility APIs:
  - [C/C++](omq-libzmq/)
  - [Crystal](https://github.com/paddor/omq-binding.cr)
  - [BEAM: Erlang, Elixir, and Gleam](bindings/beam/)
  - [Go](bindings/go/)
  - [Java](bindings/java/)
  - [Lua](bindings/lua/)
  - [.NET](bindings/dotnet/)
  - [Node.js](bindings/node/)
  - [Python](bindings/pyomq/)
  - [Ruby](bindings/ruby/) and pure Ruby [OMQ.rb](https://github.com/zeromq/omq.rb)
  - [TypeScript](https://github.com/paddor/omq.ts) for browsers (ZWS transport only)
  - [Zig](bindings/zig/)

## Performance

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_pushpull_tcp.svg" alt="PUSH/PULL throughput: TCP implementations" width="950">
</p>
<details>
<summary>REQ/REP latency</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_reqrep_tcp.svg" alt="REQ/REP latency: TCP implementations" width="950">
</p>
</details>

<details>
<summary>PUB/SUB throughput</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_pubsub_tcp.svg" alt="PUB/SUB throughput: TCP implementations" width="950">
</p>
</details>

<details>
<summary>LZ4 PUSH/PULL throughput</summary>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/lz4_tcp.svg" alt="LZ4 PUSH/PULL throughput over TCP" width="950">
</p>

[Full compression transport benchmarks (LZ4 and Zstd)](BENCHMARKS_COMPRESSION.md)
</details>

[Full comparison charts](COMPARISONS.md) |
[Other protocol comparisons](PROTOCOL_COMPARISONS.md)

## The hard parts

OMQ is designed for real ZeroMQ behavior, not just happy-path PUSH/PULL throughput. You get:

- ZeroMQ semantics without extra tuning: no topology-specific socket types, no user-visible batching API, no manual reconnection loop.
- Transport failures are normal: reconnect, connect-before-bind, peer churn, and bind-side restarts are part of the design.
- Peer failures do not become user errors: `send()` and `recv()` keep working through disconnects, reconnects, slow consumers, and bind-side restarts.
- HWM back-pressure and routing fairness under load, not only in empty-queue examples.
- The hot paths are lock-free, size-aware and latency-conscious: tiny messages stay inline without allocation, inproc passes messages by value, and large payloads use zero-copy buffers where it matters.
- The only Rust ZeroMQ implementation following libzmq's architecture: application threads stay separate from dedicated background IO threads, IO work scales linearly across those threads, and peers are assigned to IO lanes statically. Ready for your thread-per-core app!
- Extensive tests and benchmarks: throughput, latency, CPU, fan-in/fan-out, fairness across transports.

## Usage

If you know ZeroMQ, you know OMQ. Same socket types, same connect/bind/send/recv:

```rust
use omq_tokio::{Context, Message, Options, SocketType};

let ctx = Context::new();

let push = ctx.socket(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hello")).await?;

let pull = ctx.socket(SocketType::Pull, Options::default());
pull.bind("tcp://127.0.0.1:5555".parse()?).await?;
let msg = pull.recv().await?;
assert_eq!(&msg[0], b"hello");
```

Runtime flavors:

| Flavor | Use when | IO placement |
|--------|----------|--------------|
| `Context::new().socket(...)` | Async API with OMQ-managed transport work | OMQ-owned background IO threads |
| `Context::new().blocking_socket(...)` | Classic/libzmq-like synchronous API | OMQ-owned background IO threads |
| `Context::current().socket(...)` | Embedding OMQ into an existing tokio app/runtime | Caller runtime, no OMQ-owned IO thread |
| `omq_tokio::exclusive::Socket::{connect, bind}(...)` | Lowest latency for one TCP peer | Caller task, no socket driver task |

More examples in [examples/zguide/](examples/zguide/), a
port of the ZeroMQ Guide patterns to OMQ.

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature | what it adds                                      | extra deps                       |
|---------|---------------------------------------------------|----------------------------------|
| `plain` | PLAIN username/password auth (RFC 24)             | -                                |
| `curve` | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `lz4`   | `lz4+tcp://` compression transport ([RFC](doc/lz4-rfc.md)) | `lz4rip` |
| `zstd`  | Experimental `zstd+tcp://` compression transport  | `zrip`                           |
| `ws`    | WebSocket (`ws://`) and secure WebSocket (`wss://`) transports | `rustls`, `rustls-native-certs` |
| `quic`  | QUIC (`quic://`) transport ([overview](doc/udp_transports.md#quic), [example](examples/quic.rs)) | `quinn`, `rustls`, `rustls-native-certs` |
| `dart`  | Reliable ordered UDP messages (`dart://`) with ultra-low p99 latency ([overview](doc/udp_transports.md#dart), [RFC](doc/dart-rfc.md)) | `quinn-udp` |

## Workspace

Four Cargo workspace crates plus language bindings.

| Crate | What it does | Unsafe policy |
|-------|--------------|---------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions | `#![forbid(unsafe_code)]` |
| [`omq-tokio`](omq-tokio/) | Multi-thread tokio backend (Linux/macOS/Windows) | `#![forbid(unsafe_code)]` |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq` dynamic/static library) | Unsafe C ABI boundary |
| [`omq-bench`](omq-bench/) | Benchmark runner and SVG chart generator | Bench-only process control and CPU accounting |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-tokio, sync + asyncio) | PyO3 FFI boundary |
| [`OMQ.Net`](bindings/dotnet/) | .NET binding (managed wrapper over omq-libzmq) | P/Invoke/native ABI boundary |
| [`omq-rs`](bindings/ruby/) | Ruby binding (rb-sys over omq-tokio, scheduler-aware synchronous API) | Ruby C API/native extension boundary |
| [`OMQ.java`](bindings/java/) | Java 21+ binding (JNI/FFM over omq-tokio, sync + async) | JNI/FFM boundary |
| [`OMQ.go`](bindings/go/) | Go 1.25 binding (cgo over omq-tokio, goroutine-safe API) | cgo/native ABI boundary |
| [`OMQ.node`](bindings/node/) | Node.js 24.11 binding (NAPI over omq-tokio, native addon) | NAPI/native addon boundary |
| [`OMQ.lua`](bindings/lua/) | Lua 5.4 binding (mlua native module over omq-libzmq) | mlua/native ABI boundary |
| [`OMQ.beam`](bindings/beam/) | Erlang binding plus Elixir and Gleam wrappers (Rustler NIF over omq-tokio) | BEAM NIF boundary |
| [`OMQ.zig`](bindings/zig/) | Zig 0.16 binding (thin wrapper over omq-libzmq) | C ABI boundary |

## Further reading

- [COMPARISONS.md](COMPARISONS.md): cross-implementation comparison charts.
- [PROTOCOL_COMPARISONS.md](PROTOCOL_COMPARISONS.md): throughput and
  request/reply-like latency against other messaging and RPC protocols.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): lz4+tcp
  throughput on bandwidth-limited links.
- [doc/architecture.md](doc/architecture.md): architecture and tokio
  backend internals.
- [doc/libzmq/semantics.md](doc/libzmq/semantics.md): exact compatibility
  notes for no-peer sends, linger, and HWM.
- [doc/lz4-rfc.md](doc/lz4-rfc.md): LZ4 compression transport wire
  format and dictionary shipping rules.
- [doc/quic-rfc.md](doc/quic-rfc.md): native QUIC transport, TLS verification,
  liveness, and reconnect rules.
- [doc/udp_transports.md](doc/udp_transports.md): UDP, QUIC, and DART purposes,
  configuration, and benchmarks over loopback and lossy links.

## Platform and requirements

- Rust 1.93 or newer (edition 2024).
- Linux (`x86_64`, `aarch64`, `i686-unknown-linux-gnu`, `armv7-unknown-linux-gnueabihf`)
- macOS (ARM/Intel)
- Windows

Linux is the primary development and benchmarking platform.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build and test commands, and [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md) for benchmarks.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.
