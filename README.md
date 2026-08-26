<img src="doc/omq-logo.svg" alt="OMQ" width="525" />

Connect threads, processes, hosts, and languages without a broker. OMQ gives
you the same small send/recv model across in-process queues, IPC, TCP,
WebSocket, compressed links, and language boundaries.

OMQ follows [ZeroMQ](https://zeromq.org): same socket patterns, compatible
wire protocol, and libzmq-style APIs. The core is memory-safe Rust and does
not depend on libzmq, libsodium, or a C compiler.

- Messaging patterns for pipelines, publish/subscribe, request/reply,
  routed services, exclusive peers, and raw streams.
- Transports for threads, processes, hosts, browsers, and compressed links:
  inproc, IPC, TCP, UDP, WebSocket, `lz4+tcp://`, `lz4+ws://`, and
  `zstd+tcp://`.
- Security for open, password-authenticated, and encrypted connections:
  NULL, PLAIN, and CURVE.
- Near-linear I/O scalability with OMQ-owned background threads on Linux,
  macOS, and Windows.
- No C compiler, no libzmq, no libsodium.
- Native bindings and compatibility APIs:
  - [C/C++](omq-libzmq/)
  - [Crystal](https://github.com/paddor/omq-binding.cr) and pure Crystal [OMQ.cr](https://github.com/paddor/omq.cr)
  - [BEAM: Erlang, Elixir, and Gleam](bindings/beam/)
  - [Go](bindings/go/)
  - [Java](bindings/java/)
  - [Lua](bindings/lua/)
  - [.NET](bindings/dotnet/)
  - [Node.js](bindings/node/)
  - [Python](bindings/pyomq/)
  - [Ruby](bindings/ruby/) and pure Ruby [OMQ.rb](https://github.com/zeromq/omq.rb)
  - [TypeScript](https://github.com/paddor/omq.ts) for browsers (ZWS transport only)

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

[Full comparison charts](COMPARISONS.md)

## The hard parts

OMQ is designed for real ZMQ behavior, not just happy-path PUSH/PULL throughput. You get:

- ZeroMQ semantics without extra tuning: no topology-specific socket types, no user-visible batching API, no manual reconnection loop.
- Transport failures are normal: reconnect, connect-before-bind, peer churn, and bind-side restarts are part of the design.
- Peer failures do not become user errors: `send()` and `recv()` keep working through disconnects, reconnects, slow consumers, and bind-side restarts.
- HWM back-pressure and routing fairness under load, not only in empty-queue examples.
- Documented libzmq compatibility edges for no-peer sends, linger, and HWM:
  [doc/libzmq/semantics.md](doc/libzmq/semantics.md).
- The hot paths are size-aware and latency-conscious: tiny messages stay inline without allocation, inproc passes messages by value, and large payloads use zero-copy buffers where it matters.
- Latency-sensitive single-peer TCP flows can use `omq_tokio::exclusive::Socket`
  to drive `PAIR`, `DEALER`, `ROUTER`, `REQ`, `REP`, `CLIENT`, or `SERVER`
  directly from the caller task.
- The only Rust ZeroMQ implementation following libzmq's architecture: application threads stay separate from dedicated background IO threads, IO work scales linearly across those threads, and PUB peers are assigned to IO lanes automatically.
- Memory-safe Rust for the public crates. `unsafe` is isolated and checked with Miri.
- Benchmarks cover the real shapes: CPU accounting, fan-in/fan-out, fairness, transport differences.

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

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `plain`           | PLAIN username/password auth (RFC 24)             | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `lz4`             | `lz4+tcp://` compression transport ([RFC](doc/lz4-rfc.md)) | `lz4rip` |
| `zstd`            | Experimental `zstd+tcp://` compression transport  | `zrip`                           |
| `ws`              | WebSocket (`ws://`) and secure WebSocket (`wss://`) transports | `rustls`, `rustls-native-certs` |

## Design highlights

| Feature | Details |
|---------|---------|
| **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)) | Byte-in / events-out; no async, no traits on the hot path. Mirrors `rustls::ConnectionCommon`. |
| **Message-count HWM** | `send_hwm`/`recv_hwm` count complete messages, not bytes. Send HWM is per outbound pipe/ring, so total native buffered messages can exceed one `send_hwm` when several pipes or transmit slots exist. |
| **Contiguous frame payloads** | `&msg[0]` gives `&[u8]` directly; no fallible borrow, no coalesce step. |
| **Zero-copy send and recv** | Send: large `Bytes` payloads reach the kernel `writev` without a single data copy. Recv: large frames read directly into a pre-allocated buffer, bypassing intermediate queues. |
| **Patricia-trie subscription matcher** | O(M) on topic length, not O(NxM). |
| **Compression dictionary auto-training** | LZ4 and zstd can train dictionaries from early messages and send them to each peer once. This helps compress small structured payloads without manual dictionary setup. |
| **Monitor events** | Socket-like `Stream` with owned `PeerInfo` on every connect / disconnect / handshake event. |

## Workspace

Five Cargo workspace crates plus language bindings.

| Crate | What it does | Unsafe policy |
|-------|--------------|---------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions | `#![forbid(unsafe_code)]` |
| [`omq-tokio`](omq-tokio/) | Multi-thread tokio backend (Linux/macOS/Windows) | `#![forbid(unsafe_code)]` |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq` dynamic/static library) | Unsafe C ABI boundary |
| [`yring`](yring/) | Bounded SPSC ring buffer with ypipe-style batched flush / prefetch | Unsafe ring core, Miri-tested |
| [`omq-bench`](omq-bench/) | Benchmark runner and SVG chart generator | Bench-only process control and CPU accounting |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-tokio, sync + asyncio) | PyO3 FFI boundary |
| [`OMQ.Net`](bindings/dotnet/) | .NET binding (managed wrapper over omq-libzmq) | P/Invoke/native ABI boundary |
| [`omq-rs`](bindings/ruby/) | Ruby binding (rb-sys over omq-tokio, scheduler-aware synchronous API) | Ruby C API/native extension boundary |
| [`OMQ.java`](bindings/java/) | Java 25 binding (JNI/FFM over omq-tokio, sync + async) | JNI/FFM boundary |
| [`OMQ.go`](bindings/go/) | Go 1.25 binding (cgo over omq-tokio, goroutine-safe API) | cgo/native ABI boundary |
| [`OMQ.node`](bindings/node/) | Node.js 24.11 binding (NAPI over omq-tokio, native addon) | NAPI/native addon boundary |
| [`OMQ.lua`](bindings/lua/) | Lua 5.4 binding (mlua native module over omq-libzmq) | mlua/native ABI boundary |
| [`OMQ.beam`](bindings/beam/) | Erlang binding plus Elixir and Gleam wrappers (Rustler NIF over omq-tokio) | BEAM NIF boundary |

## Testing

Every socket type, transport, mechanism, and feature combination is
covered by integration tests. The suite is layered:

- **700+ Rust tests** across socket types, transports, mechanisms, and
  libzmq-compatible C API behavior.
- **Feature-gated coverage** for PLAIN, CURVE, LZ4, and pyzmq/libzmq
  interop. WebSocket has dedicated tests and soak coverage.
- **Protocol fuzzing** (~1M iterations in the default opt-in run, with
  longer runs configurable): hand-rolled fuzz of the wire parser and the
  socket-action state machine.
- **20+ soak scenarios** across Rust and pyomq: peer churn, reconnect
  storms, PUB/SUB churn, ROUTER/DEALER churn, HWM reconnect, cancel
  safety, compression (lz4), PLAIN / CURVE auth, mechanism reconnect,
  large-message throughput, multi-socket, inproc cross-thread,
  WebSocket throughput and reconnect. Soak runs sample RSS and FD counts.
- **Loom** coverage for `yring` SPSC memory ordering, async wakeups, and
  `omq-tokio` signal race windows.
- **Miri** on `yring`.
- **Release semver review** through `release-plz`.

```sh
./scripts/test-all.sh              # standard sweep with local perf gate
OMQ_FUZZ=1 ./scripts/test-all.sh   # include fuzz suites
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
OMQ_SKIP_LUA=1 ./scripts/test-all.sh
OMQ_SKIP_PERF=1 ./scripts/test-all.sh
```

Soak tests are intentionally separate from the full sweep:

```sh
FEATURES="soak lz4 plain curve ws"
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
  --features "$FEATURES" --release --test omq_soak_peer_churn -- --nocapture
```

## Further reading

- [COMPARISONS.md](COMPARISONS.md): cross-implementation comparison charts.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): lz4+tcp
  throughput on bandwidth-limited links.
- [doc/architecture.md](doc/architecture.md): architecture and tokio
  backend internals.
- [doc/libzmq/semantics.md](doc/libzmq/semantics.md): exact compatibility
  notes for no-peer sends, linger, and HWM.
- [doc/lz4-rfc.md](doc/lz4-rfc.md): LZ4 compression transport wire
  format and dictionary shipping rules.

## Platform and requirements

- Rust 1.93 or newer (edition 2024).
- Linux, macOS, and Windows.
- Linux is the primary development and benchmarking platform.
- Supported 32-bit Linux targets: `i686-unknown-linux-gnu` and
  `armv7-unknown-linux-gnueabihf`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build, test, and benchmark commands.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.
