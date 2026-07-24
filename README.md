# ØMQ.rs

Pure Rust [ZeroMQ](https://zeromq.org): brokerless message passing for distributed and concurrent applications. Socket-level messaging patterns that work the same way in-process, between processes, and over the network.

- Tokio backend for Linux, macOS, and Windows
- 20 socket types: stable ZMQ patterns plus draft CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, CHANNEL/PEER, and STREAM
- 9 transports: TCP, IPC, inproc, UDP, WS, WSS, `lz4+tcp://`, `lz4+ws://`, and `lz4+wss://`
- 3 security mechanisms: NULL, PLAIN, CURVE
- No C compiler, no libzmq, no libsodium
- Python binding ([pyomq](bindings/pyomq/)), C API ([omq-libzmq](omq-libzmq/))

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
</details>

[Full comparison charts](COMPARISONS.md)

## The hard parts

OMQ is designed for real ZMQ behavior, not just happy-path PUSH/PULL throughput. You get:

- ZeroMQ semantics without extra tuning: no topology-specific socket types, no user-visible batching API, no manual reconnection loop.
- Transport failures are normal: reconnect, connect-before-bind, peer churn, and bind-side restarts are part of the design.
- Peer failures do not become user errors: `send()` and `recv()` keep working through disconnects, reconnects, slow consumers, and bind-side restarts.
- HWM back-pressure and routing fairness under load, not only in empty-queue examples.
- The hot paths are size-aware and latency-conscious: tiny messages stay inline without allocation, inproc passes messages by value, and large payloads use zero-copy buffers where it matters.
- The only Rust ZeroMQ implementation following libzmq's architecture: application threads stay separate from dedicated background IO threads, IO work scales linearly across those threads, and PUB peers are assigned to IO lanes automatically.
- Memory-safe Rust for the public crates. `unsafe` is isolated and checked with Miri.
- Benchmarks cover the real shapes: CPU accounting, fan-in/fan-out, fairness, transport differences.

## Usage

> [!NOTE]
> The API is still evolving and may change between minor versions. Bug reports and testing in real workloads are welcome.

The Rust backend is [`omq-tokio`](omq-tokio/): tokio + mio on Linux,
macOS, and Windows. It can run socket IO on OMQ-owned runtime threads or
inside an existing tokio runtime.

| API | Runtime placement | Scaling model |
|-----|-------------------|---------------|
| `Context::new().socket(...)` | Async socket, OMQ-owned IO threads | Linear IO-lane scaling; PUB peers are sharded across lanes |
| `Context::current().socket(...)` | Async socket, caller's active tokio runtime | Tokio scheduler/work stealing; PUB fan-out stays on one OMQ lane |
| `Context::new().blocking_socket(...)` | Sync socket, OMQ-owned IO threads | Linear IO-lane scaling; caller thread stays out of IO |

Supported 32-bit Linux targets are `i686-unknown-linux-gnu` and
`armv7-unknown-linux-gnueabihf`. They require native 64-bit atomics. ZMTP wire
length fields stay 64-bit, but practical frame/message size is bounded by
platform allocation limits (below 4 GiB on 32-bit).

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

More examples in [examples/zguide-tokio/](examples/zguide-tokio/), a
port of the ZeroMQ Guide patterns to OMQ.

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `plain`           | PLAIN username/password auth (RFC 24)             | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `lz4`             | `lz4+tcp://` compression transport ([RFC](doc/lz4-rfc.md)) | `lz4rip` |
| `ws`              | WebSocket (`ws://`) and secure WebSocket (`wss://`) transports | `rustls`, `rustls-native-certs` |

## Design highlights

| Feature | Details |
|---------|---------|
| **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)) | Byte-in / events-out; no async, no traits on the hot path. Mirrors `rustls::ConnectionCommon`. |
| **Per-socket HWM** | Work-stealing send pumps on round-robin patterns; per-connection queues on fan-out and identity-routed patterns. |
| **Contiguous frame payloads** | `&msg[0]` gives `&[u8]` directly; no fallible borrow, no coalesce step. |
| **Zero-copy send and recv** | Send: large `Bytes` payloads reach the kernel `writev` without a single data copy. Recv: large frames read directly into a pre-allocated buffer, bypassing intermediate queues. |
| **Patricia-trie subscription matcher** | O(M) on topic length, not O(NxM). |
| **LZ4 dictionary auto-training** | Off by default. When enabled, trains from first 100 messages, ships to peer once; drops effective compression threshold from 512 B to 64 B. |
| **Monitor events** | Socket-like `Stream` with owned `PeerInfo` on every connect / disconnect / handshake event. |

## Workspace

Five Cargo workspace crates plus the Python binding.

| Crate | What it does | Unsafe policy |
|-------|--------------|---------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions | `#![forbid(unsafe_code)]` |
| [`omq-tokio`](omq-tokio/) | Multi-thread tokio backend (Linux/macOS/Windows) | `#![forbid(unsafe_code)]` |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq` dynamic/static library) | Unsafe C ABI boundary |
| [`yring`](yring/) | Bounded SPSC ring buffer with ypipe-style batched flush / prefetch | Unsafe ring core, Miri-tested |
| [`omq-bench`](omq-bench/) | Benchmark runner and SVG chart generator | Bench-only process control and CPU accounting |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-tokio, sync + asyncio) | PyO3 FFI boundary |

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
- **Loom** coverage for lock-free inproc queue behavior.
- **Miri** on `yring`.
- **Release semver review** through `release-plz`.

```sh
./scripts/test-all.sh              # standard sweep with local perf gate
OMQ_FUZZ=1 ./scripts/test-all.sh   # include fuzz suites
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
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
- [doc/lz4-rfc.md](doc/lz4-rfc.md): LZ4 compression transport wire
  format and dictionary shipping rules.

## Platform and requirements

**Linux is the primary development and benchmarking platform.** CI
required checks cover Linux x86_64, Linux ARM64, macOS Intel, macOS
ARM64, and Windows. macOS jobs run the Rust tests serially because
socket/timer timing is more sensitive on hosted runners.

**macOS** is covered in CI for both Intel and ARM64 runners.
`omq-tokio` uses mio / kqueue. `omq-libzmq` uses a pipe-backed
notification fd for `zmq_poll`/`ZMQ_FD` readiness.

**Windows** is covered in CI. `omq-tokio` supports TCP, IPC named
pipes, inproc, UDP, and WebSocket transports. `omq-libzmq` builds and
tests on Windows for the supported C API surface.

`pyomq` publishes Linux, macOS and Windows wheels and an sdist

Requirements:

- Rust 1.93 or newer (edition 2024).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build, test, and benchmark commands.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.
