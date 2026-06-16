# ØMQ.rs

> **~3x** libzmq TCP throughput | **2x** lower TCP latency

Pure Rust [ZeroMQ](https://zeromq.org): brokerless message passing for distributed and concurrent applications. Wire-compatible with libzmq, faster across all message sizes.

- Two async backends: **tokio** (default, Linux/macOS) and **compio** (io_uring, Linux)
- 20 socket types (11 standard + 9 draft), 8 transports (TCP, IPC, inproc, UDP, WS, WSS, `lz4+tcp://`)
- 3 security mechanisms: PLAIN, CURVE, BLAKE3ZMQ
- No C compiler, no vendored C, no libzmq, no libsodium
- Python binding ([pyomq](bindings/pyomq/)), C API ([omq-libzmq](omq-libzmq/))

### vs libzmq and other implementations

[How to beat libzmq](doc/performance.md) | [Full comparison charts](COMPARISONS.md)

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/main_tcp.svg" alt="PUSH/PULL throughput: TCP, all implementations" width="950">
</p>

<details>
<summary>omq backends: compio, tokio, tokio-mt</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/omq_tcp.svg" alt="PUSH/PULL throughput: TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/reqrep/omq_tcp.svg" alt="REQ/REP latency: TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/omq_ipc.svg" alt="PUSH/PULL throughput: IPC" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/omq_inproc.svg" alt="PUSH/PULL throughput: inproc" width="850">
</p>
</details>

<details>
<summary>Fan-out and fan-in</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanout_tcp.svg" alt="PUSH fan-out: TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/fanin_tcp.svg" alt="PUSH fan-in: TCP" width="850">
</p>
</details>

<details>
<summary>PUB/SUB throughput</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/omq_tcp.svg" alt="PUB/SUB throughput: TCP" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/omq_ipc.svg" alt="PUB/SUB throughput: IPC" width="850">
</p>
</details>

<details>
<summary>Compression: lz4+tcp://</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/lz4_tcp.svg" alt="PUB/SUB lz4+tcp fan-out: projected throughput at link speed" width="850">
</p>
</details>

<details>
<summary>Mechanisms: PLAIN / CURVE / BLAKE3ZMQ</summary>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/mechanism/tokio.svg" alt="Mechanisms: omq-tokio" width="850">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/mechanism/compio.svg" alt="Mechanisms: omq-compio" width="850">
</p>
</details>

## Install

> [!CAUTION]
> **Experimental.** The API is unstable and may change without notice. Not yet battle-tested in production. Bug reports and testing in real workloads are very welcome.

```sh
cargo add omq-tokio               # default: multi-thread tokio (Linux/macOS)
```

Two backends with identical `Socket` APIs, verified by `coverage_matrix` + `interop_compio` test suites:

- [`omq-tokio`](omq-tokio/): multi-thread tokio + mio (Linux/macOS)
- [`omq-compio`](omq-compio/): single-thread io_uring/IOCP (Linux; not yet on crates.io)

If you know ZeroMQ, you know OMQ. Same socket types, same connect/bind/send/recv:

```rust
use omq_tokio::{Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hello")).await?;

let pull = Socket::new(SocketType::Pull, Options::default());
pull.bind("tcp://127.0.0.1:5555".parse()?).await?;
let msg = pull.recv().await?;
assert_eq!(&msg[0], b"hello");
```

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `tokio-backend`   | (default) tokio multi-thread backend              | -                                |
| `compio-backend`  | compio io_uring/IOCP backend                      | -                                |
| `plain`           | PLAIN username/password auth (RFC 24)             | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `blake3zmq`       | OMQ-native BLAKE3 + ChaCha20 mechanism ([RFC](https://github.com/paddor/omq-blake3zmq/blob/main/RFC.md)) | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4`             | `lz4+tcp://` compression transport ([RFC](https://github.com/paddor/omq-lz4/blob/main/RFC.md)) | `lz4rip` |
| `ws`              | WebSocket (`ws://`) and secure WebSocket (`wss://`) transports | `rustls`, `rustls-native-certs` |

> [!WARNING]
> **BLAKE3ZMQ has not been independently security audited.** It's an
> omq-native construction (Noise XX + BLAKE3 + X25519 + ChaCha20-BLAKE3)
> and should not be relied on for anything that matters until it has had
> third-party review. Use **CURVE** (RFC 26) for production / regulated
> workloads. Audits welcome - open an issue if you can help fund or
> conduct one.

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

Seven crates, one repo.

| Crate | What it does |
|-------|-------------|
| [`omq-proto`](omq-proto/) | Sans-I/O ZMTP 3.x core: codec, messages, mechanisms, subscriptions |
| [`omq-tokio`](omq-tokio/) | Multi-thread tokio backend (Linux/macOS) |
| [`omq-compio`](omq-compio/) | Single-thread io_uring / IOCP backend (Linux) |
| [`omq-libzmq`](omq-libzmq/) | libzmq-compatible C interface (`libomq_zmq.so` drop-in) |
| [`blume`](blume/) | Batching MPSC channel with swap-drain consumer |
| [`yring`](yring/) | Bounded SPSC ring buffer with ypipe-style batched flush / prefetch |
| [`pyomq`](bindings/pyomq/) | Python binding (PyO3 over omq-tokio, sync + asyncio) |

## Testing

Every socket type, transport, mechanism, and feature combination is
covered by integration tests on both backends. The full suite:

- **750+ integration tests** across omq-compio and omq-tokio (every
  socket-type x transport x mechanism cell).
- **Protocol fuzzing** (~10M iterations per suite): hand-rolled fuzz of
  the wire parser and the socket-action state machine.
- **18 soak test scenarios** per backend: peer churn, reconnect storms,
  PUB/SUB churn, compression, PLAIN / CURVE / BLAKE3ZMQ auth
  large-message throughput, multi-socket. Each scenario samples
  RSS and file-descriptor counts to detect leaks.
- **Cross-runtime interop**: omq-compio <-> omq-tokio over TCP.
- **Wire interop** with libzmq (C), pyzmq, and
  [Pure Ruby OMQ](https://github.com/zeromq/omq.rb).

```sh
./scripts/test-all.sh          # full sweep, both backends
OMQ_FUZZ=1 ./scripts/test-all.sh   # include fuzz suites
```

## Further reading

- [COMPARISONS.md](COMPARISONS.md): cross-implementation comparison
  charts (libzmq, zmq.rs, rzmq) across all transports.
- [BENCHMARKS.md](BENCHMARKS.md): throughput / latency tables across
  message patterns, transports, message sizes, and backends.
- [BENCHMARKS_COMPRESSION.md](BENCHMARKS_COMPRESSION.md): lz4+tcp
  throughput on bandwidth-limited links with structured JSON payloads.
- [doc/architecture.md](doc/architecture.md): three-layer split, two-queue
  socket model, backend comparison.
- [doc/compio.md](doc/compio.md): compio backend internals.
- [doc/tokio.md](doc/tokio.md): tokio backend internals.
- [doc/performance.md](doc/performance.md): how omq beat libzmq.

## Platform and requirements

Linux and macOS (and likely other mio targets). `omq-tokio` uses mio /
epoll / kqueue. `omq-compio` uses io_uring (Linux 6.0+) and is not
available on macOS.

- Rust 1.93 or newer (edition 2024).
- `omq-compio`: Linux 6.0 or newer (io_uring multi-shot recv with
  provided buffers).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and [DEVELOPMENT.md](DEVELOPMENT.md) for build, test, and benchmark commands.

## AI disclosure

This project was built with significant LLM assistance throughout: architecture, implementation, tests, benchmark infrastructure, and docs. It's an experiment in what LLM-assisted development can and can't do. The design decisions and direction are mine.

## License

ISC.
