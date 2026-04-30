# ØMQ.rs

Pure Rust ZeroMQ implementation. Wire-compatible with libzmq. All 11
standard socket types plus 8 draft types, TCP / IPC / inproc / UDP
transports, NULL / CURVE / blake3zmq mechanisms, `lz4+tcp://` and
`zstd+tcp://` compression transports.

## Install

```sh
cargo add omq                                 # compio backend (default)
cargo add omq --no-default-features --features tokio-backend
```

```rust
use omq::{Endpoint, Message, Options, Socket, SocketType};

let push = Socket::new(SocketType::Push, Options::default());
push.connect("tcp://127.0.0.1:5555".parse()?).await?;
push.send(Message::single("hi")).await?;
```

`omq` is a thin facade. The default `compio-backend` feature pulls in
[`omq-compio`](omq-compio/) (single-thread io_uring/IOCP); the
`tokio-backend` feature swaps in [`omq-tokio`](omq-tokio/) (multi-thread
tokio + mio). The two are mutually exclusive — pick one at build time.
The public `Socket` API is identical, verified in lockstep by the
`coverage_matrix` and `interop_compio` test suites.

## Design highlights

- **Per-socket HWM with work-stealing send pumps** on round-robin patterns
  (PUSH / DEALER / REQ / PAIR / CLIENT / CHANNEL / SCATTER); per-connection
  queues on fan-out (PUB / XPUB / RADIO) and identity-routed patterns
  (ROUTER / REP / SERVER / PEER).
- **Optional strict per-pipe priority** (`priority` Cargo feature) on
  `Socket::connect_with(endpoint, ConnectOpts { priority })` - nanomsg-
  style 1..=255 (lower = higher priority). Round-robin send always
  prefers the highest-priority alive peer; lower tiers only run when
  higher are blocked or disconnected.
- **Sans-I/O ZMTP codec** ([`omq-proto`](omq-proto/)): byte-in / events-
  out state machine, no async, no traits on the hot path.
- **Multi-chunk frame payloads** (`Payload = SmallVec<[Bytes; 2]>`,
  `Message = SmallVec<[Payload; 3]>`): layers prepend static prefixes
  without copying, kernel stitches chunks via `writev` / `sendmsg`.
- **Monitor** as a socket-like `Stream` with owned `PeerInfo` context on
  every event.
- **Python binding** ([`bindings/pyomq`](bindings/pyomq/)): PyO3 wrapper
  over `omq-compio` with a sync API and an `asyncio`-compatible bridge.

## Platform support

Linux first. `omq-compio` uses io_uring on Linux, kqueue on macOS.
`omq-tokio` uses mio / epoll / kqueue.

## Requirements

- Rust 1.85 or newer (edition 2024).

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature           | what it adds                                      | extra deps                       |
|-------------------|---------------------------------------------------|----------------------------------|
| `compio-backend`  | (default) compio io_uring/IOCP backend            | -                                |
| `tokio-backend`   | tokio multi-thread backend                        | -                                |
| `curve`           | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `blake3zmq`       | OMQ-native BLAKE3 + ChaCha20 mechanism†           | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4`             | `lz4+tcp://` compression transport                | `lz4-sys`                       |
| `zstd`            | `zstd+tcp://` compression transport               | `zstd-safe` (vends `libzstd`; needs `cc`) |
| `priority`        | Strict per-pipe priority on `Socket::connect_with`| -                                |

† **BLAKE3ZMQ has not been independently security audited.** It's an
omq-native construction (Noise XX + BLAKE3 + X25519 + ChaCha20-BLAKE3)
and should not be relied on for anything that matters until it has had
third-party review. Use **CURVE** (RFC 26) for production / regulated
workloads. Audits welcome - open an issue if you can help fund or
conduct one.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for throughput / latency / compression
tables across transports, sizes, and backends.

## License

ISC.
