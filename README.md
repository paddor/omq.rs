# ØMQ.rs

Pure Rust ZeroMQ implementation. Wire-compatible with libzmq. All 11
standard socket types plus 7 draft types, TCP / IPC / inproc / UDP
transports, NULL / CURVE / blake3zmq mechanisms, `lz4+tcp://` and
`zstd+tcp://` compression transports.

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
- **Sans-I/O ZMTP codec** (internal `proto` module): byte-in / events-out
  state machine, no async, no tokio, no traits on the hot path.
- **Multi-chunk frame payloads** (`Payload = SmallVec<[Bytes; 2]>`,
  `Message = SmallVec<[Payload; 3]>`): layers prepend static prefixes
  without copying, kernel stitches chunks via `writev` / `sendmsg`. Sized
  to the realistic protocol shapes (1- or 2-chunk frames, 1- to 3-part
  envelopes inline; everything else heap-allocates).
- **Two interchangeable backends** sharing one `omq-proto` core:
  `omq-tokio` (multi-thread tokio, mio / epoll) and `omq-compio` (single-
  thread compio with io_uring on Linux). Identical public API.
- **Monitor** as a socket-like `Stream` with owned `PeerInfo` context on
  every event.
- **Python binding** (`bindings/pyomq`): PyO3 wrapper over `omq-compio`
  with a sync API and an `asyncio`-compatible bridge.

## Platform support

Whatever `omq-compio` and `omq-tokio` support, but definitely Linux first.
`omq-compio` uses io_uring on Linux, kqueue on macOS. `omq-tokio` uses mio /
epoll / kqueue.

## Requirements

- Rust 1.85 or newer (edition 2024).

## Cargo features

All optional. Default build is the smallest deploy: NULL mechanism +
TCP / IPC / inproc / UDP, no C compiler required. Enable any of:

| feature      | what it adds                                      | extra deps                       |
|--------------|---------------------------------------------------|----------------------------------|
| `curve`      | CURVE encrypted-handshake mechanism (RFC 26)      | `crypto_box`, `crypto_secretbox` |
| `blake3zmq`  | OMQ-native BLAKE3 + ChaCha20 mechanism            | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4`        | `lz4+tcp://` compression transport                | `lz4-sys`                       |
| `zstd`       | `zstd+tcp://` compression transport               | `zstd-safe` (vends `libzstd`; needs `cc`) |
| `priority`   | Strict per-pipe priority on `Socket::connect_with`| -                                |

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for throughput / latency / compression
tables across transports, sizes, and backends.

## License

ISC.
