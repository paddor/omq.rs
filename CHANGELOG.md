## Changelog

All notable changes to omq.rs will be documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

Initial pre-release. Three-crate Rust ZeroMQ implementation, wire-compatible
with libzmq. compio is the primary backend; omq-tokio is the cross-runtime
alternative.

### Crates

- **omq-proto**: sans-I/O ZMTP 3.1 codec, message types, mechanism state
  machines, message-transform layer, routing, options builder. No async,
  no I/O, no traits on the hot path.
- **omq-compio**: io_uring socket driver. Single-threaded runtime; spawn
  multiple runtimes for multi-core workloads.
- **omq-tokio**: multi-thread tokio backend. Identical public Socket API.

### Bindings

- **pyomq** (`bindings/pyomq`): PyO3 wrapper over `omq-compio`. Sync API
  plus an `asyncio`-compatible bridge. Built out-of-tree via maturin
  (excluded from `cargo build --workspace`).

### Socket API

- `Socket::new`, `bind`, `connect`, `unbind`, `disconnect`, `send`, `recv`,
  `try_send`, `try_recv`. Identical signatures across both backends.
  `try_send` / `try_recv` are synchronous and non-blocking: they return
  `Err(Error::WouldBlock)` immediately rather than suspending the task.
  `Error::WouldBlock` is the new variant in `omq-proto::Error`.
- `Socket::connect_with(endpoint, ConnectOpts)` (gated `priority` feature)
  for strict per-pipe priority on round-robin patterns.
- `Socket::join` / `Socket::leave` for DISH (RFC 48).
- `Socket::monitor()`: socket-like `Stream` with owned `PeerInfo` context
  on every event.
- `Endpoint` enum with `Display` / `FromStr` round-trip.

### Socket types

Standard (RFC 28 + RFC 47): PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP,
DEALER, ROUTER, PAIR. Group/transport drafts: RADIO, DISH (RFC 48). Draft
RFC stubs: CLIENT/SERVER (RFC 41), SCATTER/GATHER (RFC 49), CHANNEL
(RFC 51), PEER, RAW.

### Transports

- `tcp://` (IPv4 + IPv6).
- `ipc://` (Unix domain sockets, filesystem or abstract namespace).
- `inproc://` (process-local lock-free channel; process-wide registry).
- `udp://` (RADIO/DISH, RFC 48 datagram framing).
- `lz4+tcp://` (gated `lz4`, optional pre-trained dictionary).
- `zstd+tcp://` (gated `zstd`, optional static or auto-trained dict).

### Mechanisms

- **NULL** (default): plaintext, no handshake-time auth.
- **CURVE** (gated `curve`, RFC 26): Curve25519 box per data frame.
- **BLAKE3ZMQ** (gated `blake3zmq`): omq-native AEAD; X25519 + BLAKE3 +
  ChaCha20 + BLAKE3 MAC.

### Cargo features

All opt-in. Default build is the smallest deploy: NULL + TCP/IPC/inproc/UDP,
no C compiler required. Features: `curve`, `blake3zmq`, `lz4`, `zstd`,
`priority`. See `README.md` for the table.

### Options

Typed builder over `Options`. `ReconnectPolicy::default()` is `Fixed(100ms)`
matching libzmq's `ZMQ_RECONNECT_IVL`; `Exponential { min, max }` is opt-in.
`OnMute` controls send-HWM behaviour: `Block` (default), `DropNewest`,
`DropOldest`. Defaults differ from libzmq in two places: per-socket HWM
semantics, conflate restricted to FanOut patterns.

### Performance

See [BENCHMARKS.md](BENCHMARKS.md).

### Conventions

- Rust 2024 edition, MSRV 1.87.
- Workspace lints: rust `missing_debug_implementations` denied; clippy
  `pedantic` warned with noisy rules silenced.
- ASCII-only source.
- `Cargo.lock` deliberately untracked (this is a library workspace).
