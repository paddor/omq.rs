# OMQ.beam Changelog

## [Unreleased]

## [0.4.0] - 2026-11-21

### Added

- `ws://`, `wss://`, and `quic://` endpoints. The NIF builds the `ws`,
  `quic`, and `webtransport` features by default, and `has/1` reports them.
- Options with omq-libzmq IDs: `omq_ws_allowed_origins` lists the browser
  origins WebSocket listeners admit, `wss_*` set WSS certificates and
  trust, and `omq_quic_*` set QUIC certificates, trust, browser origins,
  and the WebTransport path. Browsers connect through OMQ.ts
  (`@paddor/omq`).

### Changed

- Build with zrip 0.8.24. Zstd level -7 encodes 4-25% faster, decoding is
  faster, and 64-256 B dictionary frames are 12-24% smaller.

### Fixed

- Bundle `omq-tokio` 0.38.0. Zstd compression with a dictionary no longer
  panics or writes a frame the peer cannot decode.

## [0.3.2] - 2026-11-19

### Changed

- Build with zrip 0.8.18 (faster Zstd encoding and decoding) and lz4rip
  0.12.3 (same LZ4 output on every target).

### Fixed

- Bundle `omq-tokio` 0.37.1. On Linux, an `shm://` sender closes a
  disconnected connection's lease table, eventfds, and peer pidfd once its
  leases complete. Before, every reconnect could leave four descriptors
  open.

## [0.3.1] - 2026-11-17

### Fixed

- Bundle `omq-tokio` 0.37.0. An inproc connect to a bound endpoint,
  subscribe, unsubscribe, join and leave take effect before the
  publisher's next send, as in libzmq. Before, that send could miss them.
- Build with salsabox 0.1.1, zrip 0.8.17 and lz4rip 0.12.2.

## [0.3.0] - 2026-11-14

### Added

- `omq_max_message_parts` (1101) and `omq_max_peer_subscriptions` (1102)
  socket options, with Elixir and Gleam constants. -1 means no limit.

### Changed

- Bundle `omq-tokio` 0.36.0. Messages queued on a `connect()` route
  survive a reconnect, as in libzmq. A peer that sends more than 1024
  message parts is disconnected, and one peer holds at most 65,536
  subscriptions or groups. An `ipc://` bind on a path with a live
  listener fails.

## [0.2.14] - 2026-11-12

### Changed

- Bundle `omq-tokio` 0.35.1. A connection driver with steady output
  pauses every 16 KiB, so other tasks and timers on its IO thread run on
  time.

## [0.2.13] - 2026-11-09

### Changed

- Bundle `omq-tokio` 0.35.0.

## [0.2.12] - 2026-11-04

### Changed

- Bundle `omq-tokio` 0.34.3. A silent peer is dropped once its announced
  PING TTL passes while a PING waits behind unsent output.

## [0.2.11] - 2026-11-03

### Changed

- Bundle `omq-tokio` 0.34.2. Single-peer receives no longer allocate when a
  message arrives mid-receive.

## [0.2.10] - 2026-11-03

### Changed

- Bundle `omq-tokio` 0.34.0. TCP REQ/REP round trips are faster.

## [0.2.9] - 2026-10-24

### Changed

- `omq_compression_level` accepts -7..=4. zrip dropped level -8.
- Bundle `omq-tokio` 0.32.0. Monitors report subscribe and cancel
  messages from ZMTP 3.0 peers.

## [0.2.8] - 2026-10-22

### Changed

- Bundle `omq-tokio` 0.31.0. CURVE is faster and rejects low-order
  peer keys. RADIO over UDP sends large datagrams on macOS.

## [0.2.7] - 2026-10-22

### Changed

- Bundle `omq-tokio` 0.30.1 with SHM send pool fixes. WSS connections no
  longer stall with the end of a burst buffered in TLS.

## [0.2.6] - 2026-10-20

### Changed

- Bundle `omq-tokio` 0.30.0 and `omq-proto` 0.34.0 with adaptive Zstd
  levels, handshake flood limits, and wakeup and lifecycle fixes.

### Fixed

- Incomplete mechanism options are rejected, and credential setup
  serializes with socket materialization.

## [0.2.5] - 2026-10-16

### Changed

- Bundle `omq-tokio` 0.29.0 and `omq-proto` 0.33.0 with configurable PEER
  queue limits and corrected fan-out send readiness.

## [0.2.4] - 2026-10-15

### Changed

- Bundle `omq-tokio` 0.28.0 and `omq-proto` 0.32.0 with LZ4/Zstd arena
  compression for batches of small messages. Compressed peers require the
  updated transport revisions.


## [0.2.3] - 2026-10-14

### Changed

- Bundle `omq-tokio` 0.26.0 and `omq-proto` 0.30.0.

### Fixed

- Timed sends, monitor receives, and `wait_any` park until woken instead of
  polling every 1-10 ms.

## [0.2.2] - 2026-10-07

- Bundle `omq-tokio` 0.25.0 and `omq-proto` 0.29.0 with bounded receive
  queues, ZMTP compliance fixes, and reconnect improvements.

- Use the current backend's 10-second setup deadline and reconnect behavior.
- Keep initial DNS errors visible; retry later resolution failures internally.

## [0.2.1] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.0] - 2026-09-04

- Add fixed PLAIN server credentials to the Erlang, Elixir, and Gleam APIs.
- Require explicit PLAIN server policy; bare PLAIN server mode fails closed.
- Reject CURVE public keys that do not match the configured secret key.
- Expand generated Erlang API documentation.
- Bundle `omq-tokio` 0.22.0.

## [0.1.0] - 2026-09-01

- First Hex releases of the `omq`, `omq_elixir`, and `omq_gleam` packages.
- Provide all 20 OMQ socket types, PLAIN and CURVE security, compression,
  monitoring, polling, proxies, and context sharing across wrappers.
