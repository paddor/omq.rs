# Changelog

## [Unreleased]

## [0.3.4] - 2026-11-25

### Changed

- Bundle `omq-tokio` 0.39.1. Receive rings, send pipes, and fan-out lanes
  start at 1024 slots and grow toward the HWM, so idle peers stay small.
  Adaptive `zstd+tcp://` picks better levels for senders that wait on
  replies.
- Build with zrip 0.8.27 and lz4rip 0.12.5. Zstd levels 3 and 4 write
  0.2-3.9% smaller output on inputs over 256 KiB, and level -7 compresses
  better but encodes slower. A Zstd dictionary whose repeat offsets exceed
  its content is rejected, as C zstd does. LZ4 with a dictionary compresses
  129-256 B messages 7-16% smaller and delivers about 11% more 256 B-16 KiB
  messages; without one, 256 KiB messages drop about 7%.

### Fixed

- ROUTER and PEER admit peers whose identity starts with a zero byte, as
  libzmq does. Only the 9-byte generated form stays reserved.
- A socket that stays readable no longer starves other sockets on its
  runtime worker. Under steady input, an `shm://` connection lets other
  connections on its IO thread run.

## [0.3.3] - 2026-11-21

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
- Build with rustls 0.23.46 (RUSTSEC-2026-0285, -0346, -0348 and -0349),
  salsabox 0.1.1, zrip 0.8.17 and lz4rip 0.12.2.

## [0.3.0] - 2026-11-14

### Added

- `max_message_parts` limits the parts of one received message (default
  1024). `max_peer_subscriptions` limits the prefixes or groups one peer may
  hold on a PUB, XPUB or RADIO socket (default 65536). A negative value
  removes a limit.

### Changed

- Bundle `omq-tokio` 0.36.0. Messages queued on a `connect()` route
  survive a reconnect, as in libzmq. A peer that sends more than 1024
  message parts is disconnected, and one peer holds at most 65,536
  subscriptions or groups. An `ipc://` bind on a path with a live
  listener fails.

## [0.2.15] - 2026-11-12

### Changed

- Bundle `omq-tokio` 0.35.1. A connection driver with steady output
  pauses every 16 KiB, so other tasks and timers on its IO thread run on
  time.

## [0.2.14] - 2026-11-09

### Changed

- Bundle `omq-tokio` 0.35.0.

## [0.2.13] - 2026-11-04

### Changed

- Bundle `omq-tokio` 0.34.3. A silent peer is dropped once its announced
  PING TTL passes while a PING waits behind unsent output.
- Require `yring` 0.3.21 and `fanring` 0.3.10.

## [0.2.12] - 2026-11-03

### Changed

- Bundle `omq-tokio` 0.34.2. Single-peer receives no longer allocate when a
  message arrives mid-receive.

## [0.2.11] - 2026-11-03

### Changed

- Bundle `omq-tokio` 0.34.0. TCP REQ/REP round trips are faster.

## [0.2.10] - 2026-10-24

### Changed

- `compression_level` accepts -7..=4. zrip dropped level -8.
- Bundle `omq-tokio` 0.32.0. Monitors report subscribe and cancel
  messages from ZMTP 3.0 peers.

## [0.2.9] - 2026-10-22

### Changed

- Bundle `omq-tokio` 0.31.0. CURVE is faster and rejects low-order
  peer keys. RADIO over UDP sends large datagrams on macOS.

## [0.2.8] - 2026-10-22

### Changed

- Bundle `omq-tokio` 0.30.1 with SHM send pool fixes. WSS connections no
  longer stall with the end of a burst buffered in TLS.

## [0.2.7] - 2026-10-20

### Changed

- Bundle `omq-tokio` 0.30.0 and `omq-proto` 0.34.0 with adaptive Zstd
  levels, handshake flood limits, and wakeup and lifecycle fixes.

### Fixed

- Socket construction no longer releases the GVL under the materialization
  lock, and close serializes with wrapper setup.

## [0.2.6] - 2026-10-16

### Changed

- Require `omq-tokio` 0.29.0 and `omq-proto` 0.33.0 with the latest PEER
  limits and fan-out send readiness fixes.

## [0.2.5] - 2026-10-15

### Fixed

- Admit REQ/REP messages when Ruby receives them, so prefetched requests
  cannot redirect replies or advance request/reply state.
- Capture REP reply destinations at application send and preserve them
  across backpressure retries.
- Require `omq-tokio` 0.28.1 and `omq-proto` 0.32.1 to reject unrelated
  REQ replies and REP requests without a body frame.

## [0.2.4] - 2026-10-15

### Fixed

- Make the packaged native manifest independent of the development workspace
  so installation works outside the source checkout.
- Check the packaged manifest in CI and require an isolated gem installation
  and messaging smoke test before publication.

### Changed

- Require `omq-proto` 0.32.0 and `omq-tokio` 0.28.0 with LZ4/Zstd arenas.

## [0.2.3] - 2026-10-14

### Changed

- Bundle `omq-tokio` 0.26.0 and `omq-proto` 0.30.0, including Linux
  shared-memory endpoints.

## [0.2.2] - 2026-10-07

- Bundle `omq-tokio` 0.25.0 and `omq-proto` 0.29.0 with bounded receive
  queues, ZMTP compliance fixes, and reconnect improvements.

### Changed

- Name authentication worker threads so lifecycle checks track owned workers.
- Require `omq-proto` 0.29.0, `omq-tokio` 0.25.0, and published
  `yring` 0.3.20.
  Workspace checks now exercise the current core through local patches.

### Fixed

- Subscribe to monitor events before socket materialization returns. Arm monitor
  waits before polling and retry stale wakes within the receive deadline.

## [0.2.1] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.0] - 2026-09-04

- Add PLAIN username/password allowlists and authentication callbacks through
  `plain_auth:` and `Socket#set_plain_auth`.
- Expand authenticator peer metadata with transport address and PLAIN
  credentials.
- Require explicit PLAIN server policy; bare PLAIN server mode fails closed.
- Reject CURVE public keys that do not match the configured secret key.
- Require `omq-proto` 0.27.0 and `omq-tokio` 0.22.0.

## [0.1.2] - 2026-09-01

- Route `Socket#try_recv` directly through the native nonblocking receive path.
- Avoid Ruby-side batch array churn in hot polling loops.

## [0.1.1] - 2026-08-23

- Document the full public API and publish the RubyDoc documentation link.
- Reject unknown socket options instead of silently ignoring misspellings.
- Make peer, subscriber, and monitor waits stop cleanly when a socket closes.
- Release the CURVE auth-worker mutex before joining its thread during close.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.
- Require `omq-proto` 0.26.1 and `omq-tokio` 0.21.4.

## [0.1.0] - 2026-08-20

- Add standalone `omq-rs` Ruby binding with all 20 OMQ socket types.
- Add PLAIN and CURVE sockets, Z85 key generation, CURVE client authentication,
  and pyzmq CURVE interoperability.
- Add LZ4/zstd transports and compression options.
- Add socket lifecycle monitoring with Fiber-aware waits.
- Add receive wake and monitor-fd hooks for backend adapters.
- Support Ractor-owned sockets and cross-Ractor inproc/TCP messaging on Ruby 4.
- Support synchronous sockets on TruffleRuby and test them in CI.
