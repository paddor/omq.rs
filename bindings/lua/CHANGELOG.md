# OMQ.lua Changelog

## [Unreleased]

## [0.3.2] - 2026-11-19

### Changed

- Build with zrip 0.8.18 (faster Zstd encoding and decoding) and lz4rip
  0.12.3 (same LZ4 output on every target).

### Fixed

- Bundle `omq-libzmq` 0.11.4 on `omq-tokio` 0.37.1. On Linux, an `shm://`
  sender closes a disconnected connection's lease table, eventfds, and peer
  pidfd once its leases complete. Before, every reconnect could leave four
  descriptors open.

## [0.3.1] - 2026-11-17

### Changed

- Receives return queue slots to a blocked sender in batches, at libzmq's
  low-water mark, so the sender wakes once per batch.

### Fixed

- Bundle `omq-libzmq` 0.11.3 on `omq-tokio` 0.37.0. An inproc connect to a
  bound endpoint, subscribe, unsubscribe, join and leave take effect
  before the publisher's next send, as in libzmq. Before, that send could
  miss them.
- A forked child can create a context and use it. On macOS it crashed
  (SIGTRAP) in its first blocking call if the parent had made one, and on
  every platform its first DNS lookup could hang.
- Build with rustls 0.23.46 (RUSTSEC-2026-0285, -0346, -0348 and -0349),
  salsabox 0.1.1, zrip 0.8.17 and lz4rip 0.12.2.

## [0.3.0] - 2026-11-14

### Added

- `max_message_parts` and `max_peer_subscriptions` socket options, with
  `set_*`/`get_*` methods. They limit the parts of one received message and
  the prefixes or groups one peer may hold; `-1` removes the limit.

### Changed

- Bundle `omq-libzmq` 0.11.0 on `omq-tokio` 0.36.0. Messages queued on a
  `connect()` route survive a reconnect, as in libzmq. A peer that sends
  more than 65,536 message parts is disconnected, and one peer holds at
  most 1,048,576 subscriptions or groups. An `ipc://` bind on a path with
  a live listener fails with `EADDRINUSE`.

## [0.2.16] - 2026-11-12

### Changed

- Bundle `omq-libzmq` 0.10.1 on `omq-tokio` 0.35.1. A connection driver
  with steady output pauses every 16 KiB, so other tasks and timers on its
  IO thread run on time.

## [0.2.15] - 2026-11-09

### Changed

- Bundle `omq-libzmq` 0.10.0 on `omq-tokio` 0.35.0. On Windows,
  socket creation fails when its wakeup events cannot be created,
  instead of returning a socket whose receives spin.

## [0.2.14] - 2026-11-04

### Changed

- Bundle `omq-tokio` 0.34.3. A silent peer is dropped once its announced
  PING TTL passes while a PING waits behind unsent output.

## [0.2.13] - 2026-11-03

### Changed

- Bundle `omq-libzmq` built on `omq-tokio` 0.34.2. Single-peer receives no
  longer allocate when a message arrives mid-receive.

## [0.2.12] - 2026-11-03

### Changed

- Bundle `omq-libzmq` 0.9.0. TCP REQ/REP round trips are faster.

## [0.2.11] - 2026-10-24

### Changed

- Bundle `omq-libzmq` 0.7.0.

## [0.2.10] - 2026-10-22

### Changed

- Bundle `omq-libzmq` 0.6.0. CURVE is faster and rejects low-order
  peer keys. RADIO over UDP sends large datagrams on macOS.

## [0.2.9] - 2026-10-22

### Changed

- Bundle `omq-libzmq` 0.5.27 with SHM send pool fixes. WSS connections no
  longer stall with the end of a burst buffered in TLS.

## [0.2.8] - 2026-10-20

### Changed

- Bundle `omq-libzmq` 0.5.26 with adaptive Zstd levels, `ZMQ_TCP_MAXRT`
  support, handle type checks, and atomic multipart sends across `EAGAIN`.

## [0.2.7] - 2026-10-16

### Changed

- Bundle `omq-libzmq` 0.5.25 with STREAM routing fixes and configurable PEER
  queue byte limits.

## [0.2.6] - 2026-10-15

### Changed

- Bundle `omq-libzmq` 0.5.23 with LZ4/Zstd arena compression for batches of
  small messages. Compressed peers require the updated transport revisions.


## [0.2.5] - 2026-10-14

### Changed

- Bundle `omq-libzmq` 0.5.22, including Linux shared-memory endpoints.

## [0.2.4] - 2026-10-07

### Changed

- Bundle `omq-libzmq` 0.5.21 with receive fairness, request/reply routing,
  and ZMTP compliance fixes.

## [0.2.3] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.2] - 2026-09-04

- Bundle `omq-libzmq` 0.5.16 with responsive peer control handling and the
  latest receive-notifier fixes.

## [0.2.1] - 2026-08-23

- Preserve `SERVER` routing IDs across native receive and send calls.
- Allow `inproc://` sends across OMQ runtime threads.
- Bound exchange retries and preserve soak deadlines during shutdown.
- Handle LZ4 compressor epoch rollover without corrupting the stream.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.2.0] - 2026-08-18

- First LuaRocks release of OMQ.lua.
