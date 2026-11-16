# OMQ.Net Changelog

## [Unreleased]

## [0.3.1] - 2026-11-17

### Changed

- `ReceiveAsync` and `Monitor.ReceiveAsync` wait on `omq-libzmq`'s ready
  callback instead of 100 ms `zmq_poll` slices on thread-pool threads. A
  waiting receive holds no thread and no socket lock, so idle receives no
  longer delay others, and `Dispose` fails a pending receive at once.
- Receives return queue slots to a blocked sender in batches, at libzmq's
  low-water mark, so the sender wakes once per batch.

### Fixed

- Bundle `omq-libzmq` 0.11.3 on `omq-tokio` 0.37.0. An inproc connect to a
  bound endpoint, subscribe, unsubscribe, join and leave take effect
  before the publisher's next send, as in libzmq. Before, that send could
  miss them.
- Build with rustls 0.23.46 (RUSTSEC-2026-0285, -0346, -0348 and -0349),
  salsabox 0.1.1, zrip 0.8.17 and lz4rip 0.12.2.

## [0.3.0] - 2026-11-14

### Added

- `SocketOption.MaxMessageParts` and `SocketOption.MaxPeerSubscriptions`
  limit the parts of one received message and the prefixes or groups one
  peer may hold; `-1` removes the limit.

### Changed

- `Poller.Wait` and `Poller.WaitAsync` blocked when the context shuts down
  throw `OmqException` with `ETERM`. Before, they reported the socket as
  readable.
- Bundle `omq-libzmq` 0.11.0 on `omq-tokio` 0.36.0. Messages queued on a
  `connect()` route survive a reconnect, as in libzmq. A peer that sends
  more than 65,536 message parts is disconnected, and one peer holds at
  most 1,048,576 subscriptions or groups. An `ipc://` bind on a path with
  a live listener fails with `EADDRINUSE`.

## [0.2.14] - 2026-11-12

### Changed

- Bundle `omq-libzmq` 0.10.1 on `omq-tokio` 0.35.1. A connection driver
  with steady output pauses every 16 KiB, so other tasks and timers on its
  IO thread run on time.

## [0.2.13] - 2026-11-09

### Changed

- Bundle `omq-libzmq` 0.10.0 on `omq-tokio` 0.35.0. On Windows,
  socket creation fails when its wakeup events cannot be created,
  instead of returning a socket whose receives spin.

## [0.2.12] - 2026-11-04

### Changed

- Bundle `omq-tokio` 0.34.3. A silent peer is dropped once its announced
  PING TTL passes while a PING waits behind unsent output.

## [0.2.11] - 2026-11-03

### Changed

- Bundle `omq-libzmq` built on `omq-tokio` 0.34.2. Single-peer receives no
  longer allocate when a message arrives mid-receive.

## [0.2.10] - 2026-11-03

### Changed

- Bundle `omq-libzmq` 0.9.0. TCP REQ/REP round trips are faster.

## [0.2.9] - 2026-10-24

### Changed

- Bundle `omq-libzmq` 0.7.0.

## [0.2.8] - 2026-10-22

### Changed

- Bundle `omq-libzmq` 0.6.0. CURVE is faster and rejects low-order
  peer keys. RADIO over UDP sends large datagrams on macOS.

## [0.2.7] - 2026-10-22

### Changed

- Bundle `omq-libzmq` 0.5.27 with SHM send pool fixes. WSS connections no
  longer stall with the end of a burst buffered in TLS.

## [0.2.6] - 2026-10-20

### Changed

- Bundle `omq-libzmq` 0.5.26 with adaptive Zstd levels, `ZMQ_TCP_MAXRT`
  support, handle type checks, and atomic multipart sends across `EAGAIN`.

### Fixed

- `Monitor.ReceiveAsync` throws `OperationCanceledException` when `Dispose`
  stops the monitor mid-receive, instead of `OmqClosedException` from the
  closed reader.

## [0.2.5] - 2026-10-16

### Changed

- Bundle `omq-libzmq` 0.5.25 with STREAM routing fixes and configurable PEER
  queue byte limits.

## [0.2.4] - 2026-10-15

### Changed

- Bundle `omq-libzmq` 0.5.23 with LZ4/Zstd arena compression for batches of
  small messages. Compressed peers require the updated transport revisions.


## [0.2.3] - 2026-10-14

### Changed

- Bundle `omq-libzmq` 0.5.22 and avoid an extra managed copy on sends.

### Fixed

- `SendAsync(ReadOnlyMemory<byte>)` on a full queue parks in the native async
  send instead of spinning on a poller that always reports writable.

## [0.2.2] - 2026-10-07

### Changed

- Bundle `omq-libzmq` 0.5.21 with receive fairness, request/reply routing,
  and ZMTP compliance fixes.

## [0.2.1] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.0] - 2026-09-04

### Added

- Fixed username/password policy for PLAIN servers through
  `ConfigurePlainServer`.

### Fixed

- Preserve SERVER routing IDs in asynchronous message sends.
- Make zero-duration asynchronous poll waits perform a nonblocking poll.
- Return the copied length when receiving a frame larger than the supplied
  `ReceiveInto` buffer.

### Changed

- Make `ReceiveInto` write directly into the caller's span and return the
  number of bytes copied, removing temporary payload allocations and copies.
- Require credentials when configuring a PLAIN server instead of enabling an
  allow-all mode.
- Bundle `omq-libzmq` 0.5.16.

## [0.1.3]

### Fixed

- Serialize managed socket migration and native handle leases across async
  operations and disposal.
- Return resolved endpoints from wildcard `bind()` calls.
- Preserve multipart messages in mixed protocol soak exchanges.
- Bound cancellation, churn, and shutdown paths under backpressure.

### Changed

- Add mixed transport, protocol, lifecycle, and resource soak coverage.
- Bundle `omq-libzmq` 0.5.15.

## [0.1.2]

### Added

- XML documentation for the public API, included in the NuGet package for
  IntelliSense.
- Contributor, test, benchmark, and packaging instructions in
  `DEVELOPMENT.md`.

### Changed

- Trim the README to installation, API, and performance context.
- Set the next package version to `0.1.2`.

## [0.1.1]

### Added

- First NuGet release of OMQ.Net.
- `net8.0` and `net10.0` managed assemblies with Linux, macOS, and Windows
  x64 native assets.
- Synchronous and cancellation-aware asynchronous socket operations.
- All OMQ socket types, multipart messages, polling, monitors, proxies,
  CURVE key helpers, PLAIN/CURVE configuration, and lifecycle tests.
