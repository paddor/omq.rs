# OMQ.dart Changelog

## [Unreleased]

## [0.1.5] - 2026-11-25

### Changed

- Pin `omq-libzmq` 0.13.2 on `omq-tokio` 0.39.2.

### Fixed

- PUB, XPUB, and RADIO dropped for a peer once 512 KiB was queued for it,
  so a subscriber that kept up lost most of a burst of 64 KiB messages.
  They now drop only at the send HWM, as in libzmq, so a stalled subscriber
  can hold up to that many messages.
- A send on an inproc connection could stay blocked when the peer opened
  its receive queue just as the send began to wait.

## [0.1.4] - 2026-11-25

### Changed

- Pin `omq-libzmq` 0.13.1 on `omq-tokio` 0.39.1. Queues start at 1024 slots
  and grow toward the HWM, so idle sockets stay small. Received frames from
  128 KiB through 8 MiB skip one copy. Adaptive `zstd+tcp://` picks better
  levels for senders that wait on replies.
- The prebuilt libraries build with zrip 0.8.27 and lz4rip 0.12.5. Zstd
  levels 3 and 4 write 0.2-3.9% smaller output on inputs over 256 KiB, and
  level -7 compresses better but encodes slower. A Zstd dictionary whose
  repeat offsets exceed its content is rejected, as C zstd does. LZ4 with a
  dictionary compresses 129-256 B messages 7-16% smaller and delivers about
  11% more 256 B-16 KiB messages; without one, 256 KiB messages drop about
  7%.

### Fixed

- A send or receive HWM of 0 means no limit, as in libzmq.
- Routing IDs may start with a zero byte, as in libzmq. Only the 9-byte form
  OMQ generates stays reserved.
- `tcp://` endpoints accept unbracketed IPv6 hosts, such as
  `tcp://::1:5555`, like libzmq. The port follows the last colon.
- Under steady input, an `shm://` connection lets other connections on its
  IO thread run.

## [0.1.3] - 2026-11-21

### Changed

- The prebuilt libraries build with zrip 0.8.24. Zstd level -7 encodes
  4-25% faster, decoding is faster, and 64-256 B dictionary frames are
  12-24% smaller.

### Fixed

- Pin `omq-libzmq` 0.12.0 on `omq-tokio` 0.38.0. Zstd compression with a
  dictionary no longer panics or writes a frame the peer cannot decode.

## [0.1.2] - 2026-11-19

### Changed

- The prebuilt libraries build with zrip 0.8.18 (faster Zstd encoding and
  decoding) and lz4rip 0.12.3 (same LZ4 output on every target).

### Fixed

- Pin `omq-libzmq` 0.11.4 on `omq-tokio` 0.37.1. On Linux, an `shm://`
  sender closes a disconnected connection's lease table, eventfds, and peer
  pidfd once its leases complete. Before, every reconnect could leave four
  descriptors open.

## [0.1.1] - 2026-11-17

### Fixed

- Pin `omq-libzmq` 0.11.3 on `omq-tokio` 0.37.0. An inproc connect to a
  bound endpoint, subscribe, unsubscribe, join and leave take effect
  before the publisher's next send, as in libzmq. Before, that send could
  miss them.
- The prebuilt libraries build with rustls 0.23.46 (RUSTSEC-2026-0285,
  -0346, -0348 and -0349), salsabox 0.1.1, zrip 0.8.17 and lz4rip 0.12.2.

## [0.1.0] - 2026-11-16

### Added

- Dart package `omq` over the `omq-libzmq` C ABI: contexts, every socket
  type except DGRAM, options, multipart and routed messages, PLAIN and
  CURVE, monitors, and shared contexts.
- `try*`, `*Sync`, and `Future` forms of send and receive, plus a
  `messages` stream. Async waits use `omq_socket_set_ready_callback`;
  receives use `omq_recv_into`.
- The build hook downloads the prebuilt `libomq_zmq` of a pinned
  omq-libzmq release and checks its SHA-256. The `library` and `system`
  user-defines bundle another build or use the system loader.
