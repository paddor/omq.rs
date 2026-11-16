# OMQ.dart Changelog

## [Unreleased]

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
