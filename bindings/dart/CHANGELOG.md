# OMQ.dart Changelog

## [Unreleased]

### Added

- Dart package `omq` over the `omq-libzmq` C ABI: contexts, every socket
  type except DGRAM, options, multipart and routed messages, PLAIN and
  CURVE, monitors, and shared contexts.
- `try*`, `*Sync`, and `Future` forms of send and receive, plus a
  `messages` stream. Async waits use `omq_socket_set_ready_callback`;
  receives use `omq_recv_into`.
