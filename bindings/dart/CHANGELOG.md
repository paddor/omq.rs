# OMQ.dart Changelog

## [Unreleased]

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
