# Changelog

## [Unreleased]

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
