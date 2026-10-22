# OMQ.node Changelog

## [Unreleased]

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

- Bundle `omq-tokio` 0.30.0 and `omq-proto` 0.34.0 with handshake flood
  limits and wakeup and lifecycle fixes.

## [0.2.6] - 2026-10-16

### Changed

- Bundle `omq-tokio` 0.29.0 with configurable PEER queue limits and
  corrected fan-out send readiness.

## [0.2.5] - 2026-10-15

### Changed

- Bundle `omq-tokio` 0.28.0 with LZ4 arena compression for batches of small
  messages. LZ4 peers require the updated transport revision.


## [0.2.4] - 2026-10-14

### Changed

- Bundle `omq-tokio` 0.26.0, including Linux shared-memory endpoints.

## [0.2.3] - 2026-10-07

### Changed

- Bundle `omq-tokio` 0.25.0 and `omq-proto` 0.29.0 with bounded receive
  queues, ZMTP compliance fixes, and reconnect improvements.

## [0.2.2] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.1] - 2026-09-04

- Keep generated platform package dependencies out of the checked-in root
  manifest and lockfile so release jobs can run `npm ci` before packaging.

## [0.2.0] - 2026-09-04

- Preserve SERVER routing IDs across receive and send operations.
- Reject the ineffective `lz4: true` option. LZ4 is selected by the endpoint
  scheme; the `lz4` option only configures a dictionary.
- Reject CURVE public keys that do not match the configured secret key.
- Bundle `omq-tokio` 0.22.0 and `omq-proto` 0.27.0.

## [0.1.3] - 2026-08-23

- Run asynchronous socket I/O on the shared Tokio runtime without blocking
  Node.js worker threads.
- Restore async send and receive throughput after the runtime migration.
- Use Linux abstract namespace endpoints for internal IPC.
- Add `sendGroup()` for RADIO messages.
- Bound shutdown paths under blocked sends and receives.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.1.2] - 2026-08-16

- Publish multi-platform npm packages with native addons.
