# OMQ.beam Changelog

## [Unreleased]

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
