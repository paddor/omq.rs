# OMQ.Net Changelog

## [Unreleased]

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
