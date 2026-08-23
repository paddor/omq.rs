# Changelog

## [Unreleased]

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
