# OMQ.zig Changelog

## [Unreleased]

## [0.1.6] - 2026-10-20

### Changed

- Bundle `omq-libzmq` 0.5.26 with adaptive Zstd levels, `ZMQ_TCP_MAXRT`
  support, handle type checks, and atomic multipart sends across `EAGAIN`.

## [0.1.5] - 2026-10-16

### Changed

- Bundle `omq-libzmq` 0.5.25 with STREAM routing fixes and configurable PEER
  queue byte limits.

## [0.1.4] - 2026-10-15

### Changed

- Bundle `omq-libzmq` 0.5.23 with LZ4/Zstd arena compression for batches of
  small messages. Compressed peers require the updated transport revisions.


## [0.1.3] - 2026-10-14

### Changed

- Bundle `omq-libzmq` 0.5.22, including Linux shared-memory endpoints.

## [0.1.2] - 2026-10-07

### Changed

- Bundle `omq-libzmq` 0.5.21 with receive fairness, request/reply routing,
  and ZMTP compliance fixes.

## [0.1.1] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.1.0] - 2026-09-04

- First release of the Zig 0.16 binding backed by `omq-libzmq`.
- Provide all 20 OMQ socket types, multipart and routed messages, socket
  options, polling, monitoring, proxies, context sharing, and CURVE helpers.
- Support fixed PLAIN server credentials and fail-closed standard ZAP
  semantics.
- Cover API parity, interoperability, soak workloads, and comparison
  benchmarks.
