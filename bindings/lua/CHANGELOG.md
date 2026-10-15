# OMQ.lua Changelog

## [Unreleased]

## [0.2.7] - 2026-10-16

### Changed

- Bundle `omq-libzmq` 0.5.25 with STREAM routing fixes and configurable PEER
  queue byte limits.

## [0.2.6] - 2026-10-15

### Changed

- Bundle `omq-libzmq` 0.5.23 with LZ4/Zstd arena compression for batches of
  small messages. Compressed peers require the updated transport revisions.


## [0.2.5] - 2026-10-14

### Changed

- Bundle `omq-libzmq` 0.5.22, including Linux shared-memory endpoints.

## [0.2.4] - 2026-10-07

### Changed

- Bundle `omq-libzmq` 0.5.21 with receive fairness, request/reply routing,
  and ZMTP compliance fixes.

## [0.2.3] - 2026-09-19

### Changed

- Bundle the current OMQ core release.

## [0.2.2] - 2026-09-04

- Bundle `omq-libzmq` 0.5.16 with responsive peer control handling and the
  latest receive-notifier fixes.

## [0.2.1] - 2026-08-23

- Preserve `SERVER` routing IDs across native receive and send calls.
- Allow `inproc://` sends across OMQ runtime threads.
- Bound exchange retries and preserve soak deadlines during shutdown.
- Handle LZ4 compressor epoch rollover without corrupting the stream.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.2.0] - 2026-08-18

- First LuaRocks release of OMQ.lua.
