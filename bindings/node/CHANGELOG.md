# OMQ.node Changelog

## [Unreleased]

- Preserve SERVER routing IDs across receive and send operations.
- Reject the ineffective `lz4: true` option. LZ4 is selected by the endpoint
  scheme; the `lz4` option only configures a dictionary.

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
