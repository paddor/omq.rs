# OMQ.java Changelog

## [Unreleased]

## [0.3.4]

- Pass the transport peer address to PLAIN and CURVE authentication callbacks.
- Reject CURVE public keys that do not match the configured secret key.
- Bundle `omq-tokio` 0.22.0 and `omq-proto` 0.27.0.

## [0.3.3]

- Unblock concurrent socket close when a PUSH or SCATTER sender is waiting for
  space in the native send ring.
- Wake blocked receivers during close without racing native ring teardown.
- Restore scalar receive throughput while preserving close synchronization.
- Preserve `SERVER` routing IDs across native receive and send calls.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.
- Bundle `omq-tokio` 0.21.4 and `omq-proto` 0.26.1.

## [0.3.2]

- Re-run the OMQ.java release through a Maven Central workflow that publishes
  automatically after validation.

## [0.3.1]

- Use timed PUSH/PULL throughput samples for the OMQ.java performance chart
  instead of fixed message-count runs.
- Fix receive ring close races for both virtual-thread and platform-thread
  receive paths.
- Use ISC license metadata in Maven and native Cargo manifests.
- Bundle native backend updates from `omq-tokio` 0.21.2 and related crates.

## [0.3.0]

- Publish OMQ.java as a Java-only main jar plus platform classifier runtime jars.
- Build and test release native libraries on CI before Maven Central upload.
- Report the required Maven classifier when native loading fails.
- Tighten Java soak resource checks with FD tracking, RSS/FD slope gates, and
  post-GC heap cleanup checks.

## [0.2.0]

- Remove public batch send/receive APIs. Scalar receive methods still use the hidden native batch receive path.
- Add `SocketOptions` for reusable pre-I/O option sets.
- Add `Context.socket(SocketType, SocketOptions)`.
- Add explicit JPMS module descriptor: `io.omq`.
- Use native async receive for blocking receive calls made from Java virtual threads when cached ring data is empty.

## [0.1.1]

- First Maven Central release of OMQ.java.
- Provide Java 25 bindings backed by native `omq-tokio`.
- Include Linux x86_64, macOS x86_64, macOS aarch64, and Windows x86_64 native libraries in the jar.
