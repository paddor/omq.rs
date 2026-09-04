# OMQ.go Changelog

## [Unreleased]

## [0.1.2] - 2026-09-04

- Pass the transport peer address to PLAIN authentication callbacks.
- Reject CURVE public keys that do not match the configured secret key.
- Bundle `omq-tokio` 0.22.0 and `omq-proto` 0.27.0.

## [0.1.1] - 2026-08-23

- Block scalar receives in native code instead of polling from Go.
- Reduce scalar request/reply latency.
- Preserve receives that complete at the timeout boundary.
- Preserve `SERVER` routing IDs across native receive and send calls.
- Bound shutdown and churn operations under backpressure.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.1.0] - 2026-08-16

- First release of the Go binding for OMQ.rs.
