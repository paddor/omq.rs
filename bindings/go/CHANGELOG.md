# OMQ.go Changelog

## [Unreleased]

## [0.1.1] - 2026-08-23

- Block scalar receives in native code instead of polling from Go.
- Reduce scalar request/reply latency.
- Preserve receives that complete at the timeout boundary.
- Preserve `SERVER` routing IDs across native receive and send calls.
- Bound shutdown and churn operations under backpressure.
- Add mixed transport, protocol, lifecycle, and resource soak coverage.

## [0.1.0] - 2026-08-16

- First release of the Go binding for OMQ.rs.
