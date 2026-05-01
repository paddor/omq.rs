# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/paddor/omq.rs/releases/tag/omq-tokio-v0.1.0) - 2026-05-01

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- *(clippy)* collapse nested if-let chains (collapsible_if, new in 1.93)
- *(curve)* wire-compatible with libzmq + pyzmq interop suite
- XPUB honors legacy ZMTP 3.0 0x01-prefix message subscribes

### Other

- cargo fmt --all (first-time sweep)
- parity fixes, new tests, and pyomq monitor/connections API
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- CURVE + BLAKE3ZMQ across the strategy buckets
- full Stage/Phase narrative sweep across all src/
- drop Stage/Phase narrative from hot files
- integrate Ruby OMQ wire-interop test
- socket-type x transport coverage + cross-runtime interop
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding
