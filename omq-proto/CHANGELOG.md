# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/paddor/omq.rs/releases/tag/omq-proto-v0.1.0) - 2026-05-01

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- *(clippy)* use is_multiple_of in z85 encode/decode guards
- *(curve)* wire-compatible with libzmq + pyzmq interop suite

### Other

- cargo fmt --all (first-time sweep)
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- full Stage/Phase narrative sweep across all src/
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding
