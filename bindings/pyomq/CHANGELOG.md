# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2026-07-04

### Changed

- *(deps)* Bump `omq-tokio` to 0.16.0, `omq-proto` to 0.20.0, and
  `yring` to 0.3.5.

## [0.13.0] - 2026-07-03

### Changed

- *(deps)* Bump `omq-tokio` to 0.15.0 and `omq-proto` to 0.19.0.

## [0.12.7] - 2026-06-27

### Added

- `Context.shadow()` method.

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.6.

## [0.12.6] - 2026-06-26

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.5 (lz4+ws transport, WS send bypass fix, Windows support).

## [0.12.5] - 2026-06-22

### Fixed

- `pyproject.toml` version was not bumped in 0.12.4, causing wheels to publish as 0.12.3.

## [0.12.4] - 2026-06-22

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.4, `lz4rip` to 0.8.

## [0.12.3] - 2026-06-17

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.3, `lz4rip` to 0.5.2.

## [0.12.2] - 2026-06-12

### Changed

- `compression_auto_train` option now defaults to `false`, matching `omq-proto` 0.17.1.
- *(deps)* Bump `omq-tokio` to 0.14.2, `omq-proto` to 0.17.1.

## [0.12.1] - 2026-06-12

### Removed

- `zstd` feature and `OMQ_COMPRESSION_LEVEL` constant. `lz4+tcp://` covers the compression use case.

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.1, `omq-proto` to 0.17.0.

## [0.12.0] - 2026-06-10

### Changed

- Port from `omq-compio` to `omq-tokio` backend. Jupyter notebook compatibility (tokio runtime coexists with running event loops).
- Set `arena_threshold` to 64 KiB (eliminates the 32 KiB message throughput cliff from gather-write refcount overhead on Python objects).
- *(deps)* Bump `omq-tokio` to 0.14.0, `omq-proto` to 0.16.0. Drop `omq-compio`, `blume`, `compio` dependencies.

## [0.11.0] - 2026-05-30

### Added

- Expose `on_mute`, `compression_level`, `compression_dict`, `compression_auto_train` options.

### Changed

- *(deps)* Bump `omq-compio` to 0.12.0, `omq-proto` to 0.15.0.

## [0.10.3] - 2026-05-25

### Fixed

- Cancel pump tasks before destroying socket.

## [0.10.2] - 2026-05-25

### Fixed

- `pyproject.toml` version was not bumped in 0.10.1.

## [0.10.1] - 2026-05-25

### Fixed

- Suppress `dead_code` warnings from PyO3 proc-macro call sites.

## [0.10.0] - 2026-05-25

### Changed

- *(deps)* Bump `omq-compio` to 0.11.0, `omq-proto` to 0.14.0, `blume` to 0.2.4, `yring` to 0.2.2.

## [0.9.0] - 2026-05-23

### Changed

- Async send bypasses the compio thread entirely — yring push runs
  inline on the Python thread, no future, no cross-thread hop.
- Async recv uses `loop.add_reader()` on the recv eventfd instead of
  routing through `compio_future_into_py`. Wakeup goes straight from
  epoll to a Python callback that pops the yring on the Python thread.
- Async throughput improved from ~9k msg/s to ~960k msg/s at 128 B
  (previously bottlenecked by ~100 µs per-message `call_soon_threadsafe`
  round-trip).
- Benchmark script (`scripts/update_perf.py`) now measures async
  throughput and latency (pyomq + pyzmq) and generates dual-axis SVG
  charts to `doc/charts/`.

### Added

- Native `_send_direct`, `_send_multipart_direct`, `_try_recv`,
  `_try_recv_multipart`, `_recv_fd` methods on `AsyncSocket` for the
  Python-side hot path.
- BLAKE3ZMQ authentication section in README.
- 7 new async tests: SNDMORE, RCVMORE, context manager, REQ/REP,
  unsubscribe, DEALER/ROUTER identity, close-while-pending.

## [0.8.1] - 2026-05-23

### Changed

- *(deps)* Bump `omq-compio` to 0.10.1, `yring` to 0.2.1.

## [0.8.0] - 2026-05-23

### Added

- CURVE client authentication via `socket.set_curve_auth()`: pass a list of
  allowed Z85 public keys (pure Rust check) or a Python callable receiving
  `PeerInfo` and returning bool. No ZAP protocol.
- `PeerInfo` class exposed to authenticator callbacks with a `public_key`
  property (40-byte Z85 bytes).

### Changed

- *(deps)* Bump `omq-compio` to 0.10.0.

## [0.7.1] - 2026-05-21

### Fixed

- Poller busy-wait fix from omq-compio 0.9.1.

### Changed

- *(deps)* Bump `omq-proto` to 0.12.0, `omq-compio` to 0.9.1.

## [0.7.0] - 2026-05-21

### Added

- CURVE encrypted sockets: `curve_server`, `curve_publickey`, `curve_secretkey`, `curve_serverkey` options wired through to the backend.
- PLAIN/CURVE socket tests and CURVE interop tests against pyzmq.
- CI smoke tests on built wheels before publishing.

### Removed

- `blake3zmq` feature from the published wheel (not yet wired up).

## [0.6.1] - 2026-05-21

### Fixed

- Fix example in README.
- Fix flaky CI port allocation with OS-assigned wildcard ports.

## [0.6.0] - 2026-05-20

### Added

- Socket attribute-style option access (`socket.linger = 0`, `socket.identity`, etc.).
- `Socket.poll(timeout, flags)` per-socket polling method.
- `Socket.set_hwm()` / `Socket.get_hwm()` / `hwm` property.
- `Socket.set_string()` / `Socket.get_string()` aliases.
- `Socket.send_serialized()` / `Socket.recv_serialized()` for custom serialization.
- `Socket.__repr__()` showing socket type.
- `Socket.underlying` property (returns self, pyzmq compat).
- `Context.closed` property.
- `Context.destroy(linger=None)` with socket tracking.
- `Poller.sockets` property.
- `zmq.select(rlist, wlist, xlist, timeout)` function.
- `zmq.zmq_version()`, `zmq.pyomq_version()`, `zmq.pyomq_version_info()`.
- `zmq.curve_keypair()` and `zmq.curve_public(secret)` (when curve feature compiled).
- `zmq.has()` now checks compiled features (curve, plain, lz4, zstd, blake3zmq).
- `ZMQVersionError` exception.
- 30+ missing pyzmq constants (ROUTING_ID, MECHANISM, PLAIN_*, SNDBUF, RCVBUF, device types, security mechanism IDs).
- `getsockopt` support for RECONNECT_IVL, RECONNECT_IVL_MAX, HEARTBEAT_IVL/TTL/TIMEOUT, HANDSHAKE_IVL, CONFLATE.
- No-op `setsockopt`/`getsockopt` for 15+ pyzmq compat constants.
- SNDBUF/RCVBUF wired through to socket options.
- PLAIN_SERVER/USERNAME/PASSWORD stored in overlay, wired to `MechanismConfig` when plain feature enabled.
- `socket_id()` on AsyncSocket (enables async Poller).
- Async parity: `asyncio.Poller`, all new Socket convenience methods, attribute access, `Context.closed`/`destroy()`.
- `CurveSecretKey::derive_public()` in omq-proto.
- `plain` Cargo feature forwarding to omq-compio.

### Removed

- Dead pyproject.toml extras (curve, blake3zmq, lz4, zstd, all). The published wheel includes all features.

### Fixed

- README install section: document that all features are built into the wheel.

## [0.5.0] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.8.0, `omq-proto` to 0.10.0.

## [0.4.2] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.7.0, `omq-proto` to 0.9.0.

## [0.4.1] - 2026-05-19

### Fixed

- Fix `pyproject.toml` version not matching `Cargo.toml` (maturin uses `pyproject.toml`).

## [0.4.0] - 2026-05-19

### Changed

- *(deps)* Bump `omq-compio` to 0.6.0, `omq-proto` to 0.9.0.

## [0.3.1] - 2026-05-18

### Fixed

- `destroy_socket`: wait for pump tasks to drain and call `sock.close()` before returning. Previously leaked driver tasks and buffers on every Context/Socket teardown cycle.

### Added

- Soak test suite (7 scenarios) covering PUSH/PULL throughput, reconnect storm, PUB/SUB churn, peer churn, REQ/REP cycles, context/socket creation churn, and large messages. Each monitors RSS for memory leaks.

## [0.3.0] - 2026-05-14

### Changed

- *(deps)* Bump `omq-compio` to 0.4.0 and `omq-proto` to 0.8.0.

## [0.2.4](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.3...pyomq-v0.2.4) - 2026-05-12

### Changed

- *(deps)* Bump `omq-compio` to 0.2.12 and `omq-proto` to 0.4.0. Update `Cargo.lock`.

## [0.2.3](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.2...pyomq-v0.2.3) - 2026-05-09

### Changed

- Commit `Cargo.lock` (omitted from 0.2.2). pyomq ships its own lockfile.

## [0.2.2](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.1...pyomq-v0.2.2) - 2026-05-09

### Changed

- *(deps)* bump `pyproject.toml` version to match `Cargo.toml`. The two were
  out of sync; maturin reads `pyproject.toml` as the authoritative version.

## [0.2.1](https://github.com/paddor/omq.rs/compare/pyomq-v0.2.0...pyomq-v0.2.1) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dep with `version = "0.19.0-rc.1"` and add
  `"time"` feature. Aligns with omq-compio's registry dep to avoid duplicate
  compio-runtime instances; `compio::time::sleep` was already used in the
  source but the feature was missing from Cargo.toml.

## [0.2.0](https://github.com/paddor/omq.rs/releases/tag/pyomq-v0.2.0) - 2026-05-04

### Added

- First PyPI release. Python binding for omq-compio (compio/io_uring backend).
  Linux x86_64 and aarch64 wheels; stable ABI covers Python 3.9+.
