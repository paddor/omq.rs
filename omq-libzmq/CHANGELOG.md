# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.10] - 2026-07-31

### Added

- Enable `zstd+tcp://` transport through the C API.

### Changed

- *(deps)* Bump `omq-tokio` to 0.20.3 and `yring` to 0.3.11.

## [0.5.9] - 2026-07-31

### Changed

- *(deps)* Bump `omq-tokio` to 0.20.2.

## [0.5.7] - 2026-07-27

### Fixed

- Bound no-peer `PUSH` sends now match libzmq: blocking sends wait for a
  route, while nonblocking or zero-timeout sends return `EAGAIN` instead of
  entering a native pre-ready connect pipe.
- `ZMQ_IMMEDIATE=1` now mutes connected no-peer round-robin sends until a peer
  completes handshaking, matching libzmq.
- `ZMQ_RECONNECT_IVL` now defaults to 100 ms, matching libzmq. The previous
  disabled default could remove connect-side pre-ready pipes after an initial
  `ECONNREFUSED`.

### Changed

- *(deps)* Bump `omq-tokio` to 0.20.0.

## [0.5.6] - 2026-07-23

### Fixed

- Context shutdown and socket close paths wake waiters through stateful
  signals instead of raw notifications. This closes related lost-wakeup races
  from issue #186.
- `LocalCell` now binds app-facing socket access to the first calling thread
  and panics before cross-thread mutation.
- `ZMQ_LINGER` now reports libzmq-compatible default linger forever while
  explicit `ZMQ_LINGER=0` still maps to zero-linger native close.
- XPUB/XSUB libzmq tests now use finite linger and nonblocking negative
  receives, avoiding Windows hangs.

### Changed

- *(deps)* Bump `omq-tokio` to 0.19.3 and `yring` to 0.3.10.

## [0.5.5] - 2026-07-22

### Added

- `ZMQ_MSG_T_SIZE` support through `zmq_ctx_get`.

### Changed

- `zmq_msg_t` storage now matches libzmq's 64-byte, pointer-aligned ABI on
  both 64-bit and 32-bit targets.
- Inproc bypass cursors now use 64-bit atomics so 32-bit targets do not alias
  at the 4 GiB cursor boundary.
- `zmq_proxy` and `zmq_proxy_steerable` now forward complete messages through
  libzmq-compatible queues, with capture, control, and backpressure handling.

### Fixed

- Context shutdown and inproc close races now wake waiters reliably.
- Socket close honors linger before final driver shutdown.

## [0.5.4] - 2026-07-19

### Fixed

- Duplicate endpoint connects now follow libzmq-compatible behavior through
  `omq-tokio` 0.19.1.

### Changed

- *(deps)* Bump `omq-tokio` to 0.19.1.

## [0.5.3] - 2026-07-19

### Added

- Lazy and zero-thread context support.

### Fixed

- Bounded inproc backpressure in the C API layer.
- REP broker envelopes through byte-stream ROUTER/DEALER proxies preserve
  all routing frames via `omq-tokio` 0.19.0.

### Changed

- Deny `unsafe_op_in_unsafe_fn` lint. All unsafe function bodies now
  require explicit `unsafe` blocks.
- Port to `Context` API. Force throughput workload profile to avoid
  REP latency recv sink.
- *(deps)* Bump `omq-tokio` to 0.19.0, `yring` to 0.3.8.

## [0.5.1] - 2026-07-10

### Fixed

- Skip `malloc(0)` for zero-length frames: `malloc(0)` is implementation-defined and may return NULL. Guard `malloc` + `memcpy` behind `sz > 0` in `zmq_msg_recv` and `zmq_msg_copy`.
- Crash on `recv_drain` mutex poison instead of silent hang: `stash_remaining_parts` silently swallowed a poisoned mutex, causing every subsequent `zmq_recv` to re-enter the drain path and hang.

### Performance

- Remove unconditional `thread::yield_now()` on send success. The old code yielded every 64 messages, causing extreme run-to-run variance (CV 50-76%). Spin-yield on queue-full reduced from 64 to 8 iterations with `spin_loop` hints for the first 4.
- Zero-alloc `zmq_recv` fast path: borrow first frame via `msg.get()` instead of `part_bytes()` which allocated on every message.
- `zmq_msg_recv` uses single `malloc` (`KIND_HEAP`) instead of `Bytes::copy_from_slice` + `Box::new` (2 allocations) for frames up to 128 bytes.

### Changed

- *(deps)* Bump `omq-tokio` to 0.17.0, `yring` to 0.3.6.

## [0.5.0] - 2026-07-04

### Added

- IPC support on Windows (named pipes).

### Fixed

- Harden message property setters, socket option edge cases, and C API boundary checks.
- Timeout and DNS connect edge case hardening.

### Changed

- *(deps)* Bump `omq-tokio` to 0.16.0, `yring` to 0.3.5.

## [0.4.10] - 2026-07-03

### Fixed

- `zmq_msg_send` clones borrowed message data instead of stealing it.
- Clamp proxy control-frame slices to the received byte count.
- `zmq_sleep` rejects negative seconds.
- Fan-out send errors propagate correctly through the C API.

### Changed

- *(deps)* Bump `omq-tokio` to 0.15.0 and `yring` to 0.3.4.

## [0.4.9] - 2026-06-27

### Fixed

- Null dangling pointer and size in `zmq_msg_send` after `Box` reclaim.
- `PollWaiter` `ready_count` to count items, not event types.
- Return `EINVAL` for null/short `optval` in `zmq_setsockopt`.

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.6, `bytes` 1.11 to 1.12.

## [0.4.8] - 2026-06-26

### Added

- Windows support: platform-specific notification module, `PollWaiter` implementation (contributed by @MatthieuDartiailh).

### Fixed

- Inproc bypass: move sender unpark from `peek()` to `advance()`.

### Changed

- `LocalCell<T>` pattern for `OmqSocket` fields (`send_accum`, `bypass_send`, `bypass_recv`, `recv_cons`, `send_yield`), eliminating ~15 inline unsafe blocks.
- Remove `Mutex` from `PollWaiter` hot path.
- *(deps)* Bump `omq-tokio` to 0.14.5, `yring` to 0.3.2.

## [0.4.7] - 2026-06-22

### Fixed

- Heap buffer overflow in inproc bypass `WRAP_SENTINEL` write.
- `write_string` now returns `EINVAL` when the caller buffer is too small instead of silently truncating.
- `send_fd` eventfd initialized with 0 instead of `DEFAULT_HWM`.

### Changed

- Remove unsound `unsafe impl Sync for OmqMsgRepr`.
- Narrow `parse_endpoint_args`/`parse_group_args` lifetime from `'static` to `'a`.
- Use `Duration::from_secs` for `ZMQ_HANDSHAKE_IVL`.
- *(deps)* Bump `omq-tokio` to 0.14.4.

## [0.4.6] - 2026-06-17

### Added

- Complete `zmq_setsockopt`/`zmq_getsockopt` coverage: all 124 libzmq option constants defined. Unknown options now return `EINVAL` instead of silently succeeding.
- `ZMQ_IPV4ONLY` get/set support (inverse of `ZMQ_IPV6`).
- `ZMQ_BLOCKY`, `ZMQ_STREAM_NOTIFY` getsockopt stubs.

### Changed

- Deduplicate blocking recv logic (`block_recv` helper) and stale bypass cleanup (`clear_stale_bypass`).
- *(deps)* Bump `omq-tokio` to 0.14.3, `yring` to 0.3.1.

## [0.4.5] - 2026-06-12

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.2.

## [0.4.4] - 2026-06-12

### Removed

- `zstd` feature: `zstd+tcp://` transport removed (follows `omq-tokio`).

### Changed

- *(deps)* Bump `omq-tokio` to 0.14.1. Tighten `tokio` to 1.52.

## [0.4.3] - 2026-06-10

### Changed

- Port from `omq-compio` to `omq-tokio` backend.
- Eliminate recv-pump relay: `RecvSink::Yring` for single-peer TCP/IPC. Inproc byte ring eliminates per-message heap allocation. TCP throughput stabilized at 5.5M msg/s (was 0.1-5M).
- Yield every 64 messages or 1 MiB sent to prevent starving the tokio worker.
- `send_accum` `Mutex` replaced with `UnsafeCell`, `send_ring` `RwLock` replaced with `AtomicBool` guard.
- Reduce `worker_threads` from 2 to 1.
- *(deps)* Bump `omq-tokio` to 0.14.0.

### Added

- `ZMQ_XPUB_NODROP` option.

### Fixed

- Inproc bypass recv hang on multipart messages.
- SPSC and recv-yring fast paths recover after peer churn.
- Harden FFI layer against panics: `lock_overlay!` macro, `OmqContext::new` returns `Option`, `run_on` returns `Result`.
- `zmq_poll`: reject negative `nitems`.
- Use `ptr::read/write_unaligned` for FFI int access.
- Stacked Borrows UB in inproc bypass: `Box<[UnsafeCell<u8>]>` for `RingBuf.buf`.

## [0.4.2] - 2026-05-30

### Changed

- *(deps)* Bump `omq-compio` to 0.12.0. Tighten `rustc-hash` to 2.1.0.

## [0.4.1] - 2026-05-25

### Changed

- *(deps)* Bump `omq-compio` to 0.11.0.

## [0.4.0] - 2026-05-25

### Fixed

- `drain_eventfds` on non-Linux: loop until pipe is empty.
- Set `O_NONBLOCK` on pipe fds in non-Linux `NotifyFd`.

### Changed

- Use `FxHashMap` for internal maps.
- *(deps)* Bump `omq-compio` to 0.11.0, `yring` to 0.2.2.

## [0.3.1] - 2026-05-23

### Changed

- *(deps)* Bump `yring` to 0.2.1.

## [0.3.0] - 2026-05-23

### Added

- `ZMQ_STREAM` socket type for raw TCP communication.

### Fixed

- Portable `errno` access in `zmq_poll`: use `libc::__errno_location()` via the `libc` crate rather than a direct extern declaration.

### Changed

- *(deps)* Bump `omq-compio` to 0.10.0.

## [0.2.0] - 2026-05-21

### Changed

- *(breaking)* Package renamed from `omq-zmq` to `omq-libzmq`. Library name
  (`omq_zmq`) and output filenames (`libomq_zmq.so` etc.) unchanged.
- 7 socket options that returned `ENOTSUP` (`ZMQ_BACKLOG`, `ZMQ_IMMEDIATE`,
  `ZMQ_CONNECT_TIMEOUT`, `ZMQ_PROBE_ROUTER`, `ZMQ_REQ_CORRELATE`,
  `ZMQ_REQ_RELAXED`, `ZMQ_XPUB_NODROP`) now store and round-trip their values.
- 13 rarely-used socket options (`ZMQ_AFFINITY`, `ZMQ_RATE`, etc.) are now
  explicitly accepted as no-ops instead of silently ignored by the wildcard arm.
- `zmq_msg_get`: returns routing ID for property 5, returns -1/EINVAL for
  unknown properties (matching libzmq).
- `zmq_msg_gets`: returns empty string for known property names (`Socket-Type`,
  `Identity`, `Routing-Id`, `Peer-Address`) instead of always failing.
- `zmq_ctx_set`/`zmq_ctx_get`: accept `ZMQ_SOCKET_LIMIT` (3) and `ZMQ_IPV6` (42).
- `zmq_socket` enforces `ZMQ_MAX_SOCKETS`; returns `EMFILE` when exceeded.
- `zmq_send` enforces `ZMQ_MAX_MSGSZ`; returns `EMSGSIZE` for oversized frames.
- `ZMQ_CONNECT_TIMEOUT` wired to backend handshake timeout.
- *(deps)* Bump `omq-compio` to 0.9.0.

## [0.1.4] - 2026-05-20

### Changed

- *(deps)* Bump `omq-compio` to 0.8.0.

## [0.1.3] - 2026-05-17

### Changed

- *(deps)* Bump `flume` to 0.12.

## [0.1.2] - 2026-05-17

### Changed

- *(deps)* Bump `omq-compio` to 0.5.2.

## [0.1.1] - 2026-05-17

### Changed

- *(deps)* Bump `yring` to 0.2.0.

## [0.1.0] - 2026-05-17

Initial release: libzmq-compatible C interface backed by omq-compio.
