# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.14] - 2026-08-16

### Added

- `ProducerOwner::push_flush()` for a same-thread push plus flush with one
  owner check.
- `Consumer::prefetch_and_pop_with_full()` for receive paths that need to
  know whether a pop released space from a full ring.
- Loom coverage for full-to-nonfull transitions, refilled full rings, and
  cursor wraparound.

### Changed

- `ProducerOwner` now uses compact thread-local ownership tokens instead of
  storing `ThreadId` values.

## [0.3.13] - 2026-08-08

### Changed

- Add Loom coverage for popped values surviving SPSC slot reuse.

## [0.3.12] - 2026-08-02

### Changed

- Miri many-seed stress checks now cap cross-thread test sizes and use the
  extended timeout profile in CI.

## [0.3.11] - 2026-07-31

### Fixed

- Test-only async waker now drops consumed raw wakers, avoiding Miri leak
  reports.

## [0.3.10] - 2026-07-23

### Added

- Loom coverage for upper-layer stale low-water reactivation and canceled
  waiter readiness races.

## [0.3.9] - 2026-07-22

### Changed

- Replace the 64-bit target rejection with pointer-width cursors
  (`AtomicUsize` + `usize`) and a half-range capacity guard so 32-bit targets
  with pointer atomics can run safely.

## [0.3.8] - 2026-07-19

### Added

- `ProducerOwner` for mutex-free single-thread producer access.
- Loom test covering async drop wake race.

### Fixed

- `ProducerOwner` thread-affinity race: bind access to first caller
  thread with `OnceLock`.
- Async drop wake race: recheck consumer shutdown after waker
  registration.

### Changed

- Deny `unsafe_op_in_unsafe_fn` lint. All unsafe function bodies now
  require explicit `unsafe` blocks.
- Remove bounded prefetch API (replaced by size-class draining in
  `omq-tokio`).

## [0.3.6] - 2026-07-10

### Added

- `close()` and consumer-drop detection so pipe owners can wake blocked senders and detach dead consumers cleanly.

## [0.3.5] - 2026-07-04

### Fixed

- Use wrapping counters to prevent overflow on long-lived rings.
- Harden capacity validation.

## [0.3.4] - 2026-07-03

### Fixed

- `drop_remaining` is panic-safe and idempotent during async teardown.

### Changed

- Widen cache-line padding to 128 bytes.
- Reject 32-bit targets at compile time.
- Rename internal ring cursor fields for clarity.

### Benchmarks

- Add `flume` to the SPSC comparison chart.

## [0.3.3] - 2026-06-27

### Changed

- Rewrite README: problem/solution framing, backpressure docs, comparison benchmarks, chart.

### Added

- Comparison benchmark against rtrb and crossbeam-channel.
- Dark-theme SVG chart (`doc/spsc_comparison.svg`).
- Loom tests for sync ring protocol and `push_async`.

### Fixed

- `examples/basic.rs`: add missing `consumer.release()` call.

## [0.3.2] - 2026-06-26

### Changed

- Add `// SAFETY:` comments on `push`, `pop`, `drop_remaining` unsafe blocks.

## [0.3.1] - 2026-06-17

### Fixed

- `AsyncConsumer`/`AsyncProducer`: use explicit `consumer_dropped`/`producer_dropped` atomic flags instead of `Arc::strong_count` for disconnect detection. Eliminates false-positive EOF when a third `Arc` clone exists.
- `AsyncConsumer::poll_next`: release consumed positions before parking so the producer can reuse slots while the consumer waits.

## [0.3.0] - 2026-05-30

### Added

- `AsyncProducer::push_async`: async push that waits for space when the ring is full.
- `Consumer::release()` / `AsyncConsumer::release()`: explicit publish of consumed position. Enables batch-pop without a Release store per item.
- `Consumer::is_disconnected()`: detect producer-dropped + ring-drained.
- `Producer::flush_and_check()`: flush + report `was_empty` (one extra Acquire load). The plain `flush()` is now a single Release store.

### Changed

- Deduplicate sync/async ring operations into `Ring<T>` shared core.
- `Consumer::pop()` no longer publishes `head` on every call; callers must call `release()` after a batch.
- `Producer::flush()` simplified to a single Release store (no `head` load). Use `flush_and_check()` when `was_empty` is needed.
- `push_and_flush` returns `Result<(), T>` instead of `Result<FlushResult, T>`.
- `Producer::drop` / `Consumer::drop` flush and release automatically.

## [0.2.2] - 2026-05-25

### Fixed

- Sync `Producer::flush()` missed wakeup on stale `cached_head`.
- `async_spsc` missed wakeup on low-throughput flush.

### Changed

- *(deps)* Pin `atomic-waker` to 1.1.0.

## [0.2.1] - 2026-05-23

### Fixed

- `AsyncProducer::flush()`: refresh `cached_head` before the `was_empty` check to avoid using a stale value, which broke alternating push-pop patterns.

## [0.2.0] - 2026-05-17

### Added

- `async` feature: `AsyncProducer` (auto-wakes on flush) and `AsyncConsumer`
  (implements `futures_core::Stream`). No runtime dependency.
- `examples/basic.rs`: cross-thread producer/consumer demonstration.
- `benches/throughput.rs`: u64 and 128-byte throughput benchmark.

## [0.1.0] - 2026-05-17

### Added

- Bounded SPSC ring with ypipe-style batched flush/prefetch.
- `Producer`: zero-atomic `push`, single-Release `flush`.
- `Consumer`: zero-atomic `pop`, single-Acquire `prefetch`.
- `FlushResult` with `was_empty` flag for waker integration.
