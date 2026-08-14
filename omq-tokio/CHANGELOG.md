# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `exclusive::Socket`, a caller-owned direct-I/O socket for latency-sensitive
  single-peer workloads, initially supporting connected TCP DEALER with
  configurable timeouts, reconnects, heartbeats, and lifecycle monitoring.

### Changed

- Large direct receives now reuse bounded pooled buffers for medium-large
  frames and avoid zero-filling frames above the pool cap.

## [0.21.1] - 2026-08-08

### Added

- `blocking::Socket::recv_timeout()` for sync callers.
- Regression coverage for large payloads under fragmented reads, partial
  writes, and SPSC ring slot reuse.

### Fixed

- Large messages delivered through receive rings now keep payload bytes stable
  after producer slot reuse.
- Disconnected TCP peers now drop empty receive rings so reconnect-heavy
  sockets do not retain stale per-peer consumers.
- Long soak tests now account for allocator cleanup windows when checking live
  byte growth.

### Changed

- *(deps)* Bump `omq-proto` to 0.25.1 and `yring` to 0.3.13.

## [0.21.0] - 2026-08-02

### Added

- Receive-side `Options::conflate(true)` now keeps only the latest inbound
  message for conflate-capable sockets.

### Fixed

- Fair-queue receive order is preserved when multiple inbound peer rings are
  ready at once.
- Fan-out lane workers clear stale data readiness after empty drains, avoiding
  owned-runtime shutdown spins after zstd auto-training tests.
- zstd fan-out auto-training tests drain subscribers through the training tail
  before attaching late raw subscribers.

### Changed

- *(deps)* Bump `yring` to 0.3.12.

## [0.20.3] - 2026-07-31

### Added

- `zstd+tcp://` transport support behind the `zstd` feature, including
  zstd dictionary training for outgoing messages.

### Fixed

- Preserve trained dictionary ordering before compressed fan-out payloads.
- Close peer teardown without racing pending outbound driver state.

### Changed

- *(deps)* Bump `omq-proto` to 0.25.0 and `yring` to 0.3.11.

### Security

- Remove `lz4+wss://`; compressed secure WebSocket is intentionally
  unsupported to avoid BREACH/CRIME-style misuse.

## [0.20.2] - 2026-07-31

### Fixed

- Large byte-stream `PUB` fan-out messages now wake transmit drivers when the
  encoded frame exceeds the transmit slot byte cap. This fixes large
  `PUB`/`SUB` delivery over IPC and TCP.

## [0.20.0] - 2026-07-27

### Security

- Do not count pre-handshake TCP/IPC peers against data-plane peer limits.
  Pending handshakes no longer block PAIR/CHANNEL slots before authentication
  or ZMTP readiness.
- Add `Options::max_pending_handshakes` to cap simultaneous inbound
  byte-stream handshakes before peer driver allocation. Lower values reduce
  pre-auth resource exposure but can reject legitimate connection bursts;
  higher values allow bigger bursts while consuming more memory/tasks.
- Reject encrypted socket configurations that disable the ZMTP handshake
  timeout. Longer timeouts let slow peers complete authentication but also let
  stalled peers hold pending-handshake slots longer.

### Fixed

- `Socket::close()` with non-zero or forever linger now keeps bind/connect
  endpoints alive while queued connect-side pre-ready sends drain to late
  peers.
- Dropping the last socket handle now applies configured linger in the
  background instead of forcing zero-linger teardown.

### Changed

- Replace the shared round-robin no-peer fallback queue with connect-side
  pre-ready pipes allocated during `connect()`.
- Bound no-peer round-robin sends now mute like libzmq: async `send()` waits
  and `try_send()` returns `Full`.
- Document native no-peer, linger, and HWM edge cases against libzmq.
- *(deps)* Bump `omq-proto` to 0.24.0.

## [0.19.3] - 2026-07-23

### Added

- `StateSignal` for generation-based state change notification.
- Loom coverage for data readiness, space readiness, fallback activation, and
  send-pipe low-water races.
- Regression coverage for the PUSH/PULL stall from issue #186.
- Test coverage that keeps raw `tokio::sync::Notify` behind
  `engine::signal`.

### Fixed

- Lost wakeups in PUSH/PULL paths when producer notification races with drain
  clear, capacity release, or peer activation. Fixes issue #186.
- Inproc and TCP recv-bypass drains now preserve readiness across canceled
  waiters and stale empty reads.
- Send-pipe reactivation now rechecks occupancy when the low-water flag is
  stale.
- Max-message-size checks now use shared accounting for inproc and ZMTP
  receives.

### Changed

- Internal readiness primitives now use stateful signal types instead of raw
  `Notify`.
- Internal inproc SPSC recv notification names now use signal terminology.
- Gate the `loom` dev dependency to 64-bit targets.

## [0.19.2] - 2026-07-22

### Added

- `Proxy` helper for async message forwarding, with capture socket support
  and steerable `PAUSE`/`RESUME`/`TERMINATE`/`KILL` control.

### Changed

- 32-bit Linux targets with native 64-bit atomics are supported. Per-frame and
  per-message payloads remain bounded by platform allocation limits.

### Fixed

- PUSH fallback handoff and reactivation after peers appear.
- PUSH/PULL wakeups with bounded prefetch.
- Direct TCP write remainder flushing on partial writes.
- Socket close honors linger before driver shutdown.
- Reconnect and duplicate TCP connect tests now wait on handshakes instead of
  timing races.

### Performance

- Reduce idle TCP peer memory.
- Start TCP read buffers at 4 KiB and grow them under sustained larger reads.
- Map fan-out workers to IO lanes and restore PUB/SUB fast-path performance.

### Removed

- Unused `blume` dependency.

## [0.19.1] - 2026-07-19

### Fixed

- Duplicate connects to the same endpoint now match libzmq behavior:
  `DEALER`, `PUB`, `SUB`, and `REQ` treat the second connect as a no-op,
  while `PUSH` keeps separate weighted pipes.
- `Socket::disconnect()` closes every duplicate connection for a `PUSH`
  endpoint before later reconnects.
- Duplicate-connect behavior is covered for TCP and lz4+tcp transports.

## [0.19.0] - 2026-07-19

### Added

- `Context` API: `Context::new()`, `Context::with_config()`, and
  `Context::current()`. Each `Context` owns one or more
  `current_thread` tokio runtimes on dedicated OS threads.
  `Context::socket()` creates async sockets;
  `Context::blocking_socket()` creates sync sockets with background
  IO for callers with no async runtime.
- `Socket::dispatch()` for recv-loop multiplexing.
- Lazy and zero-thread context support: `ZMQ_IO_THREADS=0` for
  inproc-only operation.
- CURVE peer identity exposed to authenticator via
  `MechanismPeerInfo::identity`.

### Removed

- **Breaking:** `ConnectionDriver::with_recv_direct`. Replaced by
  the `yring` recv pipe.
- **Breaking:** BLAKE3ZMQ feature and support removed. Use CURVE.
- **Breaking:** `rt-multi-thread` feature removed. Multi-thread
  runtimes are now configured through `Context` IO thread count.
- **Breaking:** `transport::inproc::InprocSpsc` struct,
  `transport::inproc::connect`/`bind` functions removed (internal
  restructure).

### Fixed

- Fan-out LZ4 dict frame ordering when shard workers compress
  independently.
- Refresh fan-out encoders after dictionary training so late
  subscribers receive correct LZ4D frames.
- Bounded inproc backpressure: full rings apply backpressure instead
  of routing through a second producer path.
- Wake blocking inproc receivers on direct `yring` delivery.
- REP broker routing over byte-stream transports preserves all identity
  frames before the empty delimiter. Fixes multi-hop TCP and lz4+tcp
  replies.

### Performance

- Recv pipe: `yring`-based receive pipe replaces `async_channel`.
  Single writer (connection driver), single reader (socket handle).
  Eliminates async-channel overhead per message.
- Per-shard fan-out encoding: each shard worker encodes and compresses
  independently.
- Fan-out always routed through shard workers. Direct dispatch path
  removed for simpler code and consistent latency.
- `TransmitSlotCache` caller-side encode bypass removed. All sends
  flow through send pipes or shard workers.
- Reduce TCP read buffer and per-connection arena allocations.
- Reduce recv path allocations: merge `DrainState`, hoist channel
  future creation out of drain loop.
- Mutex-free inproc producer path via `ProducerOwner`.
- Coalesced inproc receive notifications via `DataSignal`.
- Fair receive source rotation prevents a busy peer from consuming
  the entire drain budget.
- Recv drains bounded by message size class.
- Timer-based queue wakeups removed; armed notifications with
  predicate rechecks replace them.

### Changed

- **Breaking:** `InprocConn` fields changed (`tx`/`rx` replace
  `spsc`).
- **Breaking:** `RecvSink` gains `Rep` variant.
- **Breaking:** `Socket` no longer implements `UnwindSafe`/
  `RefUnwindSafe`.
- *(deps)* Bump `omq-proto` to 0.23.0, `yring` to 0.3.8.

## [0.17.0] - 2026-07-10

### Added

- `Socket::wait_subscribed`: deterministic PUB/SUB subscription readiness check. Atomic counter shared between actor and socket handle, incremented after `peer_subscribe()`. No actor round-trip needed.
- `Socket::wait_connected`: polls the monitor stream until at least one peer completes the ZMTP handshake, with a configurable timeout.
- `Socket::disconnect` for live peers, matching libzmq semantics. Previously only worked for pending (not-yet-connected) endpoints.
- STREAM socket integration tests: bind/connect, raw TCP interop, identity-based routing, send to disconnected peer, multi-peer scenarios.
- `reconnect_stop_conn_refused` option: a connect endpoint stops reconnecting after a TCP connection-refused error.
- PUB/SUB churn soak test covering deterministic TCP peer-count phases under sustained load.

### Fixed

- `RecvSink::Yring` livelock on consumer drop: the yring recv-bypass path looped forever with 10 ms sleeps when the consumer was dropped. Check `is_consumer_dropped()` after the first failed push.
- `Exclusive::Submitter` livelock on shutdown: `send()` looped forever polling for a peer that would never appear. An `AtomicBool` closed flag now short-circuits to `Err(Closed)`.
- Fan-out shard tests updated for reduced `DIRECT_SHARD_PEER_CAP`.
- PUB/SUB churn soak gap window: reset per-subscriber sequence baselines when the measured phase starts.
- Single-thread PUB/SUB fan-out scheduling: correctly detect `current_thread` runtime and use cooperative yielding for shard workers.
- `blume` receiver ordering for large messages: deferred fan-out throughput collapsed because `blume` delivered in LIFO order when the internal buffer wrapped.

### Performance

- Reuse per-connection `BytesMut` for large-frame receives. `BytesMut::zeroed(plen)` allocated via `mmap` for payloads at or above 128 KiB, causing kernel page zeroing and page faults on every message. PUSH/PULL throughput at 256 KiB recovered from 1.4 GB/s to 4.7 GB/s.
- Multi-peer PUSH throughput: arena direct-write, round-robin modulo elimination, batch cap raised to 512. +3-9% on 8-peer TCP fan-out for 16 B to 2 KiB messages.
- Swap-to-back deactivation for round-robin send: eliminates O(n) shifts on peer deactivation/reactivation.
- Coalesced PUSH send-pipe wakeups: wake once while data is pending, rearm after drain.
- Sharded PUB fan-out with bounded worker tasks. Gradual shard ramp-up, direct path for first peers, capped at 8 workers per socket. Deferred compression for large sharded LZ4 sends.
- `DrainBudget` enforcement on transmit slot, send pipe, fallback queue, and deferred fan-out worker drain loops.
- `DataSignal` coalescing replaces ad-hoc atomic+Notify patterns across all producer-to-consumer signaling.

### Changed

- Route PUSH through `yring` send pipes. Per-peer pipe ownership; the shared fallback queue is only for pre-peer buffered sends.
- Fan-out sockets (PUB/XPUB/RADIO) ignore `OnMute::Block`, matching libzmq. Sharded fan-out drops muted peers with bounded drain work.
- Rename internal types (see omq-proto 0.21.0 changelog).
- *(deps)* Bump `omq-proto` to 0.21.0, `blume` to 0.4.5, `yring` to 0.3.6.

## [0.16.0] - 2026-07-04

### Added

- Windows named pipes support for IPC transport. Named pipes handle Windows-specific buffer management and connection lifecycle. All 20 socket types available over IPC on Windows.
- `reconnect_stop_conn_refused` option.

### Fixed

- Inproc receiver size limit propagation.
- Identity `try_send` backpressure reporting.
- Reject UDP datagrams with reserved flags.
- Timeout and DNS connect edge case hardening.

### Changed

- *(deps)* Bump `omq-proto` to 0.20.0, `blume` to 0.4.4, `yring` to 0.3.5.

### Removed

- `omq-compio` backend references and cross-runtime interop crate.

## [0.15.0] - 2026-07-03

### Fixed

- Multi-peer PUSH uses per-peer round-robin wire slots.
- `wait_for_targets_space` enables the `Notify` future before polling.
- Test sender lifetimes in `req_rep` and random-size coverage.

### Performance

- Rework send path and teardown cleanup.
- Route PUB through `dispatch_to_targets` and remove the dead fan-out pump.
- Extract recv mux, peer lifecycle, and wire-slot cache helpers from socket actor code.

### Changed

- Soak tests can select the tokio runtime flavor.
- *(deps)* Bump `omq-proto` to 0.19.0, `blume` to 0.4.3, `yring` to 0.3.4.

## [0.14.6] - 2026-06-27

### Fixed

- XPUB `nodrop` yield spin replaced with notify-based wait.

### Changed

- *(deps)* Bump `omq-proto` to 0.18.1, `bytes` 1.11 to 1.12, `socket2` 0.6.3 to 0.6.4.

## [0.14.5] - 2026-06-26

### Added

- `lz4+ws://`, `lz4+wss://` compressed WebSocket transport.
- Windows support (contributed by @MatthieuDartiailh).

### Fixed

- WS send bypass: encode as WS binary frames instead of raw ZMTP frames.

### Changed

- `#![forbid(unsafe_code)]` crate-wide. Replace `libc::setsockopt` with `socket2::SockRef`.
- *(deps)* Bump `omq-proto` to 0.18.0, `yring` to 0.3.2.

## [0.14.4] - 2026-06-22

### Fixed

- Wire-slot drain stall under sustained throughput.
- `FanOut` arena path bypassing `xpub_nodrop` backpressure.
- REQ send/recv alternation TOCTOU under concurrent senders.
- Driver select priority: heartbeat first-tick timeout fired immediately instead of after the configured interval.

### Performance

- Fan-out: adaptive yield interval scales with peer count and copy budget.
- Fan-out: batched `data_ready` signals reduce per-peer wakeup overhead.
- Fan-out: writev delegation to `EncodedQueue::write_vectored`, arena-only dispatch.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.3.

## [0.14.3] - 2026-06-17

### Fixed

- Connection churn: tolerate small message reordering between wire slot bypass and driver inbox paths during reconnection.

### Performance

- PUB/SUB fan-out: shared `FanOutArena` + `fan_out_pump` task. Encode once into a shared arena, pump distributes pre-encoded bytes to all subscribers. Eliminates per-peer encode on the send path.
- Cached multi-peer dispatch: `Submitter` caches target list and encoder across generations, avoiding lock acquisition on every send when the peer set is stable.
- Dynamic yield interval: scale with peer count instead of fixed 256-message interval.
- Disable 10ms safety timeout polling in connection driver, eliminating ~6400 spurious wakeups/sec at 64 peers.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.2, `yring` to 0.3.1.

## [0.14.2] - 2026-06-12

### Fixed

- lz4+tcp fan-out: PUB/XPUB/RADIO with lz4 compression now encodes once and distributes the compressed bytes to all subscribers via `push_pre_encoded`. Previously the fan-out path framed messages without applying the lz4 transform, causing subscribers to reject frames with "unknown lz4 sentinel" and reset connections (~150x throughput loss at 2+ subscribers).

### Changed

- *(deps)* Bump `omq-proto` to 0.17.1.

## [0.14.1] - 2026-06-12

### Removed

- `zstd` feature: `zstd+tcp://` transport removed in favor of `lz4+tcp://`.

### Changed

- *(deps)* Bump `omq-proto` to 0.17.0. Tighten `rustls-pki-types` to 1.14.

## [0.14.0] - 2026-06-10

### Added

- `PeerWireSlot`: per-peer encode buffer (`std::sync::Mutex`, nanosecond hold time) replaces `DirectIo`'s `Mutex<Writer>`. Handle encodes, driver flushes via `data_ready` select arm.
- `PeerSend` enum (`Wire`/`Inbox`): unified dispatch for fan-out, identity, and exclusive routing. Eliminates pump tasks for all strategies.
- `Exclusive` routing strategy for PAIR/CHANNEL (single slot, no queue).
- Encode-once PUB/SUB fan-out via `push_pre_encoded`.
- Subscription elision: skip per-peer filtering when all peers have `subscribe_all`.
- `RecvSink::Yring`: `ConnectionDriver` pushes decoded messages directly into a yring, bypassing the `async_channel` relay for single-peer TCP/IPC.
- `CompressionPool`: `spawn_blocking` offload for large messages with warm `MessageEncoder` reuse.
- `last_bound_endpoint()`.
- `Options::xpub_nodrop` support: `FanOut` pre-checks wire slots and returns `Full(msg)` when capacity is reached.
- 10 ms safety-net timers on notification-based await points.
- Fair-share batch limiting for shared-queue send.

### Fixed

- `PeerSend::Wire` falls back to driver inbox for crypto/compression-ineligible messages (was silently dropping).
- Dead `PeerWireSlot` no longer surfaces `Err(Closed)` to `send()`: falls through to `SendSubmitter` queue.
- SPSC and recv-yring fast paths recover after peer churn (were one-shot).
- Lost-wakeup race in `SpscAwareRecv::recv()`.
- Silent message loss in `encode_msg` (was `unwrap_or_default()`).
- WSS cert panic: return `Err` when no system certs found.
- REQ: set `req_awaiting_reply` after successful send, not before. Loop to skip malformed messages in `try_recv`.
- `Exclusive::send()` awaits peer instead of returning error before connect.
- Flush encode slot on cancel (was losing pending data).
- `SO_REUSEADDR` on TCP listener sockets.

### Performance

- PeerWireSlot: handle encodes under nanosecond Mutex, driver flushes. Replaces DirectIo async Mutex.
- Fan-out: encode once, push pre-encoded bytes to all subscribers.
- Batch yring consumer pops, defer Release store.

### Changed

- Remove `priority` feature.
- Remove `DirectIo`, `SharedWriter`, `DirectIoSlot`.
- *(deps)* Bump `omq-proto` to 0.16.0, `chacha20-blake3` to 0.10.0.

## [0.13.0] - 2026-05-30

### Added

- Direct I/O bypass for single-peer connections.

### Fixed

- DirectIo misrouting when multiple peers connect.

### Changed

- Refactor direct I/O to keep driver alive after handoff.
- *(deps)* Bump `omq-proto` to 0.15.0. Tighten `concurrent-queue` to 2.5.0, `rustc-hash` to 2.1.0, `thiserror` to 2.0.18, `tokio` to 1.52.0.

## [0.12.0] - 2026-05-25

### Added

- Zero-copy read path with `BytesMut`.
- recv-direct bypass for REQ sockets.
- Bypass actor for REQ/REP send (latency optimization).
- Atomic REQ alternation flag, bypass Mutex on hot path.

### Fixed

- Drain codec events before propagating `handle_input` error.

### Changed

- Use `FxHashMap`/`FxHashSet` for internal maps.
- *(deps)* Bump `omq-proto` to 0.14.0, `yring` to 0.2.2. Upgrade `rand` 0.8 → 0.10.

## [0.11.1] - 2026-05-23

### Changed

- *(deps)* Bump `yring` to 0.2.1.

## [0.11.0] - 2026-05-23

### Added

- WebSocket transport (`ws://` / `wss://`).
- `ZMQ_STREAM` socket type for raw TCP communication.
- WS `EncodedQueue` fast paths (server-side and client-side).

### Fixed

- WS driver: large-message bypass, leftover bytes, and encode path correctness.
- REQ socket state machine reset race on reconnect.

### Changed

- Drop `tungstenite` / `tokio-tungstenite` dependencies.
- *(deps)* Bump `omq-proto` to 0.13.0.

## [0.10.0] - 2026-05-21

### Changed

- Replace `flume` send queue with `concurrent-queue` + `tokio::sync::Notify`.
- Batch semaphore permit release in `DropQueue`: one `add_permits(N)` per batch instead of N individual calls. 128B 1-PUSH/8-PULL TCP: 559 -> 940 MB/s (+68%).
- Remove unused `blume` dependency.
- *(deps)* Bump `omq-proto` to 0.12.0.

### Fixed

- Dead-code errors when building with `feature = "priority"`.

## [0.9.0] - 2026-05-21

### Changed

- *(deps)* Bump `omq-proto` to 0.11.0.

## [0.8.1] - 2026-05-20

### Changed

- Route compression encoder output through `EncodedQueue` for batched vectored writes. lz4+tcp 32B: 142k -> 2.3M msg/s; lz4+tcp 512B: 140k -> 1.5M msg/s.
- Sub-threshold messages on compression transports take a sentinel-prefix fast path that bypasses the encoder entirely.

## [0.8.0] - 2026-05-20

### Changed

- *(deps)* Bump `omq-proto` to 0.10.0.

## [0.7.0] - 2026-05-20

### Fixed

- Flaky `inproc_strict_precedence` priority test: replaced monitor-event polling with `connections()` polling to ensure the routing table is populated before sending.

### Changed

- Bench warmup: time-bound prime phase (500 ms cap) and start calibration at small iteration counts. Large-message cells no longer spend 30+ seconds in warmup.

## [0.6.1] - 2026-05-19

### Fixed

- Priority-mode message loss during reconnect storms: close the peer inbox and cancel the driver token on EOF so concurrent senders see the peer as dead immediately. Before: 67.9% delivery at 300 s; after: 99.6% delivery at 120 s.

## [0.6.0] - 2026-05-19

### Added

- `impl SocketApi for Socket` for compile-time API parity with omq-compio.

### Changed

- `bind()` returns `Result<Endpoint>` instead of `Result<()>`.
- `actor.rs` split into `actor/endpoints.rs` and `actor/peer.rs` for readability.

## [0.5.5] - 2026-05-18

### Added

- `EncodedQueue`: port of omq-compio's unified flat+gather encoding queue. Replaces the separate `flat_buf` and codec write paths with a single drain-based flush that reuses a `Vec<Bytes>` across iterations.
- Configurable batch byte cap via `OMQ_BATCH_BYTES` env var (default 1 MiB, up from hard-coded 512 KiB).

### Changed

- `FLAT_THRESHOLD` raised from 48 KiB to 64 KiB.
- `flush_once` now uses `transmit_chunks_capped(128)` to bound iovec count.
- *(deps)* Bump `omq-proto` to 0.8.4.

### Fixed

- Exclude PAIR from inproc SPSC eligibility: both sides receive, so concurrent recv would compete for messages from the same ring.

## [0.5.4] - 2026-05-18

### Added

- `soak` Cargo feature gating 12 long-running leak-detection scenarios.

## [0.5.3] - 2026-05-17

### Changed

- *(deps)* Bump `flume` to 0.12, `socket2` to 0.6.

## [0.5.2] - 2026-05-17

### Changed

- *(deps)* Bump `omq-proto` to 0.8.1, `blume` to 0.2.1.

## [0.5.1] - 2026-05-17

### Changed

- *(deps)* Bump `yring` to 0.2.0.

## [0.5.0] - 2026-05-17

### Changed

- *(deps)* Replace `blume::spsc` with standalone `yring` crate.
- *(deps)* Bump `blume` to 0.2.0.

## [0.4.0] - 2026-05-14

### Added

- ROUTER/SERVER identity handover: when a new connection claims an identity
  already held by an existing peer, the old connection is evicted.

### Changed

- *(deps)* Bump `omq-proto` to 0.8.0.

## [0.3.0] - 2026-05-14

### Added

- PLAIN security mechanism support via `plain` feature flag.
- PLAIN interop tests against libzmq via pyzmq.

### Fixed

- *(test)* Fix interop_ruby TCP port collisions (AddrInUse).

### Changed

- *(deps)* Bump `omq-proto` to 0.7.0.

## [0.2.8] - 2026-05-13

### Changed

- *(deps)* Bump `omq-proto` to 0.6.0.
- Post-handshake READY/ERROR commands now drop the connection (protocol
  violation per ZMTP RFC 23). Test updated accordingly.

## [0.2.7](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.6...omq-tokio-v0.2.7) - 2026-05-12

### Changed

- *(deps)* Bump `omq-proto` to 0.4.0.

## [0.2.6](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.5...omq-tokio-v0.2.6) - 2026-05-09

### Changed

- *(deps)* replace `compio` git dev-dep with `version = "0.19.0-rc.1"` to
  match omq-compio's registry dep; avoids duplicate compio-runtime instances
  that caused TLS mismatch panics in interop_compio tests. No library change.

## [0.2.5](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.4...omq-tokio-v0.2.5) - 2026-05-09

### Fixed

- *(test)* `inproc_equal_priorities_round_robin`: wait for all 3 handshakes
  before sending. Without the barrier, messages sent before a peer registered
  in the routing table skewed the distribution, causing a spurious
  "tier round-robin starved" failure. No library behavior change.

## [0.2.4](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.3...omq-tokio-v0.2.4) - 2026-05-09

### Added

- `Options::large_message_threshold(n)` /
  `Options::disable_large_message_path()` are accepted on tokio for
  API parity with omq-compio. They have no effect: tokio's recv path
  does not use buf-rings, so the multi-shot vs one-shot switch the
  knob controls only matters on omq-compio. Code that compiles
  against the compio backend stays compiling against tokio.

### Changed

- The codec inbound buffer (from `omq-proto`) is now a chunked queue. For
  tokio, the read path still copies from the stack buffer into `Bytes` once
  per read (same as before), but the codec no longer reallocates as
  messages grow: each received slice is appended as a fixed chunk rather
  than into a growing `BytesMut`. Large messages see one copy per read
  instead of O(n log n) copies from repeated doubling.

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.2...omq-tokio-v0.2.3) - 2026-05-05

### Fixed

- *(tokio)* keep race-arrived peers spawned during `begin_close()` linger
  drain. When a TCP handshake completed after `closing = true` was set,
  the actor's `Connected`/`Accepted` arms used to drop the peer entirely,
  leaving messages already in the outbound queue unsent (incl. zstd dict
  shipments). Now the peer is spawned whenever the send-strategy queue
  is non-empty; teardown still cancels it once drained or linger expires.

### Changed

- *(deps)* require `omq-proto = 0.2.3` for the wire-compatible zstd dict
  shipment (see omq-proto CHANGELOG 0.2.3).

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-tokio-v0.2.1...omq-tokio-v0.2.2) - 2026-05-04

### Fixed

- *(tokio)* sync reconnect test on second handshake under priority — the
  `peer_drop_mid_send_is_handled_cleanly` test was racing the disconnect
  with its post-reconnect send, surfacing under `--features priority`
  because the per-pipe inbox can't survive its peer's exit. Test-only
  change; no library behavior change. The standard ZMQ "messages queued
  for a vanished peer are lost" semantic is now documented in the
  priority-mode block of `routing/round_robin.rs`.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-tokio-v0.2.1) - 2026-05-04

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- *(priority)* correct strict-precedence routing; unblock REQ/REP under priority
- *(compio,tokio)* resolve REQ/REP deadlocks and tier-1 test regressions
- Socket::drop cancels tasks; REQ/REP reset on disconnect; tier-1 tests
- reconnect after mid-session drop + close() listener/dialer teardown
- *(clippy)* collapse nested if-let chains (collapsible_if, new in 1.93)
- *(curve)* wire-compatible with libzmq + pyzmq interop suite
- XPUB honors legacy ZMTP 3.0 0x01-prefix message subscribes

### Other

- optimize flat-buf threshold and update benchmarks
- pre-release polish, American English, comprehensive test-all.sh
- tighten default size sweep to 128 B / 2 KiB / 8 KiB
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- *(tokio)* bypass actor on hot send/recv paths; add zmq.rs bench
- fix all clippy warnings across workspace (--all-features)
- median-of-3 runs; default 3 sizes; --all-sizes flag
- *(transform)* split MessageTransform into MessageEncoder + MessageDecoder
- *(tokio)* FLAT_THRESHOLD 32 KiB — fix 2 KiB TCP regression; fix(compio): second codec_maybe_dirty race; bench: add 32B size, 8-peer column
- *(tokio)* 64 KiB read buffer, direct shared-queue arm, eliminate pump tasks
- *(compio)* flat-buf encoding, drain-vec reuse, codec-skip guards, passthrough fast path
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
