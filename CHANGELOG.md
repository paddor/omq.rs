## Changelog

All notable changes to omq.rs will be documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- `InprocConn::out` and `InprocConn::in_rx` expose `RelaySender` and
  `RelayReceiver` with separate command/data lanes instead of Tokio halves.

- WS/WSS ready connections default to at most 1024 per socket, configurable
  through `Options::ws.max_ready_peers` (C: `OMQ_WS_MAX_READY_PEERS`). Other
  transports have separate admission; identity handover reuses a ready slot.
- WS/WSS listeners reject browser requests unless their Origin appears in
  `Options::ws.allowed_origins` (C: `OMQ_WS_ALLOWED_ORIGINS`). Missing Origin
  remains allowed for native clients. Resource path/query must match the bind.
- Remove opt-in PEER receive partitioning: `Socket::peer_recv_lanes`,
  `PeerRecvConfig`, and `PeerRecvLane`. Use ordinary socket receive methods;
  application code owns worker dispatch. Internal fanring fairness, bounded
  queues, and reconnect fencing remain unchanged.
- `engine::driver::DriverStream::Writer` must implement the new
  `DriverWrite` trait. `tokio::io::WriteHalf` and Tokio TCP write halves
  implement it; other writers need an empty `impl DriverWrite`.

### Changed

- Use published `fanring` 0.3.9 and `yring` 0.3.20 across native and binding
  builds, including receiver lane control without Git dependencies.

- Socket-owned protocol inboxes and inproc relays use one Coordinated
  fanring producer per lane. Bounded lifecycle slots keep activation and
  shutdown reachable; inproc commands remain separate from relay messages.

- Socket-owned fallback data inboxes use bounded fanring lanes, registered
  lazily per socket clone and destination. Preserve exact lane HWMs, fan-out
  publication admission, producer FIFO, and graceful close draining.

- Socket-owned peer drivers send actor-bound application data through bounded
  fanring lanes, one per connection. Protocol events use a separate control
  mailbox; public standalone drivers retain their supplied Tokio event queue.

- pyomq admits eligible async sends and receives directly, preserving fallback
  FIFO, bounded workers, and external REQ/REP admission. Ready asyncio polls
  avoid executor dispatch. Windows native callbacks defer Python hooks until
  producer and binding locks are released.

- Inproc messages no longer pass through an I/O thread. Each direction of an
  inproc connection is one `yring` that holds the sender's `send_hwm` plus the
  receiver's `recv_hwm` messages. `send` pushes into it on the calling thread
  and the peer's `recv` drains it. This applies to round-robin, exclusive,
  identity-routed, and fan-out sends (PUSH, DEALER, REQ, REP, ROUTER, PAIR,
  CLIENT, SERVER, CHANNEL, SCATTER, PUB, XPUB, RADIO). A publisher matches
  subscriptions and pushes into each inproc subscriber's ring on the calling
  thread; `xpub_nodrop` waits per subscriber. PEER and `conflate` senders
  still relay through their peer task. A blocking `send` that hits a full
  queue now waits on the calling thread instead of handing the wait to the
  IO thread. Blocking REQ/REP round trips between two threads take two
  thread wakes instead of four.
- C API: `ZMQ_IO_THREADS` set to 0 runs one IO thread instead of a
  PUSH/PULL-only mode. Every socket type and transport works on such a
  context; `zmq_bind` and `zmq_connect` no longer return `ENOTSUP` for it.
  Inproc PUSH/PULL now uses the shared inproc rings; the separate byte ring
  (`inproc_bypass`) is removed.

- Default connection setup timeout to 10 seconds across Rust and all bindings,
  covering DNS, dialing, carrier setup, and ZMTP READY under one deadline.
  QUIC defaults to a 10-second transport idle timeout and 2-second keepalive.
- Share verified TLS trust, identity, and server-name setup internally while
  isolating WSS's explicit insecure test override. Empty custom trust PEM now
  fails configuration validation.
- Redact TLS private keys and PLAIN client passwords from socket-option
  `Debug` output in the Rust and C compatibility layers.
- WS/WSS requires a finite `handshake_timeout`, shared across transport setup
  and ZMTP. Pending-handshake admission includes outbound attempts and WS/WSS
  before HTTP/TLS; each WS/WSS listener admits at most 32 pending peers.
- PEER receives use fanring ready-peer selection and batched space credits,
  retaining identity routing, reconnect fencing, and aggregate receive budgets.
- Upgrade compression dependencies to `lz4rip` 0.11.8 and `zrip` 0.8.10.
- Tokio direct TCP receives reuse bounded buffers through 8 MiB without
  zero-filling payload memory or extending pool lifetime past connection close.
- Tokio `recv_batching` bulk receives move whole per-connection windows at
  once, under the same message and byte budgets as before.

### Added

- `HandshakeRefusal` preserves fatal ZMTP ERROR details. `ConnectStopped`
  monitor events report when a refusal stops automatic connection attempts.
- `quic://host:port` raw OMQ over QUIC behind the `quic` feature (Quinn,
  rustls/ring). One connection per
  peer: stream 0 carries unchanged ZMTP, stream 4 carries carrier liveness.
  ALPN `omq-zmtp/1`, verified server certificates, no 0-RTT, no DATAGRAM
  frames, no OMQ compression. Configure through `Options::quic`
  (`QuicOptions`). Heartbeat options drive the liveness stream, linger waits
  for FIN acknowledgment and peer close, and `recv_buffer_size` /
  `send_buffer_size` apply to the UDP sockets. Connectors share one UDP socket per IO
  thread. On Linux, a listener binds one `SO_REUSEPORT` socket per IO thread
  and steers datagrams by connection ID, so each connection stays on one IO
  thread. `omq-libzmq` exposes it through the optional `quic` feature,
  `OMQ_QUIC_*` socket options, and `zmq_has`.
- Tokio byte-stream sockets can enforce hard per-connection and aggregate
  per-IP receive token buckets with `Options::recv_rate_limit()` and
  `Options::recv_ip_rate_limit()`. Offending connections are disconnected.
- SERVER sockets can resolve a received routing id to its live `PeerInfo`
  with `Socket::peer_info()`.

### Fixed

- Stop automatic retries after fatal handshake ERROR responses while preserving
  retries after temporary authentication and transport failures.
- Default ROUTER sends drop complete messages to full destination queues.
  Mandatory ROUTER sends retain backpressure and nonblocking errors.
- pyomq sync sends honor `DONTWAIT`; empty nonblocking receives return `Again`.
- QUIC unbind/rebind preserves accepted peers and the complete UDP endpoint
  group. A replacement listener can rotate its certificate within the same
  context without interrupting existing connections.
- QUIC liveness services output and timeouts during receive floods. Delayed
  heartbeat timers skip missed probes, and oversized durations do not panic.
- QUIC connectors validate the server liveness preface before starting ZMTP
  and reporting READY. Missing and partial prefaces obey the setup deadline.

- Finite native linger includes actor command admission, so blocked
  subscription forwarding cannot prevent close from reaching its deadline.

- Full actor receive queues no longer block another peer's handshake or XPUB
  subscription state. XPUB notifications remain bounded and preserve each
  peer's FIFO through receive backpressure and driver completion.

- Preserve REQ application frames returned by a muted `try_send`.
- Keep Ruby monitor waits under their original timeout across stale wakeups.
- Apply configured reconnect delays after driver handshake failure and peer
  loss. Initial DNS errors still fail bind/connect; reconnect DNS failures
  retry internally. Named QUIC DNS runs outside the socket actor.
- REP splits the request envelope at the first empty frame, like libzmq.
  Requests whose body had an empty second part lost their leading parts.
- REP with `WorkloadProfile::Throughput` or CURVE replies to the peer that
  sent each request. It kept one envelope slot, so replies to queued requests
  failed with a protocol error, and it sent replies round-robin.
- Locally rejected handshakes report closure through the normal driver
  lifecycle, preserving outbound reconnect after ready/receive admission fails.
- Yield between bounded WebSocket parsing turns without losing buffered
  frames. Resume input after checking driver control and shutdown, preserving
  receive order in throughput and latency profiles.

- Cap WebSocket multipart and fragment counts, including empty input. Bound
  PONG backlog to one staged frame plus the latest pending payload, preserving
  partial writes and discarding pending PONG output when CLOSE is queued.

- Resolve named TCP/WS hosts on a bounded OS worker pool, independent of
  Tokio runtime shutdown. Cap queued lookups and returned addresses; canceled
  platform lookups cannot accumulate unbounded blocking tasks.
- Named endpoint setup no longer waits inside the socket actor. Cap pending
  operations, cancel on caller drop or endpoint removal, and carry shared
  admission through DNS and authentication.
- WebSocket upgrade selection must match the configured mechanism and offered
  profiles. Keep the existing browser NULL/PLAIN `ZWS2.0` profile; reject
  unoffered server selections and typed endpoint HTTP header injection.
- WebSocket HTTP upgrades validate exact start lines, required and duplicate
  fields, canonical keys, protocol lists, and response upgrade headers. Reject
  HTTP bodies and unsolicited extensions; cap heads at 4096 bytes/64 fields.
- Stalled WS/WSS setup no longer serializes accepts. Setup reservations survive
  HTTP upgrade through ZMTP, release on failure/cancellation, and share the
  socket limit. Unbind cancels pending setup while preserving ready peers.
- Reconnect-delay monitoring no longer spawns a task per notification, and
  disconnected dialers cannot materialize a late queued connection.
- Native WS/WSS client masks now use a cryptographic generator. WebSocket
  receive rejects nonminimal lengths, invalid CLOSE status/reasons, and a new
  tiny binary message interleaved into an unfinished fragmented message.
- Canceling a Tokio dial now drops an in-flight connection attempt, including
  stalled WS HTTP upgrades and WSS TLS handshakes, without changing retry policy.
- Concurrent parked PEER receives pass a batch wake onward, including when a
  notified receive is canceled, so queued messages cannot strand a waiter.
- A PEER replacement rejected by receive limits no longer evicts the
  current connection with the same identity.
- Tokio `xpub_nodrop` sends waiting for fan-out lane space no longer wait
  forever when the lane worker exits during close.
- Tokio data-ready signaling can no longer strand the last queued message of
  a burst. A producer that found the signal pending skipped its wake, and
  without a full fence on both sides the consumer could read a stale queue
  and park. `DataSignal` now fences in `mark` and `begin_drain`.
- Async inproc `PUSH` sends remain safe when a task moves between runtime
  worker threads or cloned socket handles send concurrently.

## [pyomq 0.19.1] - 2026-08-02

### Fixed

- `CONFLATE` now keeps only the latest inbound message on receive-side
  sockets through `omq-tokio` 0.21.0.
- Pyomq CI uses its own nextest `ci` profile so root workspace overrides for
  `omq-tokio` binaries do not break test runs.

### Changed

- *(deps)* Bump bundled `omq-tokio` to 0.21.0 and `yring` to 0.3.12.

## [omq-tokio 0.21.0] - 2026-08-02

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

## [omq-libzmq 0.5.11] - 2026-08-02

### Changed

- *(deps)* Bump `omq-tokio` to 0.21.0 and `yring` to 0.3.12.

## [yring 0.3.12] - 2026-08-02

### Changed

- Miri many-seed stress checks now cap cross-thread test sizes and use the
  extended timeout profile in CI.

## [omq-tokio 0.20.2] - 2026-07-31

### Fixed

- Large byte-stream `PUB` fan-out messages now wake transmit drivers when the
  encoded frame exceeds the transmit slot byte cap. This fixes large
  `PUB`/`SUB` delivery over IPC and TCP.

## [omq-libzmq 0.5.9] - 2026-07-31

### Changed

- *(deps)* Bump `omq-tokio` to 0.20.2.

## [omq-proto 0.24.1] - 2026-07-28

### Added

- `FrameBuffer::pop_front_entry()` for oldest-entry eviction in framed
  outbound queues.

### Changed

- `OnMute::DropOldest` docs now state fan-out support for `PUB`, `XPUB`,
  and `RADIO`.

## [omq-tokio 0.20.1] - 2026-07-28

### Added

- `OnMute::DropOldest` support for `PUB`, `XPUB`, and `RADIO` fan-out
  queues, including lane workers and fallback peers.

### Changed

- Bind dispatch now returns transport-resolved endpoints directly for TCP,
  IPC, WS, and WSS transports.
- PR CI now runs full-platform fmt/clippy before a Linux workspace test gate,
  with heavier Linux-only interop, fuzz, Loom, and Miri jobs behind it.
- Nightly extended CI now covers longer fuzz runs, soak groups, stress tests,
  libzmq draft interop, Ubuntu ARM, and multi-seed Miri.

### Fixed

- Named TCP connect preflight still fails at `connect()` time while later
  reconnect attempts silently retry name resolution.
- LZ4 fan-out auto-training tests now avoid the slow late-subscriber path
  that could hang on Ubuntu ARM CI.

## [omq-libzmq 0.5.8] - 2026-07-28

### Changed

- *(deps)* Bump `omq-tokio` to 0.20.1 and `omq-proto` to 0.24.1.

### Fixed

- libzmq compatibility tests now use OS-assigned TCP ports instead of fixed
  ports, removing bind collisions in parallel CI.

### Added

- **Windows support for pyomq**
  Compared to pyzmq pyomq can be used with the ProactorEventLoop
- CI coverage for `i686-unknown-linux-gnu` and
  `armv7-unknown-linux-gnueabihf`.
- ruff linting and ty type checking added to pyomq CI pipeline.

### Changed

- `yring` cursors and libzmq inproc bypass cursors now use 64-bit atomics
  on 32-bit targets.
- 32-bit builds are supported on targets with native 64-bit atomics. Per-frame
  and per-message payloads remain bounded by platform allocation limits
  (below 4 GiB on 32-bit Linux).

### Fixed

- pyomq Windows async readiness drains consume latched wakeups instead of
  reusing stale wakeups after a waiter remains blocked.
- pyomq Windows readiness signals skip unarmed wakeups to reduce stale async
  callbacks during sustained receive drains.

### Removed

- `blume` workspace crate and unused `omq-tokio` dependency.

## [omq-proto 0.23.0] - 2026-07-19

### Added

- `Options::workload_profile` for latency vs throughput tuning.
- CURVE authenticators receive peer routing identity in
  `MechanismPeerInfo`.

### Performance

- `Message` shrunk from 80 B to 64 B. Inline threshold reduced from
  71 to 55 bytes. Halves per-message memory on the hot path.
- `FrameBuffer` arena reduced from 256 KiB to 16 KiB (TCP/WS) and
  64 KiB (IPC).
- Per-shard fan-out encoding: shard workers encode and compress
  independently.
- Shared batch cap reduced from 1 MiB to 128 KiB.

### Changed

- **Breaking:** `FrameBuffer::with_arena_threshold` renamed to
  `FrameBuffer::with_config`.
- **Breaking:** BLAKE3ZMQ mechanism removed. Use CURVE.
- *(deps)* Bump `lz4rip` 0.9 to 0.11.1.

## [omq-tokio 0.19.0] - 2026-07-19

### Added

- `Context` API: `Context::new()`, `Context::with_config()`,
  `Context::current()`, and `Context::blocking_socket()`.
- `Socket::dispatch()` for recv-loop multiplexing.
- Lazy and zero-thread context support.

### Performance

- Recv pipe: `yring`-based receive pipe replaces `async_channel`.
- Per-shard fan-out encoding.
- Fan-out always via shard workers; direct dispatch removed.
- `TransmitSlotCache` caller-side encode bypass removed. All sends
  via send pipes or shard workers.
- Mutex-free inproc producer path.
- Coalesced inproc receive notifications.

### Fixed

- Fan-out LZ4 dict frame ordering with per-shard encoding.
- REP broker routing over byte-stream transports preserves all identity
  frames before the empty delimiter. Fixes multi-hop TCP and lz4+tcp
  replies.

### Removed

- **Breaking:** `ConnectionDriver::with_recv_direct`.
- **Breaking:** BLAKE3ZMQ support removed. Use CURVE.
- **Breaking:** `rt-multi-thread` feature removed.

### Changed

- *(deps)* Bump `omq-proto` to 0.23.0, `yring` to 0.3.8.

## [yring 0.3.8] - 2026-07-19

### Added

- `ProducerOwner` for mutex-free single-thread producer access.
- Loom test coverage for async drop wake races.

### Fixed

- `ProducerOwner` thread-affinity race.
- Async drop wake race.

### Changed

- Deny `unsafe_op_in_unsafe_fn` lint.

## [omq-libzmq 0.5.3] - 2026-07-19

### Added

- Lazy and zero-thread context support.

### Fixed

- Bounded inproc backpressure in the C API layer.
- REP broker envelopes through byte-stream ROUTER/DEALER proxies preserve
  all routing frames via `omq-tokio` 0.19.0.

### Changed

- Deny `unsafe_op_in_unsafe_fn` lint.
- Port to `Context` API.
- *(deps)* Bump `omq-tokio` to 0.19.0, `yring` to 0.3.8.

## [omq-proto 0.21.0] - 2026-07-10

### Added

- `DrainBudget` type: caps every drain loop by message count and byte
  count.
- `DataSignal` type: coalesced producer-to-consumer wakeups.
- `reconnect_stop_conn_refused` option.

### Performance

- `Message` inline storage raised to 71 bytes (80-byte value).
- Arena threshold lowered from 96 KiB to 4 KiB for zero-copy
  gather-write.
- Encode-once fan-out: shared pre-encoded wire bytes for PUB/XPUB/RADIO.
- LZ4 fan-out dictionary shipment from a socket-level encoder.
- `FrameBuffer` direct-write path for arena-only batches.
- Round-robin modulo elimination.

### Changed

- Fan-out sockets (PUB/XPUB/RADIO) ignore `OnMute::Block`, matching
  libzmq.
- Rename internal types: `EncodedQueue` to `FrameBuffer`,
  `WireSlot` to `PeerTransmitSlot`, `DropQueue` to `FallbackQueue`,
  `PeerSend` to `PeerOutbound`.

## [omq-tokio 0.17.0] - 2026-07-10

### Added

- `Socket::wait_subscribed`: deterministic PUB/SUB subscription
  readiness.
- `Socket::wait_connected`: poll until a peer completes the ZMTP
  handshake.
- `Socket::disconnect` for live peers.
- `reconnect_stop_conn_refused` option.

### Performance

- Reuse per-connection `BytesMut` for large-frame receives. 256 KiB
  recovered from 1.4 GB/s to 4.7 GB/s.
- Sharded PUB fan-out with bounded worker tasks.
- `DrainBudget` enforcement on all drain loops.
- `DataSignal` coalescing across all producer-to-consumer signaling.
- Multi-peer PUSH: arena direct-write, round-robin modulo elimination,
  batch cap raised to 512.
- Coalesced PUSH send-pipe wakeups.

### Fixed

- `RecvSink::Yring` livelock on consumer drop.
- `Exclusive::Submitter` livelock on shutdown.
- `blume` receiver ordering for large messages.

### Changed

- Route PUSH through `yring` send pipes. Fan-out sockets ignore
  `OnMute::Block`.

## [yring 0.3.6] - 2026-07-10

### Added

- `close()` and consumer-drop detection.

## [blume 0.4.5] - 2026-07-10

### Fixed

- Receiver drain ordering for large messages: LIFO delivery when the
  internal buffer wrapped caused fan-out throughput collapse.

## [omq-libzmq 0.5.1] - 2026-07-10

### Performance

- Remove unconditional `thread::yield_now()` on send success.
- Zero-alloc `zmq_recv` fast path.
- `zmq_msg_recv` single `malloc` for frames up to 128 bytes.

### Fixed

- Skip `malloc(0)` for zero-length frames.
- Crash on `recv_drain` mutex poison instead of silent hang.

### Removed

- Remove the experimental `omq-compio` backend and its examples,
  tests, benchmark peers, and charts.

## [omq-proto 0.20.0] - 2026-07-04

### Added

- Windows IPC path support: `IpcPath::NamedPipe` is available on
  Windows, with validation for reserved device names, invalid NTFS
  characters, length, and control characters.

### Fixed

- Harden protocol edge cases around direct receive size checks, UDP
  datagram flags, and WebSocket handshakes.

## [blume 0.4.4] - 2026-07-04

### Fixed

- Harden close/drop handling and sender-count overflow checks.

## [yring 0.3.5] - 2026-07-04

### Fixed

- Harden capacity validation and use wrapping counters for long-running
  rings.

## [omq-compio 0.12.7] - 2026-07-04

### Changed

- Isolate the remaining unsafe internals behind small wrapper modules
  and add deterministic guard-drop tests for the cell wrappers.
- Propagate inproc receive-size limits without changing the public
  socket API.

## [omq-tokio 0.16.0] - 2026-07-04

### Added

- Windows named-pipe IPC transport support. IPC now works on Windows
  using the same `ipc://` endpoint surface as Unix-domain IPC.

### Fixed

- Handle busy Windows named pipes during IPC connect retries.
- Preserve the public `omq_tokio::transport::inproc::connect` API while
  keeping inproc receive-size limit propagation.

## [omq-libzmq 0.5.0] - 2026-07-04

### Added

- Enable IPC support on Windows through the `omq-tokio` named-pipe
  transport.

### Fixed

- Harden C API edge cases around options and message properties.

## 2026-06-17

### omq-proto 0.17.2

- Windows support: `Endpoint::Ipc`/`IpcPath` gated behind `#[cfg(unix)]`, new `SocketRef` trait abstracts `AsFd`/`AsSocket`.
- Fix `Command::Error` panic on overlong reasons, frame parser rejects `isize::MAX` overflow, CURVE surfaces `ERROR` commands.
- `compression_dict` setter deferred to `Options::validate()` (no longer panics).
- Upgrade lz4rip 0.4 to 0.5.2.

### omq-tokio 0.14.3

- PUB/SUB fan-out: shared `FanOutArena` + `fan_out_pump` task eliminates per-peer encode. Cached multi-peer dispatch avoids lock on stable peer sets.
- Dynamic yield interval scales with peer count. Disabled 10ms safety timeout polling (~6400 spurious wakeups/sec at 64 peers).
- Tolerate small message reordering during connection churn.

### omq-compio 0.12.2

- Fix `flush_codec_to_wire` / `flush_codec_output` race.
- Fix heartbeat priority inversion causing spurious connection timeouts under sustained traffic.

### omq-libzmq 0.4.6

- Complete `zmq_setsockopt`/`zmq_getsockopt` coverage (all 124 options). Unknown options return `EINVAL`.
- `ZMQ_IPV4ONLY` support, `ZMQ_BLOCKY`/`ZMQ_STREAM_NOTIFY` stubs.

### blume 0.4.1, yring 0.3.1

- blume: recover from poisoned mutex in `Receiver::close()`/`drop()`.
- yring: explicit `consumer_dropped`/`producer_dropped` flags replace `Arc::strong_count`. Release consumed positions before parking.

### pyomq 0.12.3

- Dep bumps: omq-tokio 0.14.3, lz4rip 0.5.2.

## 2026-06-10

### Removed

- `priority` feature and `ConnectOpts`/`Socket::connect_with`. The
  feature was unused by any downstream consumer and doubled the routing
  architecture (145 cfg markers across 32 files). Can be re-introduced
  with a cleaner design if demand materializes.

### omq-proto 0.16.0

- **Breaking:** `MechanismSetup` variants renamed (`keypair` ->
  `our_keypair`); `MechanismConfig` merged into `MechanismSetup`.
  `MechanismSetup` is now `#[non_exhaustive]`.
- **Breaking:** `Options` gains new fields: `arena_threshold`,
  `wire_slot_cap`, `compression_offload_threshold`, `xpub_nodrop`.
- **Breaking:** `MonitorEvent` discriminant values changed
  (`PeerCommand` 7 -> 11, `Closed` 8 -> 12).
- **Breaking:** `SendCategory::Exclusive` variant added.
- **Breaking:** `ConnectOpts` module removed.
- **Breaking:** `encode_message_prefixed_gather` and
  `encode_message_gather` removed (replaced by `EncodedQueue`
  entry-based encoding).
- `EncodedQueue` moved from backends into `omq-proto`. Entry-based
  arena encoder: frame headers always go into the arena, small messages
  (< `ARENA_THRESHOLD` = 96 KiB) are contiguous (1 iovec per batch),
  large payloads tracked as external `Bytes` entries (zero-copy
  gather-write). Arena capacity increased from 128 KiB to 256 KiB.
- `SubscriptionSet::is_subscribe_all()` for PUB subscription elision.
- `EncodedQueue::push_shared_chunks()` and `push_pre_encoded()` for
  encode-once fan-out.
- Cache-line-aligned inline thresholds: `Message` inlines up to 55 B
  (was 39 B), `Payload` up to 62 B (was 38 B). Both are 64 B (one
  cache line). Eliminates the 39-to-40 B throughput cliff (+35% at
  40 B).
- `Message::from_slice(&[u8])` for zero-alloc inline construction of
  small messages (up to 55 B). No heap allocation, no refcounting.
- BLAKE3ZMQ ported to `chacha20-blake3` crate (`Session20` API).
  `SessionKeys` fields renamed to separate enc/auth keys.
- LZ4 compression: replaced `lz4-sys` (C FFI) with `lz4rip` (pure
  Rust). No C compiler required for the `lz4` feature.
- WebSocket fast paths: `try_advance_ready_ws()` for recv,
  `encode_and_push_flat_ws()` for send. ~3x throughput improvement
  on the WS hot path.
- 10 ms safety-net timers on all notification-based await points to
  prevent indefinite hangs from lost wakeups.

### omq-tokio 0.14.0

- **Breaking:** `DirectIo` module removed, replaced by
  `PeerWireSlot`. `Socket::connect_with` removed. `InboundFrame` and
  `InprocPeerSnapshot` moved to `omq-proto`. `DriverHandle` gains
  private `wire_slot` field. `DriverCommand::SendEncoded` variant
  added.
- PeerWireSlot: per-peer `EncodedQueue` under `std::sync::Mutex`
  (nanosecond hold time, encode only). The handle encodes ZMTP frames
  into the slot; the driver flushes to the wire via a
  `transmit_notify` select arm. Eliminates all pump tasks for fan-out
  and identity routing. Signal coalescing via `pending: AtomicBool`
  gates `notify_one()`.
- `PeerSend` enum (`Wire`/`Inbox`) dispatches fan-out, identity, and
  exclusive strategies to per-peer slots without pump tasks.
- Exclusive routing strategy for PAIR/CHANNEL sockets.
- Fan-out (PUB/XPUB/RADIO): encode message once via `pre_encode()`,
  push shared chunks into each matching peer's slot. Per-peer encoding
  eliminated for non-encrypted transports.
- PUB/SUB subscription elision: skip the Trie lookup when all peers
  are subscribe-all.
- Read-path zero-copy: `BytesMut` + `read_buf` replaces
  `Vec<u8>` + `Bytes::copy_from_slice`. 100-150% throughput gain at
  64 B through 4 KiB.
- REQ send bypass: `TypeState` shared via `Arc<Mutex<TypeState>>`,
  `Socket::send` locks inline and pushes through `SendSubmitter`.
- REQ recv bypass: driver pushes directly to `recv_tx`;
  `Socket::recv` applies `post_recv_req_direct` inline.
- Atomic REQ alternation flag: `AtomicBool` replaces the shared
  `Mutex<TypeState>` for REQ sockets. Saves ~200 ns per send+recv
  pair.
- Specialized `try_recv` fast path for PULL/PAIR: direct
  `cache.pop_front()` then lock + `swap_messages` + pop. No function
  calls, no `Result` wrapping. `try_recv` self-time dropped from 29%
  to 15%.
- `ChunkedInputBuf` front-cache: front chunk pulled out of `VecDeque`
  into a dedicated `front: Bytes` field. `peek_frame_header` dropped
  from 12.3% to 10.1%.
- Inproc recv_direct: `spawn_inproc_peer` passes `recv_tx` directly
  to the inproc driver, bypassing the actor.
- Configurable `arena_threshold` and `wire_slot_cap` per socket.
- Fix: lost-wakeup race and hang on inproc peer exit in recv.
- Fix: stale `identity_to_slot` entries after driver exit (47
  reconnect tests added).
- Fix: silent message loss, WS mechanism panic, and frame size
  truncation.
- Fix: `PeerSend` falls back to driver inbox when encode slot is
  ineligible.
- Fix: flush encode slot on cancel, fix `FanOut` per-message
  allocation.
- Fix: remove `DIRECT_MSG_MAX` to prevent wire_slot message
  reordering.
- Fix: `SO_REUSEADDR` set on TCP listener sockets.
- Fix: free inproc names from registry on `signal_close`.

### omq-libzmq 0.4.3

- Port from omq-compio to omq-tokio backend.
- Direct yring recv bypass: `ConnectionDriver` pushes decoded messages
  directly into the yring and signals the eventfd. One thread crossing
  instead of three. 8 B TCP: 1.1M -> 4.7M msg/s (4.3x).
- `send_accum` Mutex replaced with `UnsafeCell`; `send_ring` RwLock
  guarded by `AtomicBool` flag for TCP sockets.
- Yield every 64 msgs or 1 MiB sent to prevent starvation.
- `XPUB_NODROP` socket option.
- Fix: inproc bypass recv hang on multipart messages.
- Fix: inproc bypass recv starvation and blocking send.
- Harden FFI layer against panics with SAFETY comments.

### blume 0.4.0

- **Breaking:** `Receiver` is no longer `Sync` or `RefUnwindSafe`.
  Internal `Mutex` in the consumer cache replaced with `RefCell` for
  single-threaded consumers (matches the blume MPSC contract).

### yring 0.3.0

- **Breaking:** `Producer::flush()` and `AsyncProducer::flush()` now
  return `()` (was `bool`).
- Producer-side backpressure for async SPSC ring.
- `flush()` reduced to a single Release store (was Acquire + Release).
- Batch consumer pops with deferred Release store in `release()`.
- Deduplicate sync/async ring operations into shared `Ring<T>`.

## [0.2.14] - 2026-05-25

### pyomq 0.10.3

- Fix: `destroy_socket()` now cancels its pump tasks before attempting `Rc::try_unwrap`, releasing the `Rc<InnerSocket>` clones the pumps held. Previously sockets lingered as zombies, retaining queue memory until `ctx.term()`.
- Fix (omq-compio): evict stale `identity_to_slot` entries when a dialer reconnects. Each reconnect previously leaked one map entry, producing steady RSS growth under high reconnect rates.

## [0.2.13] - 2026-05-25

### pyomq 0.10.2

- Fix: `pyproject.toml` version was not bumped in 0.10.1.

## [0.2.12] - 2026-05-25

### pyomq 0.10.1

- Suppress `dead_code` warnings from PyO3 proc-macro call sites.

## [0.2.11] - 2026-05-25

### omq-tokio 0.12.1

- Fix: copy read buffer on compression transports to prevent buffer retention across reads.

### omq-libzmq 0.4.1

- Fix: eliminate TOCTOU race in IPv6 test port allocation (bind to `:0`, read actual endpoint).
- Package renamed from `omq-zmq` to `omq-libzmq`. Library name (`omq_zmq`) unchanged.
- 7 formerly-ENOTSUP socket options now store and round-trip values. 13 rarely-used options explicitly accepted as no-ops.
- `zmq_msg_get`/`zmq_msg_sets` improved for libzmq compatibility. Context options expanded.

### omq-zeromq 0.7.0

- `Socket::disconnect()` method.

### omq 0.12.1

- Dependency version bumps.

## [0.2.10] - 2026-05-20

### omq-tokio 0.8.1

- Route compression encoder output through `EncodedQueue` for batched writes. Up to 15x faster on compression transports.

### omq 0.8.1, omq-zeromq 0.3.3

- Dependency version bumps.

## [0.2.9] - 2026-05-20

### omq-proto 0.10.0

- `Options::compression_level(i32)` to configure zstd compression level.

### omq-compio 0.8.0, omq-tokio 0.8.0, omq 0.8.0, omq-zmq 0.1.4, omq-zeromq 0.3.2, pyomq 0.5.0

- Dependency version bumps.

## [0.2.8] - 2026-05-17

### omq-proto 0.8.1, blume 0.2.1, omq-zeromq 0.2.2

- Doc comments on all public API items for docs.rs coverage.

### omq-compio 0.5.2, omq-tokio 0.5.2, omq 0.5.1, omq-zmq 0.1.2

- Dependency version bumps only.

## [0.2.7] - 2026-05-14

### omq-proto 0.8.0

- `DisconnectReason::Handover` variant for ROUTER/SERVER identity handover.

### omq-compio 0.4.0

- ROUTER/SERVER identity handover: new connection with duplicate identity
  evicts the old peer.

### omq-tokio 0.4.0

- ROUTER/SERVER identity handover: new connection with duplicate identity
  evicts the old peer.

### omq 0.4.0

- Bump `omq-compio` to 0.4.0, `omq-tokio` to 0.4.0.

## [0.2.6] - 2026-05-12

### omq-proto 0.4.0

- **Breaking:** Remove `Deref<Target=[u8]>` and `From<Message> for Bytes`.
  Use `msg.get(i)` or `&msg[i]` for zero-copy `&[u8]` frame access;
  `msg.part_bytes(i)` for owned `Bytes`.
- **Breaking:** Remove `Payload` from public API. `PayloadInner::Multi`
  removed — all payloads are now guaranteed contiguous.
- `Payload::as_slice()` returns `&[u8]` (was `Option<&[u8]>`).
- `ChunkedInputBuf::split_to()` coalesces when spanning chunk boundaries
  instead of producing multi-chunk payloads.
- New: `Message::get(index) -> Option<&[u8]>` — checked zero-copy frame access.
- New: `impl Index<usize> for Message` — `&msg[0]` returns `&[u8]`, panics on OOB.
- Fixed: account for per-part overhead in `max_message_size` check. Zero-length
  MORE frames no longer bypass the limit.
- Fixed: reject oversized frames at header parse time.
- Fixed: `Options::authenticator` is `#[track_caller]`; panics point to the call site.
- Perf *(blake3zmq)*: stack-allocate 9-byte AAD buffer instead of `Vec` per frame.
- Security *(blake3zmq)*: `Session` key and nonce zeroized on drop.

### omq-compio 0.2.12

- Fixed: publish `MonitorEvent::HandshakeFailed` when pending messages are dropped
  after a failed ZMTP handshake. Previously the drop was silent.
- Adapted to `omq-proto` 0.4.0 Message API changes.

### omq-tokio 0.2.7

- Bump `omq-proto` to 0.4.0.

### pyomq 0.2.4

- Bump `omq-compio` to 0.2.12 and `omq-proto` to 0.4.0.

## [0.2.5] - 2026-05-09

### omq-compio 0.2.10

- Pin `blume = { version = "0.1.0" }` for crates.io publish. No code change.

## [0.2.4] - 2026-05-09

### omq-proto 0.3.0

- **Breaking:** `Connection::handle_input` now takes `Bytes` instead of
  `&[u8]`. Callers with a slice use `Bytes::copy_from_slice`; callers with
  an already-owned `Bytes` pass it directly with no copy.
- Codec inbound buffer replaced with `ChunkedInputBuf`: received bytes are
  appended as owned `Bytes` chunks without copying; the frame decoder slices
  into them directly. Eliminates O(n log n) `BytesMut` reallocation for
  large messages.
- New `Options::large_message_threshold(n)` /
  `Options::disable_large_message_path()`: tune the frame size at which
  io_uring backends switch from multi-shot to a sized one-shot read
  (default 128 KiB).
- New `Connection` API for direct-recv backends:
  `peek_next_frame_payload_size`, `begin_supplied_payload`, `supply_payload`.

### omq-compio 0.2.9

- Large-frame one-shot recv: for frames whose wire payload exceeds
  `large_message_threshold`, the multi-shot SQE is cancelled, any in-flight
  CQEs are drained, and the remainder is read directly into one contiguous
  `BytesMut`. Zero userspace memcpy on the long tail of the payload.
- Buf-ring: each slot is now copied into a `Bytes` and the `BufferRef`
  dropped immediately so the slot returns to the pool; `handle_input(Bytes)`
  is called directly. Replaces the old `BytesMut::extend_from_slice` path.

### omq-tokio 0.2.4

- `Options::large_message_threshold` / `disable_large_message_path` accepted
  for API parity with omq-compio; no effect on tokio (no buf-rings).
- Codec inbound buffer is now a chunked queue (same change as omq-proto 0.3.0
  delivers); large messages see one copy per read instead of O(n log n).

### pyomq 0.2.0

- First PyPI release. Python binding for omq-compio (compio/io_uring backend).
  Linux x86_64 and aarch64 wheels; stable ABI covers Python 3.9+.

## [0.2.0] - 2026-05-04

First public release. Three-crate Rust ZeroMQ implementation, wire-compatible
with libzmq. compio is the primary backend; omq-tokio is the cross-runtime
alternative.

### Crates

- **omq-proto**: sans-I/O ZMTP 3.1 codec, message types, mechanism state
  machines, message-transform layer, routing, options builder. No async,
  no I/O, no traits on the hot path.
- **omq-compio**: io_uring socket driver. Single-threaded runtime; spawn
  multiple runtimes for multi-core workloads.
- **omq-tokio**: multi-thread tokio backend. Identical public Socket API.

### Bindings

- **pyomq** (`bindings/pyomq`): PyO3 wrapper over `omq-compio`. Sync API
  plus an `asyncio`-compatible bridge. Built out-of-tree via maturin
  (excluded from `cargo build --workspace`).

### Socket API

- `Socket::new`, `bind`, `connect`, `unbind`, `disconnect`, `send`, `recv`,
  `try_send`, `try_recv`. Identical signatures across both backends.
  `try_send` / `try_recv` are synchronous and non-blocking: they return
  `Err(Error::WouldBlock)` immediately rather than suspending the task.
  `Error::WouldBlock` is the new variant in `omq-proto::Error`.
- `Socket::connect_with(endpoint, ConnectOpts)` (gated `priority` feature)
  for strict per-pipe priority on round-robin patterns.
- `Socket::join` / `Socket::leave` for DISH (RFC 48).
- `Socket::monitor()`: socket-like `Stream` with owned `PeerInfo` context
  on every event.
- `Endpoint` enum with `Display` / `FromStr` round-trip.

### Socket types

Standard (RFC 28 + RFC 47): PUSH, PULL, PUB, SUB, XPUB, XSUB, REQ, REP,
DEALER, ROUTER, PAIR. Group/transport drafts: RADIO, DISH (RFC 48). Draft
RFC stubs: CLIENT/SERVER (RFC 41), SCATTER/GATHER (RFC 49), CHANNEL
(RFC 51), PEER, RAW.

### Transports

- `tcp://` (IPv4 + IPv6).
- `ipc://` (Unix domain sockets, filesystem or abstract namespace).
- `inproc://` (process-local lock-free channel; process-wide registry).
- `udp://` (RADIO/DISH, RFC 48 datagram framing).
- `lz4+tcp://` (gated `lz4`, optional pre-trained dictionary).
- `zstd+tcp://` (gated `zstd`, optional static or auto-trained dict).

### Mechanisms

- **NULL** (default): plaintext, no handshake-time auth.
- **CURVE** (gated `curve`, RFC 26): Curve25519 box per data frame.
- **BLAKE3ZMQ** (gated `blake3zmq`): omq-native AEAD; X25519 + BLAKE3 +
  ChaCha20 + BLAKE3 MAC.

### Cargo features

All opt-in. Default build is the smallest deploy: NULL + TCP/IPC/inproc/UDP,
no C compiler required. Features: `curve`, `blake3zmq`, `lz4`, `zstd`,
`priority`. See `README.md` for the table.

### Options

Typed builder over `Options`. `ReconnectPolicy::default()` is `Fixed(100ms)`
matching libzmq's `ZMQ_RECONNECT_IVL`; `Exponential { min, max }` is opt-in.
`OnMute` controls send-HWM behavior: `Block` (default), `DropNewest`,
`DropOldest`. Defaults differ from libzmq in two places: per-socket HWM
semantics, conflate restricted to FanOut patterns.

### Performance

See [COMPARISONS.md](COMPARISONS.md).

### Conventions

- Rust 2024 edition, MSRV 1.93.
- Workspace lints: rust `missing_debug_implementations` denied; clippy
  `pedantic` warned with noisy rules silenced.
- ASCII-only source.
- `Cargo.lock` deliberately untracked (this is a library workspace).
