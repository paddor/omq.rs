# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.26.0] - 2026-08-16

### Added

- `Message::part_slice()` for borrowing message parts without allocating
  `Bytes` for inline payloads.
- `Options::default_arena_threshold()` and `DEFAULT_ARENA_THRESHOLD` for
  restoring the default frame arena threshold after an override.
- WSS client trust configuration through `WssTls::trust_pem`,
  `WssTls::hostname`, and `WssTls::trust_system`.

### Changed

- **Breaking:** `WssTls` public struct literals must now include
  `trust_pem`, `hostname`, and `trust_system`. Prefer `WssTls::default()`
  for forward-compatible construction.
- WSS client TLS validation trusts the platform certificate store by default.
- Large receive docs now describe pooled medium-large buffers and one-shot
  owned buffers for larger frames.
- *(deps)* Bump `zrip` to 0.8.6.

## [0.25.1] - 2026-08-08

### Changed

- Add randomized coverage for chunked input buffer split, read, and advance
  behavior.

## [0.25.0] - 2026-07-31

### Added

- `zstd+tcp://` endpoint parsing and zstd frame transforms behind the
  `zstd` feature.
- `Options::compression_level()` for zstd transport compression levels.

### Security

- Remove `lz4+wss://`; compressed secure WebSocket is intentionally
  unsupported to avoid BREACH/CRIME-style misuse.

## [0.24.0] - 2026-07-27

### Added

- **Breaking:** Add `CurveServerOptions` for CURVE server configuration.
- Add `Options::max_pending_handshakes` to bound simultaneous inbound
  byte-stream handshakes. Lower values reduce pre-auth resource exposure but
  can reject legitimate connection bursts; higher values allow bigger bursts
  while consuming more memory/tasks.

### Changed

- **Breaking:** Replace the public CURVE server cookie keyring with a
  per-connection cookie key. `MechanismSetup::CurveServer` now stores
  `CurveServerOptions`, and `Options::curve_server_with_options()` accepts
  explicit CURVE server configuration.
- Encrypted mechanisms require `handshake_timeout`. Longer timeouts let slow
  peers complete authentication but also let stalled peers hold
  pending-handshake slots longer.
- Clarify that `Options::send_hwm` is a complete-message count, not a byte cap,
  and that native HWM is per outbound pipe/ring rather than one socket-wide
  cap.
- Clarify native zero-linger default and `linger_forever()` edge cases.

### Removed

- **Breaking:** Remove `CurveCookieKeyring`, `Options::authenticator()`, and
  `MechanismSetup::curve_cookie_keyring()` from the public API.

### Security

- Reject captured CURVE `HELLO` + `INITIATE` replays on fresh TCP
  connections. CURVE cookies are now bound to the connection that issued
  them and consumed when `INITIATE` is processed.

## [0.23.2] - 2026-07-23

### Added

- `Message::max_message_size_len()` for shared max-message-size accounting.

### Changed

- `Options::max_message_size` accounting now includes payload bytes plus one
  internal payload slot per part. This bounds zero-length multipart floods
  consistently across ZMTP and inproc transports.
- Clarify that native OMQ sockets default to zero linger.

## [0.23.1] - 2026-07-22

### Changed

- Remove the blanket 32-bit target rejection. 32-bit Linux targets with
  native 64-bit atomics are supported.
- Document and enforce platform allocation bounds for 64-bit ZMTP, WebSocket,
  and LZ4 declared lengths.

## [0.23.0] - 2026-07-19

### Added

- `Options::workload_profile` field for latency vs throughput tuning.
- `MechanismPeerInfo::identity` field exposes CURVE peer identity to
  the authenticator.
- `WorkloadProfile` enum.

### Removed

- **Breaking:** BLAKE3ZMQ mechanism removed entirely (experimental,
  non-standard). Use CURVE (RFC 26) instead. Removes the `blake3zmq`
  feature, all `Blake3Zmq*` types, `MechanismSetup::Blake3Zmq*`
  variants, `Options::blake3zmq_server`/`blake3zmq_client`, and the
  `MechanismName::BLAKE3` constant.
- **Breaking:** `Options::unbounded_send`/`unbounded_recv` removed.

### Fixed

- Bounded inproc backpressure: full rings apply backpressure instead
  of routing through a second producer path.

### Performance

- `Message` shrunk from 80 B to 64 B. Inline threshold reduced from
  71 to 55 bytes, trading 16 bytes of inline capacity for halved
  per-message memory on the hot path.
- `FrameBuffer` arena reduced from 256 KiB to 16 KiB (TCP/WS) and
  64 KiB (IPC). Lower per-connection memory footprint.
- Per-shard fan-out encoding: shard workers encode and compress
  independently, eliminating shared-encoder contention.
- Shared batch cap tuned from 1 MiB to 128 KiB.

### Changed

- **Breaking:** `FrameBuffer::with_arena_threshold` renamed to
  `FrameBuffer::with_config`. The user-facing `Options::arena_threshold`
  setter is unchanged.
- **Breaking:** `MechanismSetup` discriminant values changed due to
  removed BLAKE3ZMQ variants.
- *(deps)* Bump `lz4rip` 0.9 to 0.11.1.
- *(deps)* Bump `x25519-dalek` to 3.0.

## [0.21.0] - 2026-07-10

### Added

- Compile-time rejection of 32-bit targets. `omq-proto` is the root dependency, so the gate fires before crate-specific guards.
- `DrainBudget` type: caps every drain loop by message count and byte count, preventing unbounded drains from starving the async runtime.
- `DataSignal` type: coalesces producer-to-consumer wakeups. `mark()` fires `notify_one` only on the `false`-to-`true` transition; `clear()` before draining; `reschedule()` for budget-interrupted drains.
- `reconnect_stop_conn_refused` option: a connect endpoint stops reconnecting after a TCP connection-refused error instead of retrying indefinitely.

### Fixed

- Fix stale `arena_threshold` doc comments after the threshold was lowered.
- Suppress dictionary-shipment warning when building without `lz4`.

### Performance

- `Message` inline storage raised to 71 bytes (80-byte value, one cache line). Eliminates heap and refcount work for payloads up to 64 bytes.
- Arena threshold lowered from 96 KiB to 4 KiB. Messages at or above 4 KiB use zero-copy gather-write instead of copying the full payload into the arena.
- Arena peak capacity tracking: pre-reserve to the high-water mark after `split().freeze()`, eliminating the 256K/512K/1M/2M reallocation cascade.
- Wire slot cap lowered from 2 MiB to 512 KiB, closer to the kernel TCP send buffer. Reduces repeated partial `writev` calls.
- `FrameBuffer` direct-write path for arena-only batches: skip the per-batch `split`/`freeze`/`Bytes::slice`/drop cycle.
- Round-robin modulo elimination: wrapping comparison replaces integer division in `try_send`, `deactivate`, and `remove_peer`.
- Encode-once fan-out: distribute shared pre-encoded wire bytes to all PUB/XPUB/RADIO subscribers.
- Ship LZ4 fan-out dictionaries once per connection from a socket-level encoder.

### Changed

- Fan-out sockets (PUB/XPUB/RADIO) ignore `OnMute::Block`, matching libzmq behavior. `xpub_nodrop` remains direct-path only.
- Rename internal types: `EncodedQueue` to `FrameBuffer`, `FanOutBatch` to `FanOutFrame`, `DirectEncode` to `HandleFrame`, `WireSlot` to `PeerTransmitSlot`, `DropQueue` to `FallbackQueue`, `PeerSend` to `PeerOutbound`, `WireSlotCache` to `TransmitSlotCache`.
- Batch cap raised from 256 to 512 messages for small-message workloads.

## [0.20.0] - 2026-07-04

### Added

- IPC endpoint support on Windows (named pipes).

### Fixed

- Reconnect jitter truncation.
- Direct receive size guard hardening.
- Enforce message limits on codec fast paths.
- WebSocket handshake hardening.

### Removed

- `omq-compio` backend references.

## [0.19.0] - 2026-07-03

### Added

- `Payload` and `Message` now implement `PartialEq` and `Eq`.
- `Endpoint`, `Host`, and `IpcPath` now implement `Hash`.
- Shared `flow` and `direct_encode` modules for backend send-path policy.

### Fixed

- Cap pre-auth handshake and WebSocket frame input sizes.
- Reject LZ4M decompression bombs before allocation.

### Removed

- `InboundFrame::message()` constructor.

## [0.18.1] - 2026-06-27

### Fixed

- Validate PLAIN username/password length in `encode_hello`.
- LZ4M 2 GiB test slice bounds.

### Changed

- Use `as_chunks::<N>()` instead of `chunks_exact(N)` in Z85 and WS handshake.
- *(deps)* Bump `lz4rip` 0.8 to 0.9, `bytes` 1.11 to 1.12, `socket2` 0.6.3 to 0.6.4.

## [0.18.0] - 2026-06-26

### Added

- `lz4+ws://`, `lz4+wss://` compressed WebSocket endpoint variants.

### Fixed

- WS send bypass: encode as WS binary frames instead of raw ZMTP frames (caused silent message loss on bypass paths).
- WS upgrade: parse HTTP status code as integer instead of substring match.
- `clear_arena`: add `debug_assert` for external entries invariant.

### Changed

- `#![forbid(unsafe_code)]` crate-wide. Replace `MaybeUninit` arrays with `[0u8; N]`, `align_to_mut` with `as_chunks_mut` in WS masking.

### Removed

- `SocketType::is_draft()` method.

## [0.17.3] - 2026-06-22

### Fixed

- CURVE handshake nonce counter overflow now returns `Result` instead of panicking.

### Changed

- `EncodedQueue`: arena-only dispatch path for fan-out, writev delegation to `EncodedQueue::write_vectored`, IPC send/recv buffer sizing.
- Use `MaybeUninit::zeroed()` for inline message buffers.
- *(deps)* Upgrade `lz4rip` from 0.5.2 to 0.8.

## [0.17.2] - 2026-06-17

### Added

- `socket_ref` module: cross-platform `SocketRef` trait abstracting `AsFd` (Unix) and `AsSocket` (Windows) for socket option application.

### Fixed

- `Command::Error` encoding: truncate reason to 255 bytes instead of panicking on overlong reasons.
- Frame parser: reject frames exceeding `isize::MAX` allocation limit.
- CURVE handshake: detect and surface `ERROR` commands from the peer.

### Changed

- `Endpoint::Ipc` and `IpcPath` gated behind `#[cfg(unix)]` for Windows support.
- `Options::apply_socket_buffers` and `KeepAlive::apply` now take `impl SocketRef` instead of `impl AsFd`.
- `Options::compression_dict` setter no longer panics; validation deferred to `Options::validate()`.
- *(deps)* Upgrade `lz4rip` 0.4 to 0.5.2.

## [0.17.1] - 2026-06-12

### Added

- `Message::try_as_parts(&self) -> Option<(&[u8], &[u8])>`: zero-copy accessor that returns the first two frames as a tuple without allocating, or `None` if the message does not have exactly two parts.

### Changed

- `Options::compression_auto_train` now defaults to `false`. Auto-training adds per-connection overhead that only pays off for small structured records on bandwidth-constrained links. Enable explicitly with `.compression_auto_train(true)` when needed.

## [0.17.0] - 2026-06-12

### Added

- LZ4 auto-training: `Lz4Encoder` feeds outbound messages to `lz4rip::DictTrainer`, trains a dict after 100 messages, lowering the compression threshold from 512 B to 64 B. Controlled by `Options::compression_auto_train` (default: true).
- `TrySendError` enum in `omq_proto::error` for unified `try_send` error handling across backends.

### Removed

- **Breaking:** `zstd` feature and all associated types: `ZstdEncoder`, `ZstdDecoder`, `Endpoint::ZstdTcp`, `Options::compression_level`. `lz4+tcp://` with `lz4rip` (pure Rust, no C compiler) covers the compression use case with better small-message performance.

### Changed

- *(deps)* Upgrade `lz4rip` to 0.4.0. Tighten `subtle` to 2.6.

## [0.16.0] - 2026-06-10

### Added

- `EncodedQueue`: arena-based ZMTP frame encoder moved from backends to `omq-proto`. Entry-based arena (256 KiB capacity, 96 KiB `ARENA_THRESHOLD`), `encode_auto`/`encode_prefixed_auto` dispatch, `push_pre_encoded`/`push_shared_chunks` for encode-once fan-out, configurable `with_arena_threshold`.
- `InboundFrame`/`InprocPeerSnapshot` in `omq_proto::inproc` (unified across backends).
- `generated_identity()` in `omq_proto::message`, `supports_conflate()` in `omq_proto::routing`.
- `SendCategory::Exclusive` for PAIR/CHANNEL.
- `SubscriptionSet::is_subscribe_all()`.
- `Options::xpub_nodrop`, `Options::arena_threshold`, `Options::wire_slot_cap`, `Options::compression_offload_threshold`.
- Compression offload API: `MessageEncoder::can_offload`/`new_offload`/`sync_dict`.
- Monitor events: `SubscribeReceived`, `UnsubscribeReceived`, `JoinReceived`, `LeaveReceived`.
- `Connection::take_transform`/`restore_transform`/`emit_encrypted_frames` for per-peer encryption offload infrastructure.

### Fixed

- Frame size overflow DoS: saturating/checked arithmetic in `max_message_size` and `try_decode_frame`.
- BLAKE3ZMQ: wrap all DH intermediates in `Zeroizing<>`.
- CURVE: eliminate wasted entropy on state transitions (use `.take()` instead of `mem::replace` with throwaway `SecretKey`).
- Remove incorrect `unsafe impl Sync for Lz4Encoder`.
- `write_outbound_commands` propagates encrypt failures instead of silently dropping commands.
- WS: graceful close on mechanism start failure instead of panic.

### Performance

- `Message` inline threshold widened from 39 B to 55 B, `Payload` from 38 B to 62 B (both 64 B, one cache line). Eliminates 29% throughput cliff at 40 B.
- Arena capacity 128 KiB to 256 KiB.
- BLAKE3ZMQ: port to `chacha20-blake3` `Session20` API (stateful, no per-message KDF). Eliminate 5 `Vec` heap allocations per handshake.
- Eliminate per-message identity clone from inproc path.

### Changed

- Unify `MechanismConfig`/`MechanismSetup` into single `MechanismSetup` enum.
- LZ4: replace `lz4-sys` (C FFI) with `lz4rip` (pure Rust).
- Remove `priority` feature and `connect_opts` module.
- *(deps)* `lz4rip` 0.2.0, `chacha20-blake3` 0.10.0.

## [0.15.0] - 2026-05-30

### Added

- Configurable compression thresholds (`compression_level`, `compression_auto_train`).

### Fixed

- CURVE mechanism: carry the COMMAND flag in the encrypted inner byte.
- RFC 23 compliance violations in ZMTP greeting and property parsing.

### Performance

- Optimize 8 B TCP recv codec path.

### Changed

- *(deps)* Tighten `thiserror` to 2.0.18, `zeroize` to 1.8.0.

## [0.14.0] - 2026-05-25

### Added

- `routing` module: centralized socket-type-to-routing-strategy categorization (`SendCategory`, `RecvCategory`, `FanOutKind`).
- `Options::validate()` for ZMTP protocol-limit checks.
- `Message::prepend_empty_delimiter()` for REQ pre-send framing.
- `Error::Config` variant for configuration validation failures.

### Changed

- CURVE mechanism split into `CurveClient` and `CurveServer` for compile-time role enforcement.
- PLAIN mechanism split into `PlainClient` and `PlainServer`.
- Bypass per-message routing overhead on single-peer wire send.
- *(deps)* Upgrade `rand` 0.8 → 0.10.

## [0.13.0] - 2026-05-23

### Added

- Sans-I/O WebSocket codec (`WsCodec`): HTTP upgrade handshake, frame parser/encoder, mask key generation.
- `ZMQ_STREAM` socket type for raw TCP communication.
- WS `EncodedQueue` fast paths: server-side unmasked flat encode and client-side masked flat encode.
- Thread-local `SmallRng` for WS mask key generation (avoids per-message allocation).

### Fixed

- Skip ZMTP frame introspection APIs in WebSocket mode.

## [0.12.0] - 2026-05-21

### Added

- `Message::iter_parts` for allocation-free iteration over message parts.

### Changed

- `encode_message_flat` and `encode_message_prefixed_flat` use `iter_parts` instead of allocating a `Vec` via `parts_payload()`.

## [0.11.0] - 2026-05-21

### Changed

- CURVE mechanism hardened: server handshake is now stateless until the client cookie is verified, low-order public keys are rejected, and all key comparisons use constant-time equality.
- BLAKE3ZMQ: `SessionKeys` are zeroized on drop.
- *(deps)* `CurveSecretKey::derive_public()` added for pyomq `curve_public()`.

## [0.10.0] - 2026-05-20

### Added

- `Options::compression_level(i32)` to configure zstd compression level (default -3).
- `pub` visibility on `transform::zstd::DEFAULT_LEVEL`.

## [0.9.0] - 2026-05-19

### Added

- `SocketApi` trait formalizing the shared public API between compio and tokio backends.

### Changed

- `Connection` internals split into `connection/inbound.rs` and `connection/outbound.rs` for readability.

### Breaking

- `SocketApi::bind` returns `Result<Endpoint>` instead of `Result<()>`, providing the resolved endpoint (with actual port for wildcard binds) directly.

## [0.8.4] - 2026-05-18

### Added

- `Connection::transmit_chunks_capped`: like `transmit_chunks` but caps the number of iovecs returned, preventing `SmallVec` heap spill on large batches.

## [0.8.3] - 2026-05-18

### Fixed

- `Payload::from_slice`: fall back to heap allocation when an inline `Message` (39 B max) exceeds `Payload`'s inline limit (38 B). Previously panicked during compression transforms.

## [0.8.2] - 2026-05-17

### Changed

- *(deps)* Bump `socket2` to 0.6.

## [0.8.1] - 2026-05-17

### Added

- Doc comments on all public API items for docs.rs coverage.

## [0.8.0] - 2026-05-14

### Added

- `DisconnectReason::Handover` variant for ROUTER/SERVER identity handover.

## [0.7.0] - 2026-05-14

### Added

- PLAIN security mechanism (RFC 24): four-command handshake providing
  username/password authentication. Feature-gated behind `plain` with zero deps.

### Fixed

- Return error instead of panicking on overlong CURVE metadata properties.

## [0.6.0] - 2026-05-13

### Changed

- **Breaking:** `Connection::transmit_chunks()` returns `SmallVec<[IoSlice; 8]>`
  instead of `Vec<IoSlice>`, avoiding a heap allocation on typical flushes.
- **Breaking:** Post-handshake READY/ERROR commands are now rejected as protocol
  violations (ZMTP RFC 23), dropping the connection.
- REQ/REP `pre_send` uses `into_parts_payload()` to move payloads instead of
  double-cloning via `parts_payload()`.

### Fixed

- CURVE `cookie_key`, BLAKE3ZMQ `Keypair.secret`, and `CookieKeyring` keys are
  now zeroized on drop via `Zeroizing<[u8; 32]>`.
- `SessionKeys` `Debug` impl redacted — no longer prints session keys/nonces.

### Refactored

- BLAKE3ZMQ: replaced `Role` enum + `mem::replace` placeholder dance with flat
  `Option` fields, matching CURVE's pattern.

## [0.5.0] - 2026-05-13

### Changed

- **Breaking:** Rename `tcp_recv_buffer_size` / `tcp_send_buffer_size` to
  `recv_buffer_size` / `send_buffer_size` (they apply to IPC too).

## [0.4.0](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.2...omq-proto-v0.4.0) - 2026-05-12

### Changed

- **Breaking:** Remove `Deref<Target=[u8]>` and `From<Message> for Bytes`.
  Use `msg.get(i)` or `&msg[i]` for zero-copy `&[u8]` frame access;
  `msg.part_bytes(i)` for owned `Bytes`.
- **Breaking:** Remove `Payload` from public API. `PayloadInner::Multi`
  removed — all payloads are now guaranteed contiguous.
- `Payload::as_slice()` returns `&[u8]` (was `Option<&[u8]>`).
- `ChunkedInputBuf::split_to()` coalesces when spanning chunk boundaries
  instead of producing multi-chunk payloads.

### Added

- `Message::get(index) -> Option<&[u8]>` — checked zero-copy frame access.
- `impl Index<usize> for Message` — `&msg[0]` returns `&[u8]`, panics on OOB.

### Fixed

- Account for per-part overhead (`size_of::<Payload>()`) in `max_message_size`
  check. Zero-length MORE frames no longer bypass the limit.
- Reject oversized frames at header parse time instead of waiting for
  the full payload to arrive.
- `Options::authenticator` is now `#[track_caller]`; panics point to the
  call site instead of inside the library.

### Performance

- *(blake3zmq)* Stack-allocate 9-byte AAD buffer instead of `Vec` per frame.

### Security

- *(blake3zmq)* `Session` key and nonce are zeroized on drop via
  `ZeroizeOnDrop`. Key material no longer lingers in freed memory.

## [0.3.2](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.1...omq-proto-v0.3.2) - 2026-05-10

### Added

- *(lz4)* LZ4M multi-block encoding for parts larger than 1 GiB.
  The LZ4 block API caps a single call at ~2 GiB (`i32` parameters);
  LZ4M chunks the payload into 1 GiB blocks, each independently
  compressed and decompressed. Wire format: `LZ4M` sentinel
  (`4C 5A 34 4D`) + `u64 LE` total decompressed size + per-block
  `(u32 LE compressed_len | LZ4 block)` pairs. Parts at or below
  the block size continue to use the existing `LZ4B` single-block
  encoding. `Lz4Encoder` and `Lz4Decoder` gain `with_block_size()`
  to override the default for testing.

### Fixed

- *(curve)* Enforce monotonic nonce counter on incoming CURVE MESSAGE
  commands per RFC 26. Previously, any counter value was accepted,
  allowing replay of captured encrypted frames.

### Changed

- `Message::parts_payload()` returns `SmallVec<[Payload; 1]>` instead
  of `Vec<Payload>`, eliminating a per-send heap allocation for
  single-part messages (+8% on 8 B IPC throughput).

## [0.3.1](https://github.com/paddor/omq.rs/compare/omq-proto-v0.3.0...omq-proto-v0.3.1) - 2026-05-09

### Fixed

- *(blake3zmq)* raise `chacha20-blake3` floor to 0.9.12 (paddor/chacha20-blake3);
  fixes ARM CI (aarch64 build failure introduced in 0.9.11).

## [0.3.0](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.3...omq-proto-v0.3.0) - 2026-05-09

### Added

- `Options::large_message_threshold(n)` and
  `Options::disable_large_message_path()`: tune the wire-payload size at
  which compatible recv backends switch from a buffer-pool multi-shot
  read to a single sized one-shot read. Default: `Some(128 * 1024)`.
  Honoured by `omq-compio`; accepted but ignored on `omq-tokio` for API
  parity.
- New `Connection` API for direct-recv I/O backends:
  `peek_next_frame_payload_size`, `begin_supplied_payload`, and
  `supply_payload`. A backend that has decided to recv a large frame's
  payload directly into a sized buffer (instead of via the codec's
  inbound chunk queue) consumes the wire-frame header from the codec
  with `begin_supplied_payload`, recvs the payload bytes itself, and
  hands them back as one `Bytes` via `supply_payload`. The codec runs
  the same mechanism decrypt and demux path as it would on an
  in-buffer-assembled frame. Existing callers that never invoke the
  new methods are unaffected.

### Changed

- Codec inbound buffer replaced with a chunked queue (`ChunkedInputBuf`):
  received bytes are appended as owned `Bytes` chunks without copying,
  and the frame decoder slices into them directly. This eliminates the
  `BytesMut` reallocation chain that previously scaled as O(n log n) for
  large messages, cutting total copies to one per received chunk.
- `Connection::handle_input` now takes `Bytes` instead of `&[u8]`. Callers
  with a slice use `Bytes::copy_from_slice`; callers with an already-owned
  `Bytes` (e.g. from a buf-ring slot) pass it directly with no copy.
- `frame::try_decode_frame` and `greeting::try_decode` are now
  `pub(crate)` (they were never part of the stable public API).

## [0.2.3](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.2...omq-proto-v0.2.3) - 2026-05-05

### Fixed

- *(zstd)* dict shipment is now wire-compatible with `omq-zstd` Ruby.
  The encoder used to prepend `SENTINEL_DICT` ahead of a dict body that
  already begins with `ZDICT_MAGIC`, doubling the magic on the wire; the
  decoder then stripped 4 bytes, so Rust↔Rust round-tripped only by
  symmetric mistake. Ruby ships the dict raw (per RFC), so Ruby→Rust
  dict-aware decompress failed at the first compressed message after
  auto-train (~msg 256 at 100 KiB / 402 B). Encoder now ships
  `Message::single(dict)`, decoder stores the whole received bytes, and
  `ZstdEncoder::with_send_dict` requires the dict to start with
  `ZDICT_MAGIC` (mirrors Ruby's `install_send_dict`).

### Added

- *(zstd)* public `omq_proto::proto::transform::train_zdict(samples, capacity)`
  for callers that want to ship a static dict but only have a sample
  corpus to train from. Returns ZDICT-format bytes accepted directly by
  `Options::compression_dict` / `ZstdEncoder::with_send_dict`.

## [0.2.2](https://github.com/paddor/omq.rs/compare/omq-proto-v0.2.1...omq-proto-v0.2.2) - 2026-05-04

### Changed

- *(blake3zmq)* switch `chacha20-blake3` dep from a git-pinned fork to
  crates.io v0.9.11 (paddor/chacha20-blake3). The fork inlines the `chacha`
  subcrate and carries `#[target_feature(enable = "avx2/avx512")]`
  annotations; full AVX2 throughput (~1 GiB/s) requires
  `RUSTFLAGS="-C target-cpu=native"` at build time; scalar path runs
  ~55 MiB/s without it.

## [0.2.1](https://github.com/paddor/omq.rs/releases/tag/omq-proto-v0.2.1) - 2026-05-04

### Added

- add try_send / try_recv (non-blocking send/recv)

### Fixed

- Socket::drop cancels tasks; REQ/REP reset on disconnect; tier-1 tests
- *(clippy)* use is_multiple_of in z85 encode/decode guards
- *(curve)* wire-compatible with libzmq + pyzmq interop suite

### Other

- pre-release polish, American English, comprehensive test-all.sh
- tighten default size sweep to 128 B / 2 KiB / 8 KiB
- *(payload)* Phase C — as_bytes(), single-chunk transforms, PAYLOAD_INLINE_CHUNKS=1
- fix all clippy warnings across workspace (--all-features)
- *(transform)* split MessageTransform into MessageEncoder + MessageDecoder
- *(tokio)* FLAT_THRESHOLD 32 KiB — fix 2 KiB TCP regression; fix(compio): second codec_maybe_dirty race; bench: add 32B size, 8-peer column
- *(compio)* flat-buf encoding, drain-vec reuse, codec-skip guards, passthrough fast path
- *(proto)* O(1) pending_transmit_size via cached out_bytes_total
- cargo fmt --all (first-time sweep)
- *(benchmarks)* rerun at 0.5s/cell, MB/GB units, BLAKE3ZMQ audit caveat
- *(clippy)* silence all pedantic warnings across feature combos
- full Stage/Phase narrative sweep across all src/
- Rust ZMQ: omq-proto codec, omq-tokio/omq-compio backends, pyomq binding
