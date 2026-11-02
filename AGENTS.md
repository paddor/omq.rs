# AGENTS.md

## Workspace layout

Seven-crate Cargo workspace; `bindings/` and `pure/` are excluded and built
out-of-tree (maturin, npm, etc.).

- **`omq-proto`** -- sans-I/O ZMTP 3.x core. Codec (`Connection`),
  message/payload types, greeting + frame state machines, mechanism
  handshakes (NULL / PLAIN / CURVE), compression transforms
  (lz4, zstd), endpoint parsing, options, subscription matcher. No async,
  no I/O.
- **`omq-tokio`** -- tokio backend. **Default backend.**
  Works on Linux, macOS, and Windows.
- **`omq-shm`** -- Linux shared mappings, payload storage, remote leases,
  and ancillary IO. Isolates unsafe SHM operations from the core/backend.
- **`omq-libzmq`** -- libzmq-compatible C interface (`libomq_zmq`
  dynamic/static library). Drop-in replacement: ships `zmq.h`,
  implements the `zmq_*` API.
- **`omq-bench`** -- benchmark runner and SVG chart generator. Drives
  cross-implementation peers and reads/writes append-only JSONL data in
  `~/.cache/omq/`.
- **`vendor/omq-webtrans-proto`**, **`vendor/omq-webtrans-quinn`** --
  MIT-licensed WebTransport over HTTP/3 code adapted from `webtrans` 0.6.0
  for the `webtransport` feature.
- **`bindings/`** -- language bindings over `omq-tokio` or `omq-libzmq`
  ([`doc/bindings.md`](doc/bindings.md)).
- **`pure/`** -- OMQ implementations in other languages that speak ZMTP
  without the Rust engine. `pure/typescript` is OMQ.ts, the browser client
  (`ws://`, `wss://`, `lz4+ws://`, WebTransport); its interop fixture builds
  against the workspace `omq-tokio`.

The queue crates `yring` and `fanring` are external dependencies maintained
in [fanring.rs](https://github.com/paddor/fanring.rs). Queue tests, Loom,
Miri, and queue releases belong to that workspace.

`omq-tokio` re-exports `omq-proto`'s public API. Its public `Socket` API
is covered by `tests/coverage_matrix.rs`.

## Cargo features

| feature | adds | deps |
|---------|------|------|
| `plain` | PLAIN auth (RFC 24) | - |
| `curve` | CURVE handshake (RFC 26) | `salsabox`, `x25519-dalek` |
| `lz4` | `lz4+tcp://` transform | `lz4rip` |
| `zstd` | `zstd+tcp://` transform | `zrip` |
| `ws` | `ws://` / `wss://` WebSocket transport | `rustls`, `rustls-native-certs` (backend-level) |
| `quic` | `quic://` raw OMQ over QUIC (TLS 1.3, ALPN `omq-zmtp/1`) | `quinn` (backend-level) |
| `webtransport` | `quic://` listeners also accept WebTransport over HTTP/3; browsers use `https://` | `omq-webtrans-quinn`, `omq-webtrans-proto` (backend-level) |
| `dart` | `dart://` reliable ordered messages with fragmentation | `quinn-udp`, Linux `nix` (backend-level) |
| `fuzz` | fuzz test suites | - |
| `soak` | soak test suites | - |
| `copy-stats` | per-site message copy counters for `omq_copy_budget` | - |

## Architecture

[`doc/architecture.md`](doc/architecture.md) covers components, threads, and
message paths; [`doc/bindings.md`](doc/bindings.md) covers bindings. Transport
RFCs: [`lz4`](doc/lz4-rfc.md), [`zstd`](doc/zstd-rfc.md),
[`quic`](doc/quic-rfc.md), [`dart`](doc/dart-rfc.md). SHM:
[`omq-shm/README.md`](omq-shm/README.md). Key types:

- `Connection` (omq-proto): sans-I/O ZMTP codec
  (`handle_input`/`poll_event`/`send_message`/`poll_transmit`).
- `FrameBuffer`: arena (16 KiB TCP/WS, 64 KiB IPC) plus entries. Headers and
  messages under `ARENA_THRESHOLD` (4 KiB) go into the arena; larger payloads
  go out by gather write. Used by omq-tokio and omq-shm.
- `Message` and `Payload`: 64 B, inline up to 55 B and 62 B.
- `SocketDriver`: per-socket actor owning the peer table and type state.
- `SendSubmitter`: send path for every socket type, bypassing the actor.
- `PeerTransmitSlot`: per-peer `FrameBuffer` under a short-held
  `std::sync::Mutex`; handles frame, drivers flush. Latency-profile TCP may
  write once directly through `DirectTcpWriter`.
- `PeerOutbound` (`Wire`/`Inbox`/`Inproc`): per-peer dispatch without pump
  tasks.
- `ConnectionDriver`: per-connection wire task. Receives bypass the actor
  except ROUTER and REP.
- `LaneWorker`: fan-out lane with a `ControlQueue` drained every iteration and
  a `yring` data ring bounded by `DrainBudget`.
- PEER: one socket-owned fanring receiver; outbound per-clone,
  per-destination fanring producers.
- Inproc: one `yring` per direction (`send_hwm` + `recv_hwm`) owned by the
  receiver behind an `InprocPort`; senders push from the calling thread
  through `InprocSender`. `inproc_peer_driver` carries commands and lifecycle,
  plus relays for PEER and `conflate`.
- Connect-side round-robin sockets get a pre-ready send pipe at `connect()`;
  bind-side ones without a ready peer mute.

## Build / test / bench / charts / releasing

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for build, test, stress test, and
CI commands, [`RUNNING_BENCHMARKS.md`](RUNNING_BENCHMARKS.md) for benchmarks
and chart generation, and [`RELEASING.md`](RELEASING.md) for fuzz, soak, and
release steps.
Benchmark results are collected append-only in `~/.cache/omq/*.jsonl`.

Quick reference:

```sh
cargo build --workspace
cargo fmt                                # required before commit
cargo clippy --workspace --all-targets   # required before commit
./scripts/test-all.sh                    # full sweep + local perf gate
OMQ_SKIP_PERF=1 ./scripts/test-all.sh    # skip local perf gate
```

**HARD RULE:** Clippy must pass under all three configurations before
pushing to GitHub. Never push code that produces clippy warnings or
errors. Run all three before every `git push`:

```sh
cargo clippy --workspace --all-targets                # default features
cargo clippy --workspace --all-targets --all-features # feature-gated paths
(cd bindings/pyomq && cargo clippy --all-targets)     # separate workspace
```

Fix warnings instead of silencing them. For a demonstrated false positive,
use the narrowest `#[expect(lint, reason = "...")]`. Use `#[allow]` only
when the lint fires in some feature combinations but not others (the
expectation would be unfulfilled when the lint is silent).

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Conventions

- Run benchmarks and profilers on this host only. Never run them over SSH.
- Keep benchmark HWM independent of batch limits and pool capacity. SHM
  charts use HWM 1000.
- **HARD RULE: Never create documentation files unless the user explicitly
  asks to create them.** Feature work does not authorize new docs; update
  existing files instead.
- Add new charts only when explicitly requested.
- `doc/` documents how OMQ works. Development and benchmark notes belong in
  `DEVELOPMENT.md` and `RUNNING_BENCHMARKS.md`.
- **HARD RULE: No walls of text.** Keep documentation brief and technical.
  README and architecture feature summaries: at most three short sentences,
  plus examples and links. Do not repeat reference docs or add implementation
  walkthroughs.
- Never auto-add one-off feasibility or theoretical-ceiling benchmarks to Git
  history. Keep them local unless the user explicitly requests versioning.
- `doc/architecture.md` describes current components, ownership, and data flow.
  Rewrite existing explanations when architecture changes. No change history,
  benchmark results, verification notes, tuning constants, or implementation
  walkthroughs.
- Rust 2024 edition, MSRV **1.93**. ASCII-only source.
- `main` branch is protected. All changes go through PRs.
- Commit messages describe OMQ changes and checks only. Do not mention
  downstream applications.
- `CHANGELOG.md`: extend `## [Unreleased]` only. Never edit a released
  section.

## Code quality

API design:

- Keep public APIs small and hard to misuse. Default to private or
  `pub(crate)`. Do not leak implementation types or private errors in
  public signatures.
- Take borrowed inputs (`&str`, `&[u8]`, `&T`) when ownership is not
  needed. Take `impl Into<...>` where the callee keeps the data, so callers
  hand over a buffer without a copy.
- Prefer standard traits (`From`/`TryFrom`, `Default`, `Display`,
  `FromStr`, `AsRef`). Add an inherent `new`, `get` or `to_*` only when no
  trait fits.
- Document every public item, with `# Errors` and `# Panics` where they
  apply. Make functions `const` where possible.

Code shape:

- One concern per module. Split a source file past about 800 lines (tests
  excluded) into submodules along its concerns. Unit tests go in a sibling
  `tests.rs`.
- Functions stay under 60 lines and 5 parameters (`clippy.toml`). When
  arguments travel together, pass a named struct. A flat `match` with one
  arm per command or socket type may `#[expect(clippy::too_many_lines)]`.
- Group struct fields that change together into a named sub-struct. A
  struct with more than about 12 fields usually hides one.
- Logic repeated across socket types, transports or crates lives in one
  shared helper, not in copies.
- Off the hot path, plain readable code beats clever code.

Safety and errors:

- `omq-proto` and the `omq-tokio` library forbid `unsafe`. `omq-shm`
  (mappings, ancillary IO) and `omq-libzmq` (C ABI) hold the unsafe code:
  keep each block small, with a `// SAFETY:` comment.
- Library errors use `thiserror`. Public error enums are `#[non_exhaustive]`
  and keep their source chain with `#[from]`/`#[source]`.
- No `unwrap`, `expect` or panic in production code for recoverable
  failures. Peer input never panics. Check lengths and limits before
  allocating.

Tests:

- Every new or changed function is exercised by a test. Unit tests sit next
  to private logic; public behavior goes in `tests/`. Name tests by
  behavior.
- Add a regression test before fixing a bug.
- Wait on events with a timeout, not on sleeps.

## Socket fundamentals

The user-facing crash course is [`GETTING_STARTED.md`](GETTING_STARTED.md).
These rules bind every change. Reliability is non-negotiable: never propose a
fix that weakens self-healing or trades reliability for convenience.

- Send/recv never fail because of peers. Only misuse (protocol violations) or
  a closed socket return errors.
- Connect-before-bind works; never blame connection order.
- Sockets reconnect with backoff; applications never manage connections.
- Heartbeats detect dead peers, not slow ones. Never expect them to resolve
  slow-consumer backpressure.
- Messages are atomic: delivered whole or not at all.
- HWM means backpressure, not errors. Fan-out (`PUB`, `XPUB`, `RADIO`) drops
  for mute peers unless `xpub_nodrop`; round-robin and exclusive sockets block
  per `OnMute`.
- Fast peers never starve slow ones; slow peers never block fast ones.
  Receives fair-queue across connections.
- Control commands (subscribe, cancel, add/remove peer, shutdown) use their
  own channels, drained every iteration, never behind a data backlog.
- One socket binds several transports at once; inproc has no kernel or
  serialization.
- omq-tokio sockets can be shared across tasks; omq-libzmq keeps one socket
  per thread.

20 socket types in 10 pairs, checked during the handshake:

| Pair | Semantics |
| --- | --- |
| PUSH/PULL, SCATTER/GATHER | Round-robin out, fair-queue in; SCATTER/GATHER are single-part |
| PUB/SUB | PUB filters by subscribed prefix per subscriber |
| XPUB/XSUB | Subscriptions surface as messages; used for proxies |
| RADIO/DISH | Exact groups (`Message::with_group`, `join`/`leave`); UDP too |
| REQ/REP | Strict alternation; REQ adds an empty delimiter, REP saves and restores the envelope |
| DEALER/ROUTER | Free alternation; ROUTER prepends and routes by identity, errors on unknown identity with `router_mandatory`; a new peer with the same identity evicts the old one unless authenticated as a different principal |
| CLIENT/SERVER | Single-part DEALER/ROUTER; SERVER routes by `routing_id` |
| PAIR, CHANNEL | Exactly one peer; CHANNEL is single-part |
| PEER | N:N, identity routing in both directions |
| STREAM | Raw TCP only, no ZMTP; identity frame selects the peer |

## Concurrency

- No lost wakes: make a state change visible before signaling, and check
  the condition after registering the wait.
- Never hold a lock guard across `.await`. Keep lock holds short. Comment
  the order where two locks nest.
- Every spawned task, timer, waiter and registry entry has an owner that
  removes it on every exit path, including errors and cancellation. Prefer
  drop guards to manual cleanup.
- `select!` arms must be cancel-safe, or say why losing the branch is fine.
- Use the weakest correct atomic ordering, and comment why.
- Bound every queue and buffer a peer can fill, by count or bytes.

## Performance invariants

Profile before optimizing. Change measured hotspots. Back performance
claims with before and after numbers from release builds.

- Message paths do not allocate or copy per message. Amortize copies into
  arenas (`FrameBuffer`). Watch for hidden copies in `BytesMut::reserve`.
- Walk a collection once. Fuse filter, count and collect passes; use the
  entry API instead of `contains` then `insert`.
- Reuse buffers: `clear` keeps capacity. Use `Box<[T]>` or `[T; N]` for
  storage that never grows.
- Per-connection memory matters: keep idle peers small and box one-off
  futures.

**Caller thread: enqueue only.** The caller thread pushes raw
`Message` values into send pipes or lane rings. Encoding, framing,
compression, and wire I/O happen on driver or lane worker tasks.
Never add encoding or compression work to the caller's send path.

**Per-lane encoding and compression.** Fan-out lane workers encode
and compress independently. With owned `Context` IO threads, one
fan-out lane runs on each IO thread. Duplicating compression across
lanes is cheaper than serializing through a shared encoder mutex or
adding scheduling hops to a single compression worker.

**Compression dict: one shipment per direction per connection.** The
LZ4 and Zstd RFCs (doc/lz4-rfc.md and doc/zstd-rfc.md, Sec. 7.2) allow
at most one dictionary per direction per connection. A second shipment
closes the connection. Never re-ship a dict on an existing connection.

**Budget every drain loop.** Every loop that drains a channel or
queue must be capped by both message count AND byte count
(`DrainBudget`). Unbounded drains starve the tokio runtime and
other tasks.

**One wake per batch, not per push.** Producer-to-consumer signaling
uses `DataSignal` (`AtomicU8` idle/pending/draining/dirty state +
`Notify`). `mark()` wakes only on the idle-to-pending transition.
The consumer calls `begin_drain()` before draining and
`clear_after(is_empty)` after; it re-fires if data remains or a
producer marked during the drain. For budget-interrupted drains,
`reschedule()` wakes unconditionally. Never replace `DataSignal` with bare
`Notify::notify_one()` per push.
