# CLAUDE.md

## Workspace layout

Six-crate Cargo workspace; `bindings/` is excluded and built
out-of-tree (maturin etc.).

- **`omq-proto`** -- sans-I/O ZMTP 3.x core. Codec (`Connection`),
  message/payload types, greeting + frame state machines, mechanism
  handshakes (NULL / PLAIN / CURVE / BLAKE3ZMQ), compression transforms
  (lz4), endpoint parsing, options, subscription matcher.
  No async, no I/O. Mirrors `rustls::ConnectionCommon` / `quinn-proto`.
- **`omq-tokio`** -- multi-thread tokio backend. **Default backend.**
  Works on Linux and macOS (and likely other mio targets).
- **`omq-compio`** -- single-threaded compio backend (io_uring on
  Linux, IOCP on Windows). Not available on macOS.
- **`blume`** -- batching MPSC channel for `omq-compio` inbound delivery.
- **`yring`** -- bounded SPSC ring buffer for inproc transport.
- **`omq-libzmq`** -- libzmq-compatible C interface (`libomq_zmq.so` /
  `.a`). Drop-in replacement: ships `zmq.h`, implements the `zmq_*`
  API. Backed by `omq-tokio`.
- **`bindings/pyomq`** -- PyO3 wrapper over `omq-tokio`. Own `Cargo.lock`.
  Build: `cd bindings/pyomq && maturin develop --release`.

Both backends re-export `omq-proto`'s public API and share an identical
public `Socket` API. Verified by `tests/coverage_matrix.rs` (both) and
`omq-tokio/tests/interop_compio.rs`.

## Build / test / bench

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for the full command reference
(unit tests, feature-gated tests, fuzz, soak, stress tests, benchmarks).

Quick reference:

```sh
cargo build --workspace
cargo fmt                                # pre-commit hook checks this
cargo clippy --workspace --all-targets   # pre-commit hook checks this
./scripts/test-all.sh                    # full sweep, both backends
```

**HARD RULE:** Clippy must pass under all three configurations before
pushing to GitHub. Never push code that produces clippy warnings or
errors. Run all three before every `git push`:

```sh
cargo clippy --workspace --all-targets                # default features
cargo clippy --workspace --all-targets --all-features # feature-gated paths
(cd bindings/pyomq && cargo clippy --all-targets)     # separate workspace
```

`#[allow]` vs `#[expect]`: use `#[expect]` by default. Use `#[allow]`
only when the lint fires in some feature combinations but not others
(the expectation would be unfulfilled when the lint is silent).

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Benchmarks, charts, releasing

See [`DEVELOPMENT.md`](DEVELOPMENT.md) for comparison benchmark infra,
chart generation, and release process.

**interop_compio dep constraint:** `omq-tokio/Cargo.toml`'s compio
dev-dep must use the same git rev as `omq-compio`'s dep. Different
revs link two `compio-runtime` instances -> TLS mismatch panic.

## Cargo features

| feature | adds | deps |
|---------|------|------|
| `plain` | PLAIN auth (RFC 24) | - |
| `curve` | CURVE handshake (RFC 26) | `crypto_box`, `crypto_secretbox` |
| `blake3zmq` | BLAKE3 + ChaCha20 mechanism | `blake3`, `chacha20-blake3`, `x25519-dalek` |
| `lz4` | `lz4+tcp://` transform | `lz4rip` |
| `ws` | `ws://` / `wss://` WebSocket transport | `rustls`, `rustls-native-certs` (backend-level) |
| `fuzz` | fuzz test suites | - |
| `soak` | soak test suites | - |

## ZMQ fundamentals

ZMQ sockets are opaque message queues that abstract away the network.
The user sends and receives messages. The socket handles connections,
reconnections, framing, and multiplexing internally. The transport
(TCP, IPC, inproc, UDP) is chosen by endpoint URI and is transparent
to the application.

**Reliability is non-negotiable.** ZMQ users expect the library to
Just Work. No errors from peer failures. No hangs. No stuck states.
Connections self-heal automatically. Back-pressure is applied silently.
The library must always take the optimal performance path and recover
from any transient failure without user intervention. A ZMQ library
that surfaces transport-level errors to the user, requires manual
reconnection, or gets stuck in a degraded state is broken. Never
propose a fix that weakens self-healing, adds user-visible failure
modes, or trades reliability for convenience.

Core guarantees that omq must uphold:

- **Send/recv never fail due to peers.** A peer disconnecting, a TCP
  connection dropping, or a slow consumer does not cause `send` or
  `recv` to return an error. The socket reconnects automatically and
  resumes delivery. The only user-visible send errors are protocol
  violations (e.g. REQ sending twice without recv) or socket closed.
- **Connect-before-bind works.** `connect()` queues internally and
  waits for the bind to appear. Never suggest connection ordering as
  a cause for failures or hangs.
- **Automatic reconnection.** ZMTP peers reconnect on disconnect
  with configurable backoff. The application does not manage
  connection lifecycle.
- **Messages are atomic.** A multipart message is delivered in full
  or not at all. No partial delivery.
- **HWM back-pressure, not errors.** When the outbound queue is
  full, the socket either drops (PUB default) or blocks (PUSH
  default, configurable via OnMute). It does not return an error.
- **Transport-agnostic.** The same socket can bind on TCP and IPC
  simultaneously. Inproc is in-process (no kernel, no serialization).
- **Subscriptions are prefix-matched.** SUB subscribes to byte
  prefixes. Empty prefix = all messages. PUB filters per subscriber.
- **Thread safety contract.** A single socket must not be used from
  multiple threads concurrently (ZMQ's rule). omq-tokio relaxes this
  for async (send/recv serialize internally), but the principle holds
  for omq-libzmq's C API.

## Performance invariants

Fan-out sockets (PUB, XPUB, RADIO) send the same message to many
peers. The wire bytes are identical for all peers on the same
transport when no per-peer encryption is active. **Encode once,
distribute the encoded bytes.** Never encode, compress, or frame the
same message N times for N subscribers. The correct pattern: encode
into a scratch buffer, then memcpy the pre-encoded bytes into each
peer's `EncodedQueue` via `push_pre_encoded`. This applies equally
to ZMTP framing, compression transforms (lz4), and any future
wire-level transforms. Per-peer encoding is only justified when the
wire bytes genuinely differ per peer (e.g. per-peer encryption keys).

## Architecture summary

Three-layer split: `omq-proto` (sans-I/O ZMTP codec) -> backend
(`omq-tokio` or `omq-compio`) -> user `Socket` API. Two queues per
socket: one inbound, one outbound. Per-connection driver tasks bridge
queues and wire. Full detail in `doc/`:
[`architecture.md`](doc/architecture.md),
[`tokio.md`](doc/tokio.md),
[`compio.md`](doc/compio.md),
[`performance.md`](doc/performance.md),
[`libzmq/`](doc/libzmq/).

**omq-proto key types.** `Connection`: ZMTP codec state machine
(`handle_input`/`poll_event`/`send_message`/`poll_transmit`).
`EncodedQueue`: arena (256 KiB) + entry-based encoder used by both
backends. Frame headers are always written into the arena. Small
messages (<96 KiB `ARENA_THRESHOLD`) go contiguously into the arena
(1 iovec per batch). Large payloads are tracked as external `Bytes`
entries (zero-copy gather-write). `Message`/`Payload`: 64 B
each (one cache line), inline variants (55 B / 62 B).

**omq-tokio hot path.** `SocketDriver` actor owns peer table and
type state. Send bypass: `Socket::send` skips actor for non-REQ/REP
via `SendSubmitter` (flume MPMC). Per-peer `PeerWireSlot`
(`EncodedQueue` under `std::sync::Mutex`, nanosecond hold): handle
encodes, driver flushes via `data_ready` select arm. `PeerSend` enum
(`Wire`/`Inbox`) dispatches fan-out/identity/exclusive to per-peer
slots without pump tasks. Recv bypass: `ConnectionDriver` pushes
straight to user `recv_tx` for PULL/SUB/REQ/etc. REP/ROUTER go
through actor for identity routing.

**omq-compio hot path.** Single-threaded, cooperative. `DirectIoState`
per wire peer: `EncodedQueueCell` (Cell-based borrow, no atomics) for
send bypass. `recv_claim: AtomicU8` arbitrates driver vs user owning
the read path (direct-recv). Multi-shot recv from io_uring `BUF_RING`
pool. Cell fields (`driver_in_select`, `handshake_done`) avoid atomic
overhead on the single-thread runtime.

**Inproc.** No ZMTP, no driver. Cross-thread SPSC via `yring`
(lock-free ring buffer, 64 B `Message` by value). Same-thread via
`blume` (batching MPSC, swap-drain consumer).

## Conventions

- Rust 2024 edition, MSRV **1.93**. ASCII-only source.
- `rustfmt.toml`: `edition = "2024"`, `max_width = 100`.
- `Cargo.lock` untracked at workspace root (library). `bindings/pyomq`
  ships its own.
- Per-crate versioning, tags are `<crate>-v<version>`.
- `main` is protected. All changes go through PRs.

## Chart generation

**HARD RULE:** Chart subtitle configuration lives in `.chart_hw`
(gitignored, repo root). All `scripts/gen_*_chart.py` scripts read
it automatically via `scripts/chart_hw.py`. Never run chart gen
scripts without verifying `.chart_hw` exists. See `DEVELOPMENT.md`
for the exact commands.

## Adding new transport / mechanism

- **Transport:** `Endpoint` variant + parser in `omq-proto/src/endpoint.rs`,
  `transport/<name>.rs` in each backend. Compression transports are
  `transform/` layers on TCP, not separate transports.
- **Mechanism:** module under `omq-proto/src/proto/mechanism/`,
  feature-gate, register with greeting state machine, integration
  test in **both** `tests/<mechanism>.rs`.
