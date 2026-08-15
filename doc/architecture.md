# Architecture

`omq.rs` is split into a sans-I/O protocol crate, an async backend, and
compatibility layers. The protocol crate owns ZMTP correctness. The backend
owns tasks, transports, queues, reconnect, and socket semantics.

```text
user API: omq-tokio, omq-libzmq, pyomq
        |
        v
omq-tokio backend: SocketDriver, ConnectionDriver, transports, routing
        |
        v
omq-proto core: Connection, frames, mechanisms, messages, options
```

## Context and runtime management

`Context` is a cheap handle to a shared `ContextCore`. The core owns
one or more independent `current_thread` tokio runtimes,
each on its own OS thread. Each runtime has its own IO reactor (epoll /
kqueue), timer wheel, and task scheduler. There is no cross-thread work
stealing and no shared scheduler lock. Connections assigned to one
thread run with zero contention from connections on other threads.
`ContextCore` also owns the `inproc://` namespace for sockets created
through that context.

- `Context::new()`: one IO thread. Default.
- `Context::with_config(cfg)`: N IO threads. Each IO thread is a
  separate `current_thread` runtime on a dedicated OS thread.
  Connections are distributed across threads by least-load assignment.
  Fan-out sockets (`PUB`, `XPUB`, `RADIO`) create one lane worker
  per IO thread for parallel subscription matching and encoding.
- `Context::current()`: wraps the caller's active tokio runtime
  (works with both `current_thread` and `multi_thread`). No background
  threads, no IO pool. All connections share the caller's runtime.
  Fan-out lane count is always 1. This mode is useful for embedding
  omq in an existing async application, and for single-connection benchmarks
  where a `multi_thread` runtime can push a single TCP pipe to its
  limit. It does not scale fan-out across threads.

`Context::socket()` creates async sockets whose driver tasks run on the
context's runtime. With an owned context, callers await socket futures
from their own async runtime while OMQ IO stays on the context's runtime
threads. `Context::block_on()` is a plain-`fn main()` helper that runs a
future on the owned runtime and blocks the caller (not available on
`Context::current()`).

`Context::share_key()` returns a process-local opaque `u128` key.
`Context::from_share_key()` turns that key back into another handle to
the same `ContextCore`, if the owner still exists and has not been
terminated. The registry stores weak references only, so a key cannot
keep a context alive. C and foreign bindings pass the key as two `u64`
words; imported binding contexts must not terminate the owner unless
their API explicitly says they own it.

## Blocking API (background IO)

`Context::blocking_socket()` creates a sync socket for callers with no
async runtime. The application thread never touches tokio. The Context's
IO thread handles all network I/O, connection management, encoding, and
decoding. The application thread communicates with the IO thread through
lock-free queues.

**Send path.** `blocking::Socket::send()` tries `try_send()` first,
which pushes the message into the send pipe (yring) on the caller's
thread with no cross-thread hop. Only when the pipe is full does it
fall back to `Context::block_on()`.

**Recv path.** `blocking::Socket::recv()` calls `blocking_recv()`,
which drains the recv pipe directly on the caller's thread via
`try_drain()`. If no data is available, the thread parks via
`std::thread::park()`. The IO thread's connection driver unparks
the caller (via `BlockingRecvWaker`) when new data arrives.

**Performance tradeoffs.** The blocking API pipelines I/O and
application work across two threads: the IO thread reads from TCP,
decodes frames, and pushes into the recv pipe while the application
thread independently drains it. The async 1T path serializes these
on one thread (connection driver and user recv take turns). This
pipelining gives the blocking API roughly 2x throughput at small
message sizes (16-128 B). At 4+ KiB, wire bandwidth saturates and
the extra thread stops helping.

Latency is worse: each message crosses a thread boundary (yring push
plus `unpark()`), adding roughly 30 us per hop. In throughput mode
this cost is amortized across batches; in request/reply it is paid
on every round trip (roughly 80 us vs 47 us p50 at 16 B).

| metric | async 1T | bg 1T | why |
|--------|----------|-------|-----|
| small-msg throughput | 7M msg/s | 14M msg/s | parallel I/O + app |
| small-msg CPU (push) | 100% | 200% | 2 threads both saturated |
| large-msg throughput | 5.5 GB/s | 5.2 GB/s | wire-limited |
| REQ/REP latency | 47 us | 80 us | cross-thread signaling |

When N > 1, accepted or connected TCP/IPC streams migrate from the
accepting thread's reactor to the assigned thread's reactor via
`into_std()` / `from_std()` re-registration. This is necessary because
each `current_thread` runtime owns its own epoll fd; a socket registered
on one reactor cannot be polled from another.

The `OMQ_IO_THREADS` environment variable sets the default IO thread
count for `ContextConfig::from_env()`.

## Crates

`omq-proto` is pure protocol code. It has no file descriptors and no async
runtime. `Connection::handle_input` consumes bytes, `poll_event` emits decoded
events, `send_message` queues frames, and `poll_transmit` exposes wire bytes.

`omq-tokio` is the default runtime backend. It owns TCP, IPC, inproc, UDP,
WS/WSS, reconnect supervisors, monitor events, socket actors, connection
drivers, and hot-path send/recv shortcuts.

`omq-libzmq` exposes a libzmq-compatible C ABI. `omq-bench` drives
cross-implementation benchmark peers and SVG chart generation.
`bindings/pyomq` exposes sync and asyncio Python APIs through PyO3.
`yring` provides hot-path queues used by inproc and routing.

## Socket Model

Every socket has one logical inbound queue and one logical outbound routing
surface. Connected peers attach driver tasks to those surfaces.

```text
Socket::send -> SendSubmitter / SocketDriver -> per-peer send pipe
per-peer driver -> recv_tx / SocketDriver -> Socket::recv
```

`SocketDriver` owns state that must be serialized: peer table, bind/connect
lifecycles, reconnect timers, monitor events, ROUTER identities, XPUB
subscriptions, DISH groups, and REQ/REP type state. It is not on every hot
message path.

Stateless sends bypass the actor through `SendSubmitter`. REQ/REP still check
shared type state before submit. Plain recv paths bypass the actor when no
identity, group, or subscription post-processing is needed.

### Caller-driven exclusive sockets

`omq_tokio::exclusive::Socket` is an opt-in alternative for latency-sensitive,
single-peer workloads. It owns both the Tokio TCP stream and the sans-I/O
`omq_proto::Connection`; the caller drives TCP, ZMTP, reconnects, and heartbeat
timers through methods requiring `&mut self`. It supports TCP `PAIR`,
`DEALER`, `ROUTER`, `REQ`, `REP`, `CLIENT`, and `SERVER`.

```text
regular Socket:   caller -> routing/queue -> ConnectionDriver task -> TCP
exclusive Socket: caller -> omq_proto::Connection                  -> TCP
```

The exclusive path removes the connection-driver task and userspace data relay,
but deliberately gives up the regular socket's cloneable handles, multi-peer
routing, background progress, and userspace outbound queue. A failed send is
not replayed because it is ambiguous whether the peer received a partial or
complete command. When no `send` or `recv` future is active, the application
must call `maintain()` to drive idle heartbeat and reconnect work.

The initial implementation supports connected TCP DEALER, REQ, and REP sockets
using the NULL mechanism. Unsupported socket types and transports return
explicit configuration errors; bind/accept and additional socket semantics can
be added incrementally.

## Messages

`Payload` and `Message` are small-value enums optimized for common single-part
traffic.

```rust
enum PayloadInner {
    Empty,
    Inline { len: u8, data: [u8; 62] },
    Single(Bytes),
}

enum MessageInner {
    Empty,
    Inline { len: u8, data: [u8; 71] },
    Single(Payload),
    Multi(Vec<Payload>),
}
```

`Payload` is 64 B and stores up to 62 B inline. `Message` is 64 B and stores up
to 55 B inline, avoiding heap allocation and refcount traffic for small
messages. Larger single-part messages use `Bytes`; multipart messages use
`Vec<Payload>`.

## Encoding

`FrameBuffer` is the outbound framing buffer: an arena (16 KiB for TCP/WS,
64 KiB for IPC) plus an entry list. Frame headers always go into the arena.
Small messages below `ARENA_THRESHOLD` (4 KiB) encode header and payload
contiguously into the arena. Large messages
write the header into the arena and keep payload `Bytes` as external entries
for gather write. The arena tracks its peak capacity so that after
`split().freeze()` reclaims the buffer, the next reserve pre-allocates at full
size instead of cascading through doubling copies.

`PeerTransmitSlot` wraps `FrameBuffer` in a short-held `std::sync::Mutex`, capped
at 512 KiB (close to the kernel TCP send buffer). Socket handles encode into
the slot. `ConnectionDriver` owns the writer and flushes from its `data_ready`
select branch. Producer-to-consumer signaling uses `DataSignal`: an atomic
flag plus `Notify` that coalesces wakes so only the `false`-to-`true`
transition fires `notify_one`. The consumer clears the flag before draining,
then calls `rearm_if_nonempty` to self-wake if data remains. For
budget-interrupted drains, `reschedule` fires unconditionally.

Latency-profile TCP peers also carry a stateless `DirectTcpWriter` with a
duplicated nonblocking descriptor. After `PeerOutbound` encodes an arena-only
message into the peer slot, it may try one direct `write()` from
`FrameBuffer::arena_bytes()` on the caller side. The writer reports the actual
byte count. `PeerTransmitSlot` advances the arena by that count and leaves any
remainder queued under normal `DataSignal` readiness, so partial writes are
finished by the connection driver and never live in a side buffer.

The async driver has a separate arena-only path. It copies slot arena bytes
into a reusable owned buffer before `write_all().await`, because the slot mutex
guards the `arena_bytes()` borrow and must not be held across await. Gather
entries still use `Bytes` and vectored writes.

CURVE keeps per-connection nonce state, so encrypted traffic uses
per-connection ordered transforms. CURVE encrypts and decrypts in place
(`SalsaBox::encrypt_in_place_detached` / `decrypt_in_place_detached`) with one
allocation per message.

Compression transports (`lz4+tcp://`, `zstd+tcp://`) transform whole messages
before ZMTP framing.

- Peer-routed sends (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `PAIR`, `ROUTER`,
  `REP`, `SERVER`, `PEER`, `STREAM`) compress in the selected peer driver.
  Large messages (>= `Options::compression_offload_threshold`, default 8 KiB)
  borrow a warm encoder from the socket's `CompressionPool`, run compression
  on Tokio's blocking pool, and drain results in send order.
- Lane-routed broadcast sends (`PUB`, `XPUB`, `RADIO`) use lane-local
  encoders. Each lane compresses once for its matched peers, with ordered dict
  updates, and does not use the offload pool.

## Routing

Round-robin sockets (`PUSH`, `DEALER`, `REQ`, `CLIENT`, `SCATTER`) use per-peer
`yring` send pipes for both byte-stream and inproc peers. The submitter scans
active pipes from a moving cursor. If every active pipe is full, async send
waits on a rotating peer and `try_send` reports HWM backpressure.

Connect-side round-robin endpoints allocate a pre-ready pipe during
`connect()`. The producer is immediately eligible for routing, so
connect-before-bind sends can queue before ZMTP READY. When the handshake
completes, the peer driver drains the same pipe; messages queued before READY
are not overtaken by later sends.

Bind-side round-robin sockets with no ready pipe are mute, matching libzmq:
blocking `send()` waits and `try_send()` returns `Full`. `omq-libzmq` maps that
native `Full` to `EAGAIN` for `DONTWAIT`/zero-timeout C sends. Native
`omq-tokio` has no `ZMQ_IMMEDIATE` option; `omq-libzmq` implements
`ZMQ_IMMEDIATE=1` by gating connected no-ready sends before they enter the
pre-ready pipe.

`Options::send_hwm` is a complete-message count, not a byte cap. It applies per
outbound pipe/ring: connect-side pre-ready pipes, materialized peer pipes, and
fan-out lane rings each have their own cap. Effective native capacity can
exceed `send_hwm` when multiple pipes or transmit slots exist.

Fan-out sockets (`PUB`, `XPUB`, `RADIO`) use lane workers for parallel
subscription matching and encoding. With N owned IO threads, N lane workers run,
one per IO thread. The caller pushes each message once to lane 0 (the
distributor). Lane 0 distributes the batch to active secondary lanes first,
then processes its own peers. This keeps the caller's send path to a single
`yring` push regardless of lane count. Each lane has split channels: a
`yring` control channel for subscribe, cancel, add-peer, remove-peer, and
shutdown commands, and a `yring` data channel for encoded dispatches. The
worker drains all control commands unconditionally every iteration, then drains
data dispatches up to `DrainBudget::WORKER` (256 messages / 2 MiB). This separation
guarantees control commands are reachable within bounded time regardless of
data throughput. Fan-out sockets drop on mute; `OnMute::Block` does not make
`PUB` or `XPUB` wait. `xpub_nodrop` stays on the direct backpressure path.

With `Context::current()` (borrowed runtime), fan-out always uses a single
lane regardless of the runtime's thread count.

Identity-routed sockets (`ROUTER`, `REP`, `SERVER`, `PEER`) route by peer
identity. Exclusive sockets (`PAIR`, `CHANNEL`) target one peer. Fair-queue
recv preserves per-peer ordering while rotating across peers.

## Proxy

`omq-tokio::Proxy` composes two existing sockets. It has no socket type of
its own and no forwarding yring; local buffering is limited to one pending
message per direction when a target reports HWM backpressure. Socket HWMs,
routing policies, and drop/block behavior remain the source of truth.

The loop drains at most `burst_size` complete messages from one direction
before rechecking control and the opposite direction. Default burst is 64:
large enough to avoid one `select!` per message under load, bounded enough
that a hot frontend cannot indefinitely starve backend-to-frontend traffic.
When a target is full, the proxy retries the pending message before reading
more from that source.

Steerable control supports `PAUSE`, `RESUME`, `TERMINATE`, and `KILL`.
`STATISTICS` is omitted. Capture sockets receive best-effort copies via
nonblocking send and never backpressure data forwarding.

## Inproc

Inproc bypasses ZMTP framing and kernel I/O. Cross-thread peers use `yring`
send pipes and deliver `InboundFrame::Message` through `inproc_peer_driver`.
Same-thread paths use direct `yring::ProducerOwner` access where applicable.
Names are scoped to the owning `ContextCore`, not the process. Two
independent contexts can bind the same `inproc://name` without seeing
each other. Handles imported with `Context::from_share_key()` share the
same namespace, which lets different language bindings in one process
communicate over inproc without a process-global registry. Public
semantics remain the same: HWM backpressure, round-robin fairness, and
connect-before-bind.

## Drain Budgets And Signaling

Every loop that drains a channel or queue is capped by `DrainBudget`: both a
message count and a byte count. Unbounded drains would starve the tokio runtime
and other tasks. Standard presets: `DrainBudget::WORKER` (256 messages / 2 MiB)
for lane workers and deferred fan-out, `DrainBudget::WIRE_DRAIN` (1024 / 1
MiB) for wire-slot drain.

Data-ready paths use `DataSignal`, a small atomic state machine plus `Notify`.
`mark()` fires `notify_one` only on the idle-to-pending transition. The consumer
calls `begin_drain()` before draining and `clear_after(is_empty)` afterward. A
producer mark that races with drain clear moves the signal to `DIRTY`, so
readiness survives stale empty observations. `reschedule()` fires
unconditionally for budget-interrupted drains where the consumer already knows
data remains. Wire slots, send pipes, drop queues, and lane workers all use
`DataSignal`.

State-change waits use `StateSignal`: a generation counter plus `Notify`.
Waiters capture a generation, enable their waiter, re-check caller state, then
await only if nothing changed meanwhile. This is used where readiness is not a
single data queue, for example pipe-space release and route-state changes.

Control commands (subscribe, cancel, add-peer, remove-peer, shutdown) travel on
dedicated channels separate from data. Lane workers, for example, drain all
control commands unconditionally before draining data up to budget. This
guarantees control latency is bounded by one data budget drain, not by queue
depth.

Loom covers the race windows that would lose these wakeups:
`omq-tokio/tests/loom_signal.rs` models `DataSignal` rearming, `StateSignal`
generation checks, pipe-space release, and route waits that race with peer
activation. `yring/tests/loom.rs` covers the lower-level SPSC cursor ordering,
wraparound, producer drop, async `push_async` wakeups, and upper-layer
readiness patterns built on the ring.

## Transports

| URI | implementation |
| --- | --- |
| `tcp://host:port` | TCP with `TCP_NODELAY` |
| `ipc:///path` | Unix stream or Windows named pipe |
| `inproc://name` | in-process channel |
| `udp://host:port` | datagrams for RADIO/DISH |
| `lz4+tcp://host:port` | TCP plus LZ4 transform |
| `ws://...`, `wss://...` | ZWS over WebSocket, optional TLS |

Reconnect supervisors replay subscriptions and groups after reconnect.
Connect-side round-robin sends use bounded pre-ready pipes; bind-side
round-robin sends with no ready pipe mute.

## Mechanisms And Monitoring

Mechanisms live under `omq-proto/src/proto/mechanism/`: NULL is always on;
PLAIN and CURVE are feature-gated. LZ4 and WS are transport features.

`Socket::monitor()` returns a `Stream<Item = MonitorEvent>`. Events carry owned
`PeerInfo` snapshots for listening, accept/connect, delayed connect,
handshake, disconnect, peer command, and close.

## Source Map

Protocol: `omq-proto/src/message.rs`, `frame_buffer.rs`, `flow.rs`,
`routing.rs`, `subscription.rs`, `proto/connection/`, `proto/frame.rs`.

Backend: `omq-tokio/src/socket/actor/`, `socket/handle.rs`,
`engine/driver.rs`, `engine/send_pipe.rs`, `engine/transmit_slot.rs`,
`engine/signal.rs`, `routing/`, `transport/`, and
`tests/loom_signal.rs`.

To add a socket type, extend `omq_proto::proto::SocketType`, compatibility
checks, protocol routing, and the matching backend strategy. To add a
transport or mechanism, extend endpoint/mechanism parsing first, then add the
backend module and integration tests.
