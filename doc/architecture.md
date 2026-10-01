# Architecture

OMQ sockets send and receive complete messages. The backend handles routing,
connections, framing, and backpressure; applications choose a transport by URI.

Three distinctions explain most of the implementation:

- **Protocol vs. I/O:** `omq-proto` processes bytes; `omq-tokio` owns sockets
  and tasks.
- **Control vs. data:** the socket actor manages peers and lifecycle. Most
  messages travel through queues without an actor hop.
- **Ownership vs. notification:** queues hold messages. Signals tell tasks
  when to check those queues; a wake is not a message.

Start with [runtime ownership](#runtime-ownership) and
[a message through the system](#a-message-through-the-system). For a specific
path, see [routing](#routing), [receives](#receives),
[buffers](#messages-and-buffers), or [scheduling](#scheduling-and-wakeups).
The [source map](#source-map) points to implementations.

## Crates and responsibilities

```text
 Rust application       C application        Python application
        |                     |                     |
        |                omq-libzmq                pyomq
        |                     |                     |
        +---------------------+---------------------+
                              |
                          omq-tokio
                 sockets, routing, tasks, transports
                       /             \
                  omq-proto       yring / fanring
                codec + values    message queues
```

| Component | Owns |
| --- | --- |
| `omq-proto` | ZMTP codec, messages, options, mechanisms, transforms; no async or I/O |
| `omq-tokio` | Default backend, async/blocking APIs, drivers, routing, reconnects |
| `omq-libzmq` | libzmq-compatible C ABI and C socket semantics |
| `bindings/` | Language ownership, conversion, and API adapters |
| `omq-bench` | Cross-implementation peers, benchmark data, SVG generation |

The queue crates live in the separate
[fanring.rs workspace](https://github.com/paddor/fanring.rs). Their releases,
Loom tests, and Miri checks belong there. Language bindings build outside the
main Cargo workspace.

The sans-I/O `Connection` exposes four main operations:
`handle_input` accepts bytes, `poll_event` emits decoded events,
`send_message` accepts outgoing messages, and `poll_transmit` exposes wire data.
The backend decides when and how to read or write it.

## Runtime ownership

`Context` is a cheap handle to a shared `ContextCore`. The core owns runtime
resources and the context's `inproc://` namespace.

| Construction | Runtime placement | Fan-out lanes |
| --- | --- | --- |
| `Context::new()` | One background `current_thread` runtime for control and I/O | 1 |
| `Context::with_config`, N > 1 | N data runtimes plus one internal control runtime | N |
| `Context::current()` | Borrows the caller's active Tokio runtime; creates no threads | 1 |

Owned multi-IO contexts look like this:

```text
 ContextCore
 |
 +-- Control thread: current_thread runtime
 |     socket actors, endpoint management, blocking control calls
 |
 +-- IO thread 0: current_thread runtime + reactor
 |     assigned connection drivers, fan-out lane 0
 |
 +-- IO thread 1: current_thread runtime + reactor
 |     assigned connection drivers, fan-out lane 1
 |
 +-- ... IO thread N-1
 |
 +-- inproc namespace shared by this context's sockets
```

The control thread is not included in `io_threads`. With one IO thread,
control and data share that thread. Each owned runtime has its own reactor,
timers, and scheduler; there is no work stealing between them.

Connections use least-load assignment. TCP/IPC streams accepted or connected
on the control runtime are re-registered on the chosen data reactor through
`into_std()` / `from_std()`. Sharing a stream handle does not migrate its
reactor registration.

Each materialized byte-stream or inproc driver future owns one IO-thread load
lease, reserved before spawning. Completion, setup failure, task abortion, and
panic unwinding release it once. Peer-table removal does not release another
owner's load. Raw STREAM currently runs on the actor runtime and holds no data
IO-thread lease. Socket teardown joins or aborts tracked driver tasks so their
leases also leave the shared context's counters.

### Application APIs

- **Async:** `Context::socket()` creates a socket whose drivers run on the
  context. Application futures can be awaited on the caller's own runtime.
- **Blocking:** `Context::blocking_socket()` uses the same background I/O.
  Ready sends try admission directly; ready receives drain on the application
  thread. Empty receives park through `BlockingRecvWaker`. Optional
  `Options::recv_spin(Duration)` polls before each park, across socket types.
  The budget defaults to zero, independently of the latency profile. It also
  applies to timed/cancelable receives and bulk calls waiting for their first
  message; deadlines and cancellation stop the spin early. Async and
  nonblocking receives do not spin. Enable only when the CPU cost is acceptable;
  sharing CPUs with IO threads or peers can make tail latency worse. A full send
  can fall back to `Context::block_on()`.
- **Embedded:** `Context::current()` supports either Tokio runtime flavor.
  It has no owned IO pool, and `Context::block_on()` is unavailable.
- **Configuration:** `ContextConfig::from_env()` reads `OMQ_IO_THREADS`.
  Configuring zero IO threads selects the borrowed-runtime mode.

Background I/O can overlap decoding with application work, at the cost of
cross-thread signaling. A single-thread embedded application shares execution
time with its drivers. Neither layout is universally faster; measure the
actual payload, socket pattern, and placement.

### Sharing contexts across bindings

`share_key()` exports a process-local opaque `u128`; `from_share_key()` imports
another handle to the same core. C bindings carry it as two `u64` words.

The registry holds weak references. A key neither keeps a context alive nor
revives a terminated context. Imported binding contexts must not terminate
the owner unless their API explicitly grants ownership.

## A message through the system

Each regular socket has one logical receive interface and one logical send
routing interface. These are not necessarily single physical queues.

For ordinary throughput-mode TCP PUSH/PULL:

```text
 Sending process                       Receiving process

 application: send(Message)             application: recv() -> Message
             |                                      ^
       SendSubmitter                          fair application drain
             |                                      ^
      per-peer send pipe                       fanring sender lane
             |                                      ^
     ConnectionDriver                         ConnectionDriver
      frame / encode                           read / decode
             |                                      ^
             +---------------- TCP -----------------+

 Each SocketDriver manages its own peers and lifecycle alongside this path.
```

`SocketDriver` serializes the peer table, bind/connect lifecycle, monitor
events, identities, subscriptions, groups, and socket-type state. It does not
process every payload. `ConnectionDriver` bridges one peer's queues and wire.

Ordinary throughput callers enqueue raw messages. Drivers or fan-out workers
encode and compress them. Existing plain-TCP latency fast paths are a specific
exception, described under [writes](#framing-and-writes).

REQ/REP still enforce send/receive alternation. REQ has a shared hot-path flag;
latency-profile REP can strip and save its request envelope in the connection
driver. Receive paths needing actor-owned routing state keep that processing.

## Routing

| Policy | Socket types | Destination |
| --- | --- | --- |
| Round-robin | PUSH, DEALER, REQ, CLIENT, SCATTER | One eligible peer |
| Fan-out | PUB, XPUB, RADIO | Matching subscribers or groups |
| Identity/reply route | ROUTER, REP, SERVER, PEER | Selected peer or saved request route |
| One peer | PAIR, CHANNEL | The exclusive peer |

STREAM uses a separate raw-TCP path: application messages carry peer identity
frames, but the wire has no ZMTP framing or handshake.

### Round-robin and startup

Byte-stream and inproc peers use per-peer `yring` send pipes. The submitter
scans from a moving cursor; if all active pipes are full, async send waits
for space and `try_send` reports `Full`.

Connection ordering does not require application coordination:

1. A connect-side endpoint allocates a bounded pre-ready pipe at `connect()`.
2. Sends can enter that pipe before the remote endpoint binds or reaches READY.
3. After handshake, the driver drains the same pipe. Later sends cannot
   overtake the queued messages.

A bound socket with no ready pipe is mute: blocking send waits, and
`try_send` returns `Full`. The C ABI maps this to `EAGAIN` for nonblocking
sends. Its `ZMQ_IMMEDIATE=1` option gates admission to connected, not-yet-ready
peers; the native API has no `ZMQ_IMMEDIATE` option.

`send_hwm` counts complete messages per pipe/ring, not bytes or total socket
memory. Multiple peers, pre-ready pipes, lane rings, and transmit slots can
make aggregate capacity larger than one HWM.

### Fan-out lanes

The application enqueues once to lane 0, regardless of lane count. Lane 0
distributes batches to active secondary lanes before processing its own peers.

```text
 caller -- raw Message --> lane 0 / distributor
                              |
                              +--> lane 1 --> match + encode --> peer slots
                              +--> lane 2 --> match + encode --> peer slots
                              |
                              +-----------> match + encode --> peer slots

 SocketDriver -- separate control ring --> each lane
 peer slots   -- ConnectionDriver writes --> transports
```

Each owned IO thread hosts one lane. Borrowed contexts always use one lane,
even on a multi-thread runtime. Socket clones share lane 0's producer under
a short mutex; adding clones does not add distributor lanes.

Endpoint URI parsing recognizes complete carriers before interpreting one
leading codec prefix. `Endpoint::with_compression` uses the same enabled-carrier
policy and retains existing wrapped variants. The endpoint selects codec kind;
ordinary bind/connect operations inherit immutable socket options. Explicit
`bind_with_compression_options` and `connect_with_compression_options` capture
five codec parameters: dictionary, auto-training, threshold, level, and target
dictionary capacity. Unset fields select codec defaults; construct
`CompressionOptions` from `&Options` to inherit values before overriding them.
The C bridge captures its current compression overlay per operation, including
after backend materialization. Mechanism and decoder limits retain the original
socket configuration. Typed conflicts, unsupported
carriers, and invalid WS addresses fail before setup. Effective CURVE/STREAM
policy is checked before DNS or carrier IO. Pending resolution, listener
accepts, completed connects, and reconnects retain their operation's immutable
configuration. Updating the C compression overlay never replaces a live
dictionary or reconfigures an existing listener.

The private fan-out registration module owns peer admission, lane compatibility,
and slot-capacity reactivation. `CodecSetup` captures concrete encoder/decoder
state and an immutable profile from effective connection options. Slots retain
that same profile. Compatibility compares codec kind, dictionary contents,
effective thresholds, levels, training configuration, and logical size limits.
Equivalent defaults share a group. Decoder dictionary caps stay per connection;
ignored encoder fields do not split groups. Auto-training with a dynamic default
threshold remains distinct from a fixed threshold. Queue, TLS, and mechanism
options do not enter the key after encrypted-codec validation. Configured logical
limits remain conservative until their wire boundaries are covered.

Each existing IO lane owns at most eight compatible groups, including the plain
group. Groups own concrete encoders and train independently on the lane task.
No encoding or dictionary sampling runs on the caller thread. WS retains its
connection-driver path; additional native profiles beyond the group limit also
retain that reliable fallback. Removing the last peer retires its group. Ordered
control commands initialize fresh state before a retired slot is reused.

Training examines at most 32 parts per publication and finishes within 100
publications, 1,000 nonempty samples, or 100 KiB of sample bytes. Individual
samples stay below 2 KiB; dictionary capacity is at most 8 KiB. Static
configuration disables training. Valid dictionary views are compacted when
profiles are captured, so an 8-KiB slice cannot retain a larger backing buffer.
Each group keeps its one dictionary shipment for late subscribers; shipment
state remains per connection and dependent payloads follow that shipment.

A publication is filtered before encoding. Each matched group encodes once.
Blocking dispatch tries ready targets across all groups before waiting for
slot space, continues serving control commands while waiting, and drops a
removed connection's pending output without passing it to a replacement.
Prepared storage holds at most eight payload frames and eight dictionary frames
for one publication, shared by its blocked recipients. No prepared publication
backlog is added. Absolute aggregate byte accounting and configured logical
versus codec-wire size boundaries remain open in the transport plan.

Fan-out preparation retains every gathered chunk of an atomic multipart
publication. The 1,024-chunk wire-drain cap still limits individual write turns;
it cannot serve as a limit on prepared message contents. Regression cases cover
600 and 1,025 body parts, including grouped compressed output and all mute
policies. Six production connection drivers also deliver three consecutive
1,026-part publications through bounded memory streams and continued write/read
turns. TCP has no explicit 1,024-part limit; WS retains its 65,536-part cap.

With `xpub_nodrop`, fallback inbox admission waits independently for each
peer while native lane admission progresses alongside it. Socket close wakes
these waits. `try_send` reserves all fallback capacity before admitting any
lane or fallback publication, so a full inbox does not silently drop data
or publish part of a retry to the other peers.

The important work-saving choices are:

- Match before encoding. Prefix subscriptions use a Patricia trie; an
  all-subscribed shortcut skips per-peer lookups. RADIO uses group membership.
- Encode/compress once per compatible target set in a lane. Share large
  payload chunks; retain per-peer paths for connection-specific transforms.
- Bound total copying: if encoded size times target count exceeds 8 KiB,
  use shared gather chunks rather than copying the body into every peer slot.
- Reuse framing and target scratch; signal each touched peer once per batch.
- Under drop-newest policy, skip muted destinations until space returns.
  A slow subscriber must not stop delivery to other subscribers.

Fan-out drops on mute by default. `OnMute::Block` alone does not make PUB or
XPUB wait; `xpub_nodrop` enables their backpressure path.

### Identity routing and PEER sends

ROUTER borrows the identity frame for lookup, then removes it only after
capacity admission. A full retry returns the original message without rebuilding
its envelope. SERVER routes using numeric metadata rather than an identity frame.
REP uses its saved request route.

PEER uses immutable identity tables published through ArcSwap:

- Each socket clone lazily registers one fanring producer per destination.
- Hot sends lock that clone's producer for that destination. Latency-profile
  plain TCP additionally locks the connection's write admission guard.
- The existing connection I/O task fair-drains producers, then frames,
  compresses, and writes. There is no additional payload dispatcher.
- Sequential sends through one clone preserve per-destination FIFO. Different
  clones and concurrent sends have no relative order.

All producers for one PEER connection share admission limits:

| Limit | Bound |
| --- | --- |
| Application producer rings | 64, plus an empty registration ring |
| Capacity per ring | `min(send_hwm, 64)`, rounded to a power of two, minimum 1 |
| Total queued messages | Connection's send HWM, not HWM times clone count |
| Total charged bytes | `max(64 MiB, max_message_size)`, including frame slots |

Retired rings count until reclaimed. Registration exhaustion backpressures;
cloning cannot multiply the connection's payload allowance. Ring descriptors
have a separate bound of 65 times ring capacity. Driver/wire buffers are
additional and have their own batch and message-size bounds.

Failed `try_send` admission returns the intact message. Canceling an
uncompleted send consumes no capacity; accepted sends survive clone drop.
Oversized local PEER messages return a protocol error.

Route retirement clears cached producers before publishing replacement routes
and wakes blocked senders. Coordinated queue teardown releases unread payloads
even if stale handles remain. Old disconnect events cannot remove a new route.

## Receives

### Fair fan-in and bulk receives

PULL, GATHER, SUB, and XSUB use socket-owned fanring MPSC channels. Each
connection driver owns a producer lane; the application owns the drain.
Ordinary receives rotate after each message and preserve each peer's FIFO.

`Options::recv_batching` changes bulk receive scheduling: move whole
per-connection windows instead of popping one message at a time. Count and
byte budgets still apply. Admission checks queued values, so no extra message
is staged on the application side between calls.

Receive queues store plain `Message` values. Drain predicates derive conservative
byte charges from message lengths, without adding per-slot metadata.
`recv_many_into` and related APIs append directly to caller-owned vectors.
Generation-based peer snapshots avoid rebuilding the peer list on every drain.

### Capacity credits are not messages

A credit is a consumed ring slot that the producer may reuse. Publishing
credits can wake a producer waiting for space; doing that after every message
adds atomic and wakeup work to the receive hot path.

Ordinary fan-in receives retain native credit batching:

- Release after half a ring of consumed slots, with a 64-slot minimum capped
  at capacity. Larger full rings resume at the half-full low watermark;
  small rings keep their existing batch size. Fairness remains independent.
- Release partial credits when a lane is observed empty and before receive
  can park. Bulk calls release consumed slots before returning.
- Return messages immediately. Do not wait for a batch to fill, change
  per-message fairness, or weaken signal fences.

The tradeoff is delayed slot reuse: a producer can temporarily see up to one
credit batch minus one consumed slots as unavailable. This is local flow
control, not TCP or ZMTP acknowledgment batching.

### PEER receive ownership

PEER uses one socket-owned fanring receiver, with one bounded producer per
connection. Fanring selects ready connections; idle peers need no manual scan.
Single-message receives rotate fairly across ready producers. Socket clones
share the receiver; concurrent receive calls serialize the drain.
Parked async callers hand off a batch wake only when another caller is waiting;
canceling a receive passes that wake onward if messages remain queued.

Use ordinary `recv()` or `recv_many_into()` on application threads. Each message
retains its identity prefix for replies and application dispatch. OMQ does not
assign identities to application workers. TCP, IPC, and inproc use the same
ownership model; connection I/O assignment and sending remain independent.

```text
connection A --> producer A --+
connection B --> producer B --+--> socket fanring --> recv / recv_many_into
connection C --> producer C --+    shared count/byte budget
```

Each entry carries its connection's identity/generation state and an aggregate
budget permit. Handover invalidates the old state; receive discards stale entries
and returns their permits. Discards count toward both drain limits, with an
async yield when cleanup exhausts a budget.

Space credits follow fanring's batching above. Empty drains and bulk returns
release partial credits before the application can park. Driver batches
coalesce application wakes through `DataSignal`. Registration and shutdown
share a cold lock; message drains never take it.

| Default per PEER socket | Bound |
| --- | --- |
| Allocated peer rings | 128, including retired generations still owned |
| Queued messages | 8192 |
| Charged bytes | 64 MiB of payload plus per-frame slot storage |
| Per-connection ring | Receive HWM rounded to a power of two, minimum 16 |

All peers share the socket's aggregate budget. The byte allowance grows to
`max_message_size` when that option exceeds 64 MiB. PEER defaults
`max_message_size` to 64 MiB. Receive HWM still bounds each connection's ring.

These are admission limits, not a total-memory promise. Decoder/transport
buffers, one pending decoded message per connection, descriptors, frame tables,
slice-retained backing storage, and caller-owned storage add memory beyond the
charged queue bytes.

### Backpressure, disconnect, and close

- A full PEER receive queue pauses inbound data, not replies, close commands,
  or cancellation. Local heartbeat receive-timeout accounting pauses while
  OMQ deliberately stops reading.
- Socket receive teardown cancels its peers, including idle peers. Coordinated
  fanring teardown immediately reclaims unread payloads even with live producers.
- Identity handover invalidates unread old-generation messages before new
  traffic becomes visible. Ordinary disconnect leaves queued messages readable.
- Socket close ends receive admission independently of send linger. A late
  connection attachment must tolerate the receive queue already being closed.

Heartbeats retain at most one locally queued probe. Its timeout
starts after the complete codec wire prefix reaches the writer; arena/slot
traffic does not advance that prefix. Actual received bytes acknowledge an
admitted probe. Periodic admitted keepalives preserve the original silence
deadline. Local receive backpressure suspends silence accounting while
retaining outbound keepalives for the other peer's liveness judgment.
Writer admission is not remote receipt: TCP/WS ordering, TLS/kernel buffering,
and runtime scheduling still constrain remote control progress.

Close stops new caller sends while preserving accepted queues. Nonzero linger
then asks each connection to drain its own batch, offload, arena, deferred,
slot, and partial-write state. Removing a message from a send ring is not wire
completion. Fan-out also waits for distributor and secondary worker batches.
One absolute socket deadline covers draining, WS CLOSE reply, and writer/TLS
shutdown; zero linger retains immediate cancellation. Transport completion
does not acknowledge remote application delivery. Resource-accounting and
control-progress limits are tracked in [WebSocket limits](zws.md).

Heartbeats detect missing peer activity; they do not make a slow application
drain its queue. Heartbeat traffic can continue when outbound space allows,
but transport backpressure may still trigger the remote endpoint's timeout.

## Messages and buffers

### Value layout and ownership

| Representation | Purpose |
| --- | --- |
| `Message`: 64 B, up to 55 B inline | Common single-part messages without heap payload storage |
| `Payload`: 64 B, up to 62 B inline | Small multipart parts without separate payload allocation |
| `Bytes` or shared `Arc<PayloadOwner>` | Large payloads share storage across clones/slices |
| Multipart frame table | Owns part descriptors, not necessarily copies of their bodies |

The 64-byte size is not a cache-line alignment guarantee. Inline construction
copies bytes into the value; larger owned `Vec`/`String` inputs can transfer
their storage instead.

Borrow with `part_slice`, `iter_slices`, or `as_slice` when ownership is not
needed. Converting inline data to `Bytes` can allocate and copy. Converting a
shared owner allocates an adapter without copying its body; empty payloads
also retain explicitly supplied owners.

Compact forms avoid frame tables for common REQ delimiters and SERVER routing
IDs. Routing changes retain existing multipart tables where possible. Cached
byte totals avoid rescanning parts; mutable table access invalidates the cache.

### Pools reduce allocation, not admission

`MessagePool` recycles multipart frame tables. Its cache is bounded, but
exhaustion allocates normally and oversized tables are not retained. It does
not cap all in-flight messages or payload memory.

`Options::recv_message_pool` opts native byte-stream decoders into table reuse;
it is off by default. Inproc already transfers owned messages. Returning a
table drops payload owners before taking the cache lock, so application release
callbacks do not run under that lock.

### Framing and writes

`FrameBuffer` combines an arena with external payload entries. Default initial
arenas are 16 KiB for TCP/WS and 64 KiB for IPC.

- Below the default 4 KiB threshold, copy headers and bodies into the arena.
  Many small messages then share one write buffer.
- For large bodies, keep headers in the arena and bodies as shared `Bytes`.
  Gather writes avoid concatenating large payloads first.
- Driver-owned arena-only output writes directly from its slice and retains
  allocation capacity. Mixed-entry drain copies arena bytes once into shared
  storage, retaining the original arena for reuse.
- A `PeerTransmitSlot` holds framed output under a short mutex, with a default
  512 KiB byte cap. Its arena is staged into reusable driver storage before
  awaiting I/O; the mutex never spans the write await.
- Partial writes retain chunks and offsets. A canceled write future resumes
  without re-encoding or copying the unwritten tail.

Eligible plain-TCP latency routes have a `DirectTcpWriter`. That specialized
path may frame into the slot and attempt one caller-side nonblocking write.
CHANNEL has the same eligibility as PAIR. PEER keeps its ArcSwap routes,
per-clone fanring producers, shared admission limits, and fair draining under
both profiles. Its direct attempt happens after normal admission.

One connection guard coordinates queue publication with write ownership.
Handshake starts driver-owned. The driver publishes idle only when its partial
write, codec, framing buffer, batch, offloads, send pipe, and inbox are empty.
Fallback data inboxes admit at most `min(64, max(1, send_hwm))` messages.
A caller may write only while idle. A short write or EAGAIN retains the tail in
the slot and transfers ownership to the driver. Later sends queue behind it.
Driver ownership persists across awaits and canceled write polls without holding
the mutex. Retirement closes admission and wakes blocked senders. Throughput and
transformed paths use workers.

TCP uses `TCP_NODELAY`; userspace batches provide coalescing. Queued-byte totals,
header scratch, chunk vectors, and arena capacity are retained rather than
recomputed or reallocated for every frame.

### Reads and decoding

The rolling read buffer starts at 4 KiB and grows to at most 128 KiB after
consecutive full reads. The codec accepts owned chunks without an append copy.

| Incoming data | Handling |
| --- | --- |
| Complete untransformed single-part frame <= 55 B | Decode directly into inline `Message` |
| Larger frame contained in one chunk | Share the chunk by slicing |
| Frame spanning chunks | Coalesce into owned storage |
| Eligible plain TCP/IPC frame >= 128 KiB by default | Read directly into final owned payload storage |

Direct reads copy any already-read prefix once, then read the remainder into
spare `BytesMut` capacity without zero-filling it. WS and frame transforms such
as CURVE do not use this path. The threshold can be changed or disabled.

Per-connection receive pools recycle direct-read buffers through 8 MiB and
retain at most 64 MiB. A buffer returns only after its last payload clone/slice
drops. Weak pool references let connection close free idle buffers even while
applications retain messages. Larger payloads use fresh, unpooled storage.

Here, "zero-copy" only means avoiding a specific userspace payload copy. It
does not promise zero kernel copies, no metadata allocation, or copy-free
multipart, encryption, and binding conversions.

## Compression and wire transforms

Compression transports transform messages before ZMTP framing.

| Path | Encoder ownership and scheduling |
| --- | --- |
| Peer-routed sends | Selected connection driver; eligible large work can use a warm `CompressionPool` encoder on Tokio's blocking pool |
| PUB/XPUB/RADIO lanes | One independent encoder per lane; one result shared by its matching peers |
| CURVE | Per-connection cipher and nonce state; ordered transforms |

Compression offload defaults to messages at least 8 KiB. Results stay in send
order. Small messages, unavailable pool capacity, or pending dictionary/training
state can keep work on the connection driver. Fan-out lanes do not use this pool.
Reused pool encoders synchronize the primary's threshold, level, dictionary,
and size settings. They never change a live connection's dictionary or ship
another dictionary. Active LZ4 auto-training targets must be at least 32 bytes
because the pinned COVER trainer requires eight-byte segments.

LZ4 and Zstd reuse contexts and scratch. Small parts can pass through without
compression; encoders also reject results without enough wire-size saving.
Eligible plaintext paths frame the sentinel plus original payload directly.
Warm contexts do not eliminate owned output allocations.

Dictionary setup and shipment are separate:

- Static or trained dictionaries initialize reusable encoder state.
- Fan-out distributes a trained dictionary to lanes instead of training once
  per subscriber.
- Shipment is tracked per direction and connection, before dependent data.
  Offload encoders do not ship dictionaries.
- LZ4 permits at most one dictionary shipment per direction on a connection;
  a second shipment closes it. See the [LZ4 RFC](lz4-rfc.md).

CURVE encrypts/decrypts in place using retained cipher state, but constructing
encrypted wire messages still needs mutable storage and copying. WS has fused
framing and tiny-message decode paths; client masking needs writable storage.
These are specialized paths, not end-to-end zero-copy guarantees.

Connection materialization resolves a private `WireFraming` once: ZMTP or
ZWS with the local WS role. Codec setup and transmit slots consume that same
value, so masking cannot disagree with the codec's role. TLS/carrier IO and
message transforms retain their separate owners. This uses the existing fused
arena and gather encoders; it adds no forwarding task or per-message trait
dispatch. WS fan-out still uses its existing fallback pending lane evidence.

## Scheduling and wakeups

### Bound data work; keep control reachable

Data drains use both count and byte limits. Separate control channels keep
subscribe, cancel, peer changes, and shutdown out of payload backlogs. Workers
service queued control commands before starting the next bounded data batch.

| Work | Default limit |
| --- | --- |
| Lane/deferred fan-out drain | `DrainBudget::WORKER`: 256 messages / 2 MiB |
| Wire-slot drain | `DrainBudget::WIRE_DRAIN`: 1024 drain iterations / 1 MiB |
| Driver encoding | 512 messages, configured batch bytes, or 1 ms |
| Driver write turn | Wire-drain budget or 1 ms |
| Codec-event admission | 64 events / 64 KiB of logical work / 1 ms |

Wire-drain iterations are not necessarily individual messages. The default
encode byte limit is 128 KiB; `OMQ_BATCH_BYTES` is read once and cached.

Writes run as a main `select!` arm and preserve partial progress. A stalled
transport therefore does not trap control behind a full-buffer write loop.
Async sends that complete synchronously yield periodically; workers yield at
batch boundaries. Hot drain loops avoid unnecessary per-message clock reads.

Receive queue admission lives in [recv_sink.rs](../omq-tokio/src/engine/recv_sink.rs).
A full queue leaves one decoded message owned by the connection driver. Space,
control, writes, cancellation, and linger remain selectable; retries neither
decode again nor charge the receive limiter twice. MPSC reservations stay pinned
across select turns and commit through their actual permits, preserving waiter
order. REP publishes its envelope and body under one admission lock.
Large native byte-stream payloads retain one claimed destination across 64-KiB
reads in the same select, preserving buffer reuse and direct receive ownership.
An inbound payload claim keeps the independent outbound direction ready,
including CURVE command protection.

Codec-event admission lives in [peer_events.rs](../omq-tokio/src/engine/peer_events.rs).
A full actor mailbox retains one popped event and waits for its actual reserved
slot under the driver select. Further input and decoded-message admission pause
until the older event prefix is admitted. Metadata preparation happens once;
reverse writes, local commands, cancellation, and deadlines remain reachable.
The setup deadline stays active until handshake-event admission. Parse errors
retain their original failure while older events await admission. Authentication
ERROR output also runs under this select, with cancellation and the original
setup deadline intact. Socket-owned drivers publish final disconnect through reserved completion
slots, independent of mailbox space. The actor retains peer state until the
admitted event prefix and any prepared receive finish. See
[WebSocket limits](zws.md).

### DataSignal: work is pending

`DataSignal` combines `Notify` with four states: `IDLE`, `PENDING`, `DRAINING`,
and `DIRTY`. It coalesces data-ready notifications without losing work that
arrives during a drain.

```text
 Producer                                Consumer
    |                                       |
 publish message into queue                 |
    |                                  begin_drain()
 SeqCst fence                          enter DRAINING if PENDING
    |                                  SeqCst fence
 mark(): inspect signal                     |
    |                                  inspect / drain queue
    +-- IDLE: set PENDING, notify            |
    +-- DRAINING: set DIRTY             clear_after(is_empty)
    +-- PENDING/DIRTY: coalesce              |
                                       DIRTY or nonempty: rearm
                                       DRAINING + empty: become idle
```

The columns show each participant's ordering, not a fixed interleaving.
Both fences matter: without them, a producer can skip its wake while the
consumer reads stale queue state and parks. Release/acquire alone is not
enough for this store-then-load handoff across queue and signal atomics.

When a budget expires and work is known to remain, `reschedule()` notifies
unconditionally. It must not rely on another producer transition to wake the
consumer. Wire slots, send pipes, and fan-out lanes use this discipline.

### StateSignal: something changed

Space availability and route changes use a generation counter plus `Notify`:

1. Capture the generation before attempting the operation.
2. Enable the waiter and recheck readiness or closure.
3. Sleep only if no relevant state changed. Retry after a change.

Worker exit must wake blocked senders, and senders must check exit before
parking. `BlockingRecvWaker` similarly avoids `unpark()` and its thread mutex
while the application is active; waiter registration closes the sleep race.

Capacity notifications are batched separately from data notifications.
Fan-in returns [slot credits](#capacity-credits-are-not-messages) in groups;
transmit slots notify space/reactivate fan-out peers after crossing low-water
marks. Neither optimization permits dropping a required wake.

### Concurrency checks

[Signal Loom tests](../omq-tokio/tests/loom_signal.rs) cover rearming, the fenced
skip-wake handoff, generation checks, space release, and route activation.
The unfenced negative control demonstrates the stranded-message race.

Queue-level Loom/Miri checks live in the external queue workspace.
`omq_soak_driver_control` exercises bounded close with a saturated data inbox
and stalled transport. Fairness, reconnect, linger, and backpressure also have
integration tests in `omq-tokio/tests/`.

## Other execution paths

### Inproc

Inproc transfers owned messages without ZMTP framing or kernel transport I/O.
Cross-thread peers use send pipes and `inproc_peer_driver`; eligible same-thread
paths use direct `yring::ProducerOwner` access. HWM, fairness, and
connect-before-bind still apply.

Names belong to `ContextCore`, not the process. Independent contexts can bind
the same name. Handles imported through `from_share_key()` share the namespace,
including across language bindings in one process.

### Caller-driven exclusive sockets

`omq_tokio::exclusive::Socket` is a separate, opt-in API. The caller owns the
TCP stream and codec through `&mut self`; there is no connection-driver task,
background progress, multi-peer routing, or userspace outbound queue.

It supports TCP bind/connect for PAIR, DEALER, ROUTER, REQ, REP, CLIENT, and
SERVER with NULL authentication, accepting one peer at a time. Unsupported
types/transports return configuration errors.

Idle applications must call `maintain()` for reconnect and heartbeat work.
Failed sends are not automatically replayed: the peer may have received a
partial or complete command. This contract differs from the regular socket API.

### Proxy

`Proxy` composes two sockets rather than introducing a socket type or another
forwarding queue. It retains at most one pending message per direction when
the target is full, retrying that message before reading more from its source.

The default burst is 64 messages before rechecking control and reverse traffic.
Socket HWMs and routing/drop policies remain authoritative. Capture uses
best-effort nonblocking copies and cannot backpressure forwarding.

Steerable control supports `PAUSE`, `RESUME`, `TERMINATE`, and `KILL`, not
`STATISTICS`.

### C and language boundaries

The C layer preserves libzmq's externally serialized socket ownership contract;
native Tokio sockets allow shared async handles. C's `LocalCell` avoids a
socket-state mutex under that contract. Debug builds check thread ownership.

The same performance rules apply at binding boundaries:

- Try ready operations before entering a runtime or OS wait. Direct receive
  sinks and registered waiters avoid extra relay and registration work.
- Borrow native parts when copying to caller-provided buffers. Do not create
  temporary `Bytes` merely to copy them again.
- Retain native payload owners for view-based receives where the language
  supports them. Converting to language-owned byte strings may still copy.
- Reuse batch buffers and combine native API crossings. A batch API can reduce
  call/object overhead without being payload-copy-free.

For example, Python `copy=False` views retain native ownership, while copying
to Python `bytes` creates new storage. Retained send buffers must stay valid
and unchanged for their documented lifetime. C `zmq_msg_t` has no native
`Message`-style inline payload representation.

## Transports, reconnects, and monitoring

| URI | Transport |
| --- | --- |
| `tcp://host:port` | TCP with `TCP_NODELAY` |
| `ipc:///path` | Unix stream or Windows named pipe |
| `inproc://name` | Context-local message transfer |
| `udp://host:port` | RADIO/DISH datagrams |
| `lz4+tcp://host:port` | TCP with LZ4 message transform |
| `zstd+tcp://host:port` | TCP with Zstd message transform |
| `ws://...`, `wss://...` | ZWS over WebSocket, optionally TLS |

Regular sockets supervise reconnects and replay subscriptions/groups. Peer
failure is handled internally; applications do not manage connection ordering.
Queued sends follow the routing, HWM, and linger rules described above.

NULL authentication is always available. PLAIN and CURVE are feature-gated
protocol mechanisms; LZ4, Zstd, and WS are transport features.

`Socket::monitor()` returns a stream of lifecycle events with owned `PeerInfo`
snapshots: listening, accept/connect, delayed connect, handshake, disconnect,
peer commands, and close.

## Source map

| Question | Start here |
| --- | --- |
| Who owns runtimes and threads? | [context.rs](../omq-tokio/src/context.rs) |
| What does a socket call do? | [handle.rs](../omq-tokio/src/socket/handle.rs), [blocking.rs](../omq-tokio/src/blocking.rs) |
| Who changes peers and lifecycle? | [socket/actor/](../omq-tokio/src/socket/actor/) |
| Where are queues drained and bytes written? | [engine/driver.rs](../omq-tokio/src/engine/driver.rs), [send_pipe.rs](../omq-tokio/src/engine/send_pipe.rs) |
| How are peers selected? | [routing/](../omq-tokio/src/routing/) |
| How are receives and credits managed? | [fanin.rs](../omq-tokio/src/socket/fanin.rs), [recv.rs](../omq-tokio/src/socket/recv.rs), [peer_recv.rs](../omq-tokio/src/socket/peer_recv.rs) |
| How are payloads stored and framed? | [message.rs](../omq-proto/src/message.rs), [frame_buffer.rs](../omq-proto/src/frame_buffer.rs) |
| What keeps wakeups and drains safe? | [signal.rs](../omq-tokio/src/engine/signal.rs), [flow.rs](../omq-proto/src/flow.rs) |
| Where are codec and transport rules? | [proto/connection/](../omq-proto/src/proto/connection/), [transport/](../omq-tokio/src/transport/), [LZ4 RFC](lz4-rfc.md) |
| How do I test or measure a change? | [DEVELOPMENT.md](../DEVELOPMENT.md), [perf-verification.md](perf-verification.md) |

Add socket types through protocol compatibility/routing and the matching
backend strategy. Add transports or mechanisms through endpoint/mechanism
parsing, backend support, and integration tests. The public socket contract is
checked by [coverage_matrix.rs](../omq-tokio/tests/coverage_matrix.rs).
