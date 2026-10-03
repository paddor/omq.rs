# Architecture

OMQ sockets exchange complete messages. Applications choose a transport by URI;
the socket owns routing, connections, framing, and backpressure. This page maps
component ownership and message paths. Protocol details live in the transport
RFCs; transport limits and parser behavior live in [WebSocket transport
limits](zws.md) and the source.

## Components

```text
 Rust application        C application       language bindings
        |                      |                    |
        |                 omq-libzmq                |
        +----------------------+--------------------+
                               |
                           omq-tokio
                 sockets, routing, tasks, transports
                         /             \
                    omq-proto      yring / fanring
                  codec + values   message queues
```

| Component | Responsibility |
| --- | --- |
| `omq-proto` | Sans-I/O ZMTP codec, messages, options, mechanisms, and transforms |
| `omq-tokio` | Async and blocking sockets, routing, I/O drivers, and reconnects |
| `omq-libzmq` | libzmq-compatible C API and its socket ownership rules |
| `bindings/` | Language-specific ownership and value conversion |
| `omq-bench` | Benchmark peers, result storage, and charts |

`Connection` in `omq-proto` consumes bytes and emits decoded events or wire
output. It never owns a socket or runtime. `omq-tokio` decides when to read,
write, and deliver those events. The queue crates are maintained in the
separate [fanring.rs workspace](https://github.com/paddor/fanring.rs).

## Runtime and task ownership

`ContextCore` owns the runtime resources, socket actors, and the context's
`inproc://` namespace. `Context` handles share that core.

| Context construction | Runtime placement | Fan-out lanes |
| --- | --- | --- |
| `Context::new()` | One background Tokio thread for control and I/O | One |
| `Context::with_config()` with multiple I/O threads | One control runtime plus data runtimes | One per data runtime |
| `Context::current()` | Borrowed caller runtime; no owned threads | One |

Owned data runtimes have separate reactors and do not steal tasks from one
another. The socket actor runs on the control runtime. Regular connection
drivers and fan-out lanes run on assigned data runtimes; raw STREAM runs with
the actor. With one I/O thread, control and data share that thread. A borrowed
context runs its tasks on the caller's runtime.

```text
 ContextCore
   |-- socket actors: peers, endpoints, socket-type state
   |-- connection drivers: one per materialized peer
   |-- fan-out lanes: publication matching and encoding
   +-- inproc namespace
```

Each byte-stream connection driver owns one peer's wire I/O and codec state.
Drivers keep their runtime assignment for their lifetime. Socket actors
supervise connection setup, shutdown, and peer replacement. Transport handles
move to the assigned reactor before driver I/O begins.

Async and blocking sockets use the same backend. Blocking calls enter it
through a context-owned runtime; `Context::current()` instead borrows the
application runtime. Sharing a context across bindings shares its socket
resources and inproc namespace without changing binding-level ownership rules.

## Message paths

For an ordinary TCP PUSH/PULL exchange:

```text
 Sending process                       Receiving process

 application send(Message)               application recv() -> Message
          |                                         ^
     send pipe                                  receive queue
          |                                         ^
   ConnectionDriver                         ConnectionDriver
   encode + frame                             read + decode
          |                                         ^
          +---------------- TCP -------------------+

 SocketDriver manages peers and lifecycle alongside both data paths.
```

The socket actor owns the peer table, endpoint lifecycle, identities,
subscriptions, groups, and monitor events. It does not process every payload.
The caller normally enqueues an unencoded `Message`; connection drivers or
fan-out lanes perform encoding, compression, and wire I/O. Receive paths that
need actor-owned routing state, including REP and ROUTER, pass through the
actor. Other receive paths can deliver to the socket queue directly.

Socket-owned drivers publish decoded messages through one bounded fanring lane
per driver, registered when the connection is materialized. The actor owns the
Coordinated receiver and shares no producer among drivers. Lane capacity is
256 messages; fan-in rotates between ready lanes while preserving each
driver's FIFO. The actor polls async readiness for the first item, then drains
under 64-message and 64-KiB budgets, with time checks, before checking control
again. A complete final message may cross the byte limit. The actor releases
partial credits before ending that drain. Async receives themselves
release consumed credits on each poll; native blocking receive LWM batching
does not carry over automatically.

Codec handshake and command events use a separate bounded control mailbox.
The actor checks it under message, byte, and time budgets each iteration,
including while an application receive waits for space. Driver data records
carry their admitted protocol prefix, so observing separate queue publications
cannot deliver a message before its preceding protocol events. Reserved peer
completions count both queues and retain peer routing state until their
admitted data and pending application receives finish.

XPUB subscription state is control; its application notification uses the
driver's data lane. The sole producer checks notification capacity before
publishing the command and notification together, without an intervening
await. A full notification lane pauses that peer's input while other drivers'
handshakes and subscriptions remain reachable. The actor retains at most one
data record awaiting its protocol prefix and one application receive awaiting
space. Standalone public connection drivers retain their supplied Tokio event
queue and combined event ordering.

Actor-to-driver protocol inboxes use one bounded Coordinated fanring producer
with 64 command slots. Internal handle copies share that physical producer.
One activation slot and one close slot keep lifecycle commands reachable when
protocol forwarding is blocked. Graceful close drains accepted protocol
commands; immediate close preempts them. Standalone driver inputs retain their
caller-supplied Tokio channels. The shared actor control mailbox, authenticated
receive queues, context jobs, and multi-producer inproc registry requests remain
on Tokio.

A Linux VM comparison used 64-byte ROUTER/DEALER traffic, HWM 1000, and two
current-thread runtimes pinned to separate CPUs. Three alternating serial
pairs compared the prior shared Tokio mailbox with the batched per-driver
lanes. Throughput used three-second windows after warmup and included tail
delivery; latency used 10,000 echo round trips after 2,000 warmup trips.
Dependency versions matched. Medians were:

| Transport | Throughput before / after | CPU us/message before / after | RTT p99 before / after |
| --- | --- | --- | --- |
| TCP | 1.026 / 1.433 M/s | 1.170 / 0.878 | 64.958 / 66.399 us |
| WS | 0.905 / 1.222 M/s | 1.327 / 1.026 | 66.954 / 68.145 us |

These are local measurements. The throughput gain does not imply lower
round-trip latency or Windows performance.

A latency-profile plain-TCP route may attempt one immediate nonblocking write
from the caller after normal send admission. A partial write transfers its
remaining output to the driver. This is a specialized path; routing and queue
ownership still follow the socket's ordinary rules.

Fallback driver data inboxes use Coordinated fanring queues. Each socket clone
lazily registers one producer per destination. Each lane holds at most
`min(64, send_hwm)` messages, including reservations; non-power-of-two HWMs
remain exact. Each destination allows 64 producer lanes plus its unused
registrar. Retired lanes count until the driver reclaims them. A clone prunes
closed destinations when adding a new cache entry.

Publication reserves every required fallback lane before enqueuing anything.
Each reservation holds its producer lock through commit, so another send
through that clone cannot take the checked slot. Capacity waits register with
the actual producer's `poll_ready` and observe close and route replacement.
Graceful close stops admission and drains accepted messages and reservations;
immediate teardown destroys unread payloads even if idle clones remain alive.
Public standalone drivers retain their supplied Tokio inboxes. Internal
binding and proxy handle copies share the original send scope to preserve
direct/fallback FIFO and wait on the same capacity as their sends.

A serial Linux VM comparison against the preceding actor-lane implementation
used the same 64-byte messages, HWM 1000, pinned runtimes, warmup, and confirmed
delivery barriers. Three alternating pairs gave these medians. The forced
fallback echo used a latency-profile ROUTER and a throughput-profile DEALER;
TCP ran for 10 seconds and WS for 3 seconds. One-way throughput and ping-pong
latency kept both sockets in the throughput profile.

| Probe | Before | After |
| --- | --- | --- |
| Forced TCP echo | 92.956 k/s | 89.612 k/s |
| Forced WS echo | 342.982 k/s | 328.755 k/s |
| TCP one-way throughput | 1.432 M/s | 1.419 M/s |
| WS one-way throughput | 1.234 M/s | 1.218 M/s |
| TCP ping-pong p99 | 68.162 us | 66.831 us |
| WS ping-pong p99 | 67.440 us | 68.149 us |

The fallback lane model costs 3.6% TCP and 4.1% WS throughput in these forced
probes. CPU per message rose 4.7% and 3.5%; TCP switches per message rose 21.8%,
with substantial variation between runs. TCP profiles were dominated by
kernel socket locks. This migration provides separate producer capacity and
bounded registration; these measurements do not establish a speedup or
Windows performance.

## Routing and fan-out

| Policy | Socket types | Destination |
| --- | --- | --- |
| Round-robin | PUSH, DEALER, REQ, CLIENT, SCATTER | One eligible peer |
| Fan-out | PUB, XPUB, RADIO | Matching subscribers or groups |
| Identity or reply route | ROUTER, REP, SERVER, PEER | Selected peer or saved request route |
| Exclusive | PAIR, CHANNEL | One peer |

Round-robin byte-stream peers have bounded per-peer send pipes; inproc peers
push into their connection's ring (see [Inproc](#inproc)). Connect-side pipes
exist before the remote endpoint binds, so messages sent before READY retain
their order through handshake and reconnect. A bind-side
socket without a ready pipe is mute. HWM limits each pipe's queued messages;
it does not cap total socket memory.

Fan-out sends enter lane 0. Owned contexts distribute publications across
lanes on their I/O threads; borrowed contexts use one lane. Each lane matches
subscribers or groups before encoding. Peers with compatible codec settings
can share encoded output within that lane. Connection-specific transforms
remain with the peer's driver.

```text
 caller -> lane 0 -> match + encode -> peer slots -> drivers
                    |
                    +-> other lanes -> match + encode -> peer slots

 socket actor -> separate control channels -> lanes
```

A slow subscriber cannot stop delivery to other subscribers. Fan-out drops
for muted peers by default; `xpub_nodrop` enables PUB/XPUB backpressure.
Control commands use separate channels so data backlog cannot hide peer
changes or shutdown.

ROUTER selects peers by identity, SERVER by routing ID, and REP by the saved
request route. PEER combines identity routing in both directions. Socket
clones share the receive drain; outbound PEER sends use per-clone,
per-destination producers consumed by the existing connection driver. A
replacement identity invalidates the old route and its queued receive data.

## Receive ownership and backpressure

PULL, GATHER, SUB, and XSUB use socket-owned fan-in queues with a producer for
each connection. The application drains them fairly while preserving order
within each peer. PEER uses the same socket-owned receive model; application
workers choose how to dispatch received identities.

A receive queue holds `Message` values, not notifications. Consumed slots
return capacity to producers. Capacity updates can be batched, but a receiver
must publish available space before parking. A full application queue pauses
further inbound data while the driver remains able to service local control,
outbound writes, cancellation, and close.

Round-robin and exclusive sends normally wait for space when mute; nonblocking
sends report `Full`. Fan-out sockets apply their configured drop or block
policy. Peer disconnects and slow consumers do not turn ordinary sends into
peer errors. Regular sockets reconnect and replay subscriptions or groups.

Socket close stops new sends. Nonzero linger lets accepted output drain under
one socket-wide deadline when finite; zero linger cancels immediately. Removing
a message from a queue does not complete its wire write. Wire completion does
not prove remote application delivery. Heartbeats judge missing peer activity,
not a slow application queue.

## Message storage and wire work

Small message bodies can live inline in `Message` or `Payload`. Larger bodies
use shared storage; cloning a message need not copy its payload. Multipart
messages retain part descriptors and shared body owners.

`FrameBuffer` stores headers and small bodies in an arena. Large bodies remain
external shared chunks for gather writes. Partial writes retain their chunks
and offsets until completion. Native byte-stream receives can hand owned read
storage to the decoder; direct reads into final payload storage are available
for eligible large frames. Transforms and WebSocket masking can still require
copies.

Compression transforms complete messages before ZMTP framing. Peer-routed
sends encode on their connection driver, with optional offload for larger
work. Fan-out lanes encode independently and share results with compatible
peers. Dictionary shipment remains ordered and scoped to each connection.
The [LZ4 RFC](lz4-rfc.md) and [Zstd RFC](zstd-rfc.md) define their wire formats.

Endpoint URIs select carriers and optional transforms. Bind/connect capture
the effective options for that operation; reconnect uses the same captured
configuration. Changing the C binding's option overlay does not reconfigure
active connections. Codec, mechanism, and carrier compatibility are checked
before connection setup.

## Scheduling and signals

Data paths drain bounded batches so actors and drivers revisit control,
shutdown, and the opposite I/O direction. Control commands have separate
queues and are serviced regardless of data volume. A full receive queue or
actor mailbox keeps its pending item owned while the task waits for capacity;
retries preserve message order and do not repeat decoding.

`DataSignal` coalesces producer wakeups for send pipes, transmit slots, and
fan-out lanes. Consumers clear and rearm it around each bounded drain; work
remaining after a budget expires schedules another turn. `StateSignal` tracks
capacity and route changes with a generation counter so waiters cannot sleep
through a change. Neither signal owns messages.

## Other execution paths

### Inproc

Inproc transfers owned messages without ZMTP framing or kernel I/O. Each
direction of a connection is one `yring` that holds the sender's send HWM plus
the receiver's receive HWM. `send` pushes into it on the calling thread, and
the receiving socket's `recv` drains it, so no I/O thread touches a message
once the peers are connected. The receive path applies the socket type's rules
as it drains: ROUTER messages carry the peer identity, SERVER messages the
routing ID. REP retains the complete request plus its peer route in the
same queue item, then splits the envelope and admits the reply route at
application receive. Compatibility receive relays forward these items without
advancing REQ/REP state.

Native drains publish popped credits at the ring's LWM or when their cached
window ends. Full-producer wake hints avoid unnecessary capacity broadcasts.
Bulk budget boundaries and empty drains release partial credits before handing
control elsewhere. Ring release policy stays in OMQ; yring only reports wakes.
Blocking receivers register individual OS-thread waiters after an empty drain.
Concurrent socket clones cannot replace one another's waiter; ready receives
skip registration.

```text
 sender thread                         receiver thread

 send(Message) -> routing -> [ yring: send HWM + receive HWM ] -> recv()
```

A connect-side socket queues sends in its pre-ready pipe until the peer binds.
Those messages move into the ring first, in order. Peer tasks still exchange
commands (SUBSCRIBE, JOIN) and report connection state. Fan-out senders (PUB,
XPUB, RADIO) match subscriptions on the calling thread and push into each
inproc subscriber's ring in turn; fan-out lanes serve wire peers only. With
`xpub_nodrop`, `send` waits for each full subscriber separately. Senders with
`conflate` keep their own send queue and their peer task relays each message
into the same ring. PEER connections and authenticated C-API sockets still run
through their peer tasks.

Each inproc direction has separate bounded fanring command and relay-data
lanes, with one physical producer each and 1024 slots per lane. Forwarding
retains at most one pending command and one bounded data batch. Incoming
application backpressure retains one message while local lifecycle commands,
remote protocol commands, cancellation, and deadlines remain selected.
XPUB notifications wait behind an older pending application message on the
same actor data lane. `InprocConn` exposes `RelaySender`/`RelayReceiver` with
parsed-frame send/receive methods.

These async fanring receives release consumed credits per call. They do not
apply the native direct yring LWM policy. Bounded synchronous drains explicitly
release partial credits before yielding. Finite native close starts its caller
deadline before actor command admission; expiry cancels the driver tree even
when a subscription handler is blocked on protocol capacity.

A matched Linux comparison measured the control migration against the prior
commit, using three alternating serial pairs and 64-byte messages. TCP/WS
throughput used three-second windows at HWM 1000. RTT used 10,000 measured
round trips after 2,000 warmups. PEER relay RTT used two application runtimes
pinned to separate CPUs sharing one owned IO thread. Direct blocking inproc
used two application threads restricted to CPUs 0/1, with HWM 8 or 1000.
Dependency versions matched. Medians were:

| Gate | Before | After |
| --- | --- | --- |
| TCP throughput | 1.395 M/s | 1.425 M/s |
| WS throughput | 1.210 M/s | 1.215 M/s |
| TCP RTT p99 | 68.650 us | 68.486 us |
| WS RTT p99 | 68.286 us | 70.751 us |
| PEER inproc relay RTT p50 / p99 | 49.470 / 58.400 us | 53.935 / 62.912 us |
| Direct inproc throughput, HWM 1000 | 6.901 M/s | 6.909 M/s |
| Direct inproc throughput, HWM 8 | 3.222 M/s | 3.647 M/s |

The relay model costs 9.0% p50, 7.7% p99, and 9.4% CPU per round trip in
this comparison; WS p99 rises 3.6%. Small-ring direct results vary across
runs. These measurements do not establish a relay speedup. Matched relay
profiles had no lost samples and showed syscall, wake-registration, and
queue-readiness costs. Windows runtime measurements remain pending PR CI.

A blocking `send` that finds its queue full waits on the calling thread and is
woken by the peer that frees space. Blocking `bind`, `connect`, and the other
control calls still run on the context's IO thread.

Connection setup still needs a tokio runtime: the socket actor, `bind`,
`connect`, peer tasks, and subscription commands run as tasks. A `Context`
with zero IO threads borrows the caller's runtime instead of starting a
thread; the blocking API needs at least one owned IO thread. The C API treats
`ZMQ_IO_THREADS` set to 0 as one IO thread. Direct inproc paths use the
calling threads, including C API inproc REQ/REP with the direct receive sink.
Additional peers use receive relays when that sink is occupied. The IO
thread serves those fallback paths, setup, and the control plane.

HWM, fairness, and connect-before-bind still apply. Names belong to a context,
so separate contexts may bind the same name.

### Caller-driven exclusive sockets

`omq_tokio::exclusive::Socket` is a separate API. The caller owns one TCP
stream and codec through `&mut self`, without a connection-driver task or
background progress. The caller must invoke `maintain()` for idle reconnect
and heartbeat work. Its send and replay contract differs from regular sockets.

### Proxy

`Proxy` composes two sockets without another socket type or unbounded
forwarding queue. It retains a pending message in each direction when the
target is full and retries it before reading more from that source. Socket
HWM and routing policy remain authoritative.

### C and language bindings

The C API keeps libzmq's externally serialized socket ownership. Native
Tokio socket handles can be shared by async callers. Bindings convert values
at their API boundary and may either copy payload bytes or retain native
owners for views. Batch APIs reduce call overhead; they do not imply
copy-free payload conversion.

## Transports and monitoring

| URI family | Path |
| --- | --- |
| `tcp://`, `ipc://` | Byte streams with ZMTP framing |
| `ws://`, `wss://` | ZWS over WebSocket, optionally TLS |
| `inproc://` | Context-local message transfer without ZMTP |
| `udp://` | RADIO/DISH datagrams |
| `lz4+...`, `zstd+...` | Message transforms over supported carriers |

STREAM uses raw TCP without a ZMTP handshake. Regular byte-stream sockets
supervise reconnects. `Socket::monitor()` exposes lifecycle events and peer
snapshots to applications.

## Source map

| Question | Start here |
| --- | --- |
| Who owns runtimes and threads? | [context.rs](../omq-tokio/src/context.rs) |
| What does a socket call do? | [handle.rs](../omq-tokio/src/socket/handle.rs), [blocking.rs](../omq-tokio/src/blocking.rs) |
| Who manages peers and lifecycle? | [socket/actor/](../omq-tokio/src/socket/actor/) |
| Where are queues drained and bytes written? | [engine/](../omq-tokio/src/engine/) |
| How are peers selected? | [routing/](../omq-tokio/src/routing/) |
| How are receives managed? | [fanin.rs](../omq-tokio/src/socket/fanin.rs), [recv.rs](../omq-tokio/src/socket/recv.rs), [peer_recv.rs](../omq-tokio/src/socket/peer_recv.rs) |
| How are payloads stored and framed? | [message.rs](../omq-proto/src/message.rs), [frame_buffer.rs](../omq-proto/src/frame_buffer.rs) |
| Where are codec and transport rules? | [proto/connection/](../omq-proto/src/proto/connection/), [transport/](../omq-tokio/src/transport/) |
| How do I test or measure a change? | [DEVELOPMENT.md](../DEVELOPMENT.md), [perf-verification.md](perf-verification.md) |
