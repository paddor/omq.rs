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

A latency-profile plain-TCP route may attempt one immediate nonblocking write
from the caller after normal send admission. A partial write transfers its
remaining output to the driver. This is a specialized path; routing and queue
ownership still follow the socket's ordinary rules.

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
