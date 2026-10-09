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

Driver-to-actor data uses bounded fanring lanes, one producer per connection.
The actor owns the receive drain and processes ready lanes fairly. Handshakes
and protocol commands use a separate control mailbox, so data backpressure
cannot hide connection management or shutdown. Delivery preserves each
connection's protocol ordering.

Actor-to-driver commands and application data have separate bounded queues.
Application handles enqueue raw messages; drivers own their encoding and
transmission. Closing a socket either drains accepted output within its linger
deadline or cancels the driver tasks.

A latency-profile plain-TCP route can perform one immediate nonblocking
vectored write from the caller: framed small messages and headers from the
slot arena, large payloads from their own buffers. The connection driver owns
any unfinished output.

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

Owned contexts distribute publications across fan-out lanes on their I/O
threads; borrowed contexts use one lane. Each lane matches
subscribers or groups before encoding. Peers with compatible codec settings
can share encoded output within that lane. Connection-specific transforms
run on the peer's driver.

```text
 caller -> fan-out lane -> match + encode -> peer slots -> drivers
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
per-destination producers consumed by the connection driver. A
replacement identity invalidates the old route and its queued receive data.

## Receive ownership and backpressure

PULL, GATHER, SUB, XSUB, and PEER own their receive queues. Each connection
has an independent bounded lane. Applications drain the lanes fairly, with
FIFO order within each connection. Socket clones share the receive drain;
application worker selection belongs to the application.

Each receive call owns the mutable drain state until its synchronous drain
ends. Atomic ownership transfer supports concurrent calls through one handle.
Multiple application handles also serialize receives with a shared mutex;
driver and endpoint references do not count as application handles.

PEER, PULL, and GATHER let the application claim a receive source while deciding
whether to admit its message. Receipt lifetime controls the claim; the receiving
socket enforces the pause and owns any returned message. A claim pauses that
connection's drainage while other sources, outbound traffic, and control work
continue. Claims belong to a physical
connection generation, so a reconnect cannot inherit a held message.

Backpressure propagates through bounded queues. A full connection lane stops
transport reads; transport buffers and sender queues then fill. Resuming the
source allows drainage and frees queue capacity. PEER bounds retained memory
per source; PULL and GATHER use message-count HWMs. Socket memory also includes
held messages and transport buffers.

The [receive backpressure contract](receive-backpressure.md) describes source
receipts, retries, and socket-type constraints.

Round-robin and exclusive sends normally wait for space when mute; nonblocking
sends report `Full`. Fan-out sockets apply their configured drop or block
policy. Peer disconnects and slow consumers do not turn ordinary sends into
peer errors. Regular sockets reconnect and replay subscriptions or groups.

Socket close stops new sends. Nonzero linger lets accepted output drain under
one socket-wide deadline when finite; queue drain signals wake the actor as
accepted output clears. Zero linger cancels immediately. Removing
a message from a queue does not complete its wire write. Wire completion does
not prove remote application delivery. Heartbeats judge missing peer activity,
not a slow application queue.

## Message storage and wire work

Small message bodies can live inline in `Message` or `Payload`. Larger bodies
use shared storage; cloning a message need not copy its payload. Multipart
messages retain part descriptors and shared body owners.

Applications explicitly create transport-independent `PayloadPool` handles
with fixed storage classes. Message construction selects inline storage or
the smallest fitting available class, with an owned allocation fallback.
Multipart parts select independently; `MessagePool` caches frame tables.
`Options::recv_payload_pool` supplies socket-wide receive storage. Configuration
freezes before the first bind/connect; inproc transfers existing owners.
Final owners return slots; overlapping releases use bounded reclamation.

`FrameBuffer` owns encoded headers and small bodies. Large bodies use shared
chunks for gather writes. Drivers retain unfinished output until its wire
write completes. Received storage passes from the transport through the codec
to the application message.

Compression transforms complete messages before ZMTP framing. Peer-routed
sends encode on their connection driver, which can delegate compute work to
worker tasks. Fan-out lanes encode independently and share results with
compatible peers. Dictionary shipment is ordered and scoped to each connection.
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

`DataSignal` coalesces data-available notifications. `StateSignal` reports
capacity and route changes. Signals schedule work; queues own the messages.

## Other execution paths

### Inproc

Inproc transfers owned messages without ZMTP framing or kernel I/O. The
receiving socket owns a bounded queue for each connection direction, sized
from the sender's send HWM and receiver's receive HWM. Direct sends enqueue on
the sender's calling thread; application receives drain on the receiver's
calling thread. PULL/GATHER drain their inproc queues through the same fanring
receiver as wire connections.

```text
 sender thread                              receiver thread

 send(Message) -> routing -> [ bounded connection queue ] -> recv()
```

The receive path applies socket-type routing and request/reply rules. ROUTER
messages carry the peer identity, SERVER messages carry a routing ID, and REP
admits the saved reply route when the application receives the request.

Connection setup, subscription commands, and lifecycle management run on the
context runtime. Relay paths, including PEER and conflate, forward messages
through peer tasks. Their data and control queues are separate, so a blocked
application receive does not prevent cancellation or shutdown.

Connect-side sends queue until the peer binds, then enter the connection queue
in order. Fan-out senders match subscriptions on the calling thread and enqueue
into each inproc subscriber's queue. Contexts have separate inproc namespaces.

### Caller-driven exclusive sockets

`omq_tokio::exclusive::Socket` is a separate API. The caller owns one TCP
stream and codec through `&mut self`, without a connection-driver task or
background progress. The caller must invoke `maintain()` for idle reconnect
and heartbeat work. Its send and replay contract differs from regular sockets.

### Proxy

`Proxy` composes two sockets without another socket type or unbounded
forwarding queue. When the target is full, a direction keeps one pending
message in the target's own send, which completes once that message is
accepted, before reading more from that source. The other direction and the
control socket keep running. Socket HWM and routing policy govern forwarding.

### Send readiness

`Socket::send_ready` and `wait_send_ready` follow libzmq `ZMQ_POLLOUT`. Each
send strategy probes the queues its `try_send` would use, and a full queue
arms its space wake as a failed send does. The wait snapshots the space and
topology signals before probing, so no change is lost. `zmq_poll`,
`ZMQ_EVENTS`, and the Python `Poller` use it.

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
| `quic://` | ZMTP byte stream over Quinn with TLS |
| `inproc://` | Context-local message transfer without ZMTP |
| `udp://` | RADIO/DISH datagrams |
| `dart://` | Reliable ordered messages over UDP |
| `lz4+...`, `zstd+...` | Message transforms over supported carriers |

STREAM uses raw TCP without a ZMTP handshake. Regular byte-stream sockets
supervise reconnects. `Socket::monitor()` exposes lifecycle events and peer
snapshots to applications.

QUIC peers retain their UDP endpoint group and assigned data runtime. The
listener controls new admission; established peers own their connections.
A separate liveness stream keeps heartbeats independent of application
receive backpressure. See [quic-rfc.md](quic-rfc.md) for the native protocol.

Each Dart endpoint task drives all peers' sans-I/O sessions and owns its
deadline timer (monotonic timerfd on Linux). Sessions retain
outbound bodies until receipt acknowledgment and enforce bounded receive
credit. Peers share explicit receive payload storage when configured.
Final owned and pooled bodies return credit to their original
session when storage is reusable; inline values return credit after bounded
queue delivery. Fragment chunks can use the configured payload classes.
Fragmented bodies reserve their full allocation after length validation;
intermediate credits return during assembly, and the final credit follows
the complete body's last owner. See [dart-rfc.md](dart-rfc.md).

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
| How do I test or measure a change? | [DEVELOPMENT.md](../DEVELOPMENT.md), [RUNNING_BENCHMARKS.md](../RUNNING_BENCHMARKS.md), [perf-verification.md](perf-verification.md) |
