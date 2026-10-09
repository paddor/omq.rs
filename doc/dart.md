# Native Dart sockets

Enable the `dart` Cargo feature on `omq-tokio`. It uses `quinn-udp` without
QUIC, TLS, or a C compiler. Async and blocking Rust sockets share the same
transport and buffer API. The libzmq compatibility API rejects `dart://`.

Dart delivers ordered, deduplicated messages within a live session, with
receipt feedback, bounded receive credits, retained retransmissions, and
silent backpressure. Receipt acknowledgment confirms remote storage, not
application consumption. Session retirement follows normal socket reconnect
semantics; it does not provide durable delivery across process restarts.
There is no authentication or encryption. Large messages use ordered fragments. The
[RFC](dart-rfc.md) specifies the wire format.

## Socket pairs and limits

| Pair | Routing |
| --- | --- |
| SCATTER/GATHER | Round-robin send, fair local receive |
| CLIENT/SERVER | SERVER replies with the received local routing ID |
| PEER/PEER | `send_to` selects a READY identity |
| RADIO/DISH | RADIO broadcasts; DISH filters joined groups locally |
| CHANNEL/CHANNEL | One live peer |

Bind or connect to `dart://host:port`. DNS and IPv4/IPv6 are supported.
Bind port zero chooses a local port; connect needs a concrete address and
nonzero port. Endpoints are unicast. Other socket types are rejected.

Each message has one application body; fragmented bodies advertise a u64 length.
PEER identity and RADIO group prefixes are routing metadata. RADIO groups
occupy 1 to 255 bytes. Small
bodies use DATA packets; larger bodies use FIRST (`0x81`, with the full u64
body length) and consecutive CONT (`0x82`) packets. DATA and fragments stay
within 1200 bytes. Fragment sizes account for FIRST's extra metadata.
When other messages are queued, the sender uses at most one extra fragment
to equalize all wire lengths when possible, allowing GSO/GRO batches to span
message boundaries. Isolated messages use the minimum fragment count.
The receiver checks `max_message_size` before reserving the complete body,
fills it incrementally, and delivers it atomically. There is no 64 KiB cap.
Multipart application bodies fail before enqueueing.

Data datagrams opportunistically pack up to 128 already queued messages for
one peer, within a 1452-byte limit. A count byte and one byte per payload
form the length table; payloads are concatenated after the session header.
Payloads over 255 bytes, including group metadata, use individual DATA
datagrams. There are no length escapes. An isolated message uses DATA framing and
flushes immediately; packing never waits for more messages. GSO/GRO can
also batch complete datagrams. Both peers must use Dart protocol version 1.

A challenge-confirmed READY exchange admits compatible peers. Connect retries
before a listener exists. Valid traffic and feedback/probes refresh peer
liveness, including when the application holds all receive storage. Expired
peers can return automatically.
`wait_connected` confirms local admission; it does not guarantee delivery.

## Preparing messages

Use ordinary `Socket` construction, bind/connect, and send/receive methods.
Buffer preparation is transport-independent. Create an optional standalone
pool with `BufferPool::new(bytes_per_buffer, count)` and reuse its handle:

```rust
use omq_tokio::{BufferPool, Socket};

async fn publish(scatter: &Socket, pool: &BufferPool, data: &[u8]) -> omq_tokio::Result<bool> {
    let Some(message) = pool.try_message(data.len(), |body| {
        body.copy_from_slice(data);
    })? else {
        return Ok(false); // Existing owners hold every buffer. Retry later.
    };
    scatter.send(message).await?;
    Ok(true)
}
```

`try_message` stores bodies up to 55 bytes inline without a pool checkout or
allocation. Larger bodies use pooled storage when they fit; `Ok(None)` means
the pool is exhausted. Bodies exceeding its buffer size own a heap allocation.
Pool buffers never grow. Dart fragmentation happens on the IO task above the
1 KiB wire body limit.

`pool.try_take()` exposes fixed writable storage for manual filling; set its
length before `into_message`. This consumes writable access and transfers the
body into an immutable message without copying. Pool clones share storage;
they have no socket lifecycle dependency. Batch checkout and
`BufferPool::recycle_many` are bounded by count and bytes. Concurrent clone
releases use bounded deferred reclamation without per-slot locks.

`try_send` returns the original message on `Full`. Retain and retry it.
`recv` returns an immutable `Message`; ungrouped bodies up to 55 bytes are
inline, except PEER bodies, which use pooled storage for their identity prefix.
Bodies up to 2 KiB normally use the receive pool, including fragment
assembly. Larger bodies own their allocation.
Borrow with `part_slice`.
Message clones and byte views retain their storage until the last owner
is dropped. Buffers and messages may outlive a closed socket. Creating an
independently owned `Bytes` adapter can allocate; borrowing does not.

For SERVER, pass the received message back to `send` to keep its routing ID,
or attach that ID to a new body with `with_routing_id`. For PEER, use
`send_to(identity, body)`. For RADIO, prepend a group with
`Message::with_prefix(group, body)`; DISH receives group and body parts.

## Capacity and spinning

Configure `Options::dart` before constructing the socket:

| Setting | Default | Meaning |
| --- | --- | --- |
| `pool_buffers` | 8192 | Internal receive buffers per socket; 2 KiB each, must be nonzero |
| `window_messages` | 256 | Receive and retention positions per peer; power of two, at most 65536 |
| `max_ready_peers` | 1024 | Ready peer cap across all Dart endpoints |
| `io_spin` | Zero | At most 50 microseconds; `Duration::MAX` polls continuously |
| `congestion` | Adaptive | Congestion window, pacing, and validated ECN; LAN uses fixed credit |
| `ecn` | Auto | Enable adaptive ECT(0) when the carrier supports feedback; Disabled opts out |
| `max_send_rate` | None | Optional bytes per second cap, including repairs |

`pool_buffers` also bounds simultaneous blocked native RADIO publications
per sender scope. Queued and unacknowledged messages retain send admission.
Full receive windows stop new transmissions until storage is reusable.
All peers share the socket's internal receive pool, separate from application
send pools. The default receive pool reserves 16 MiB of body storage plus
metadata. Pooled credit returns with the last owner; inline credit returns
when the bounded application queue accepts its independent message value.
Control traffic continues while receive credit is closed.
Receive ownership remains bounded by peer windows and socket HWM. If the
shared pool is exhausted, owned receive allocations allow gap repair to
progress without waiting for other peers' retained pooled storage.
For large messages, intermediate fragment slots return credit as the reserved
body fills. The final slot remains charged until the assembled body's last
clone or byte view is dropped, so even a one-slot window can carry large bodies.

RADIO's ordinary mute policy may drop new publications before sequencing;
messages already sequenced remain retained for recovery. `xpub_nodrop` waits
for admission. Source pausing stops application drainage and eventually closes
that source's receive window without blocking other peers.

Use `DartCongestion::Lan` only for provisioned private paths. LAN keeps
reliable recovery and receiver credit, disables outbound ECT and adaptive
congestion control, and honors an optional byte rate cap. The adaptive policy
is the default. Congestion state is per peer, without aggregate control across
separate endpoints or sockets.

Application `recv_spin` and transport `io_spin` are separate settings.
Both default off. IO spinning polls incoming datagrams and newly queued
sends. Idle workers return to readiness waits after the configured window.
Set either budget to `Duration::MAX` to poll continuously, including while
idle. Application polling still stops on close, timeout, or cancellation;
IO polling still yields bounded turns to other runtime tasks. Reserve separate
CPU resources for each continuously polling thread.
Explicit IO polling uses bounded cooperative turns for every workload profile.
With `WorkloadProfile::Latency`, both spinning and readiness receives use one
buffer, which can hold a GRO aggregate. Other profiles receive into batches.
The latency profile publishes each complete receive batch before probing
for more datagrams; other profiles publish at the end of the bounded turn.
During explicit latency spinning, the endpoint polls directly between bounded
cooperative yields and processes received packets in that same turn. Malformed
traffic switches to bounded batch drainage to
preserve handshake progress. Spin uses CPU; control and other tasks continue
to receive turns.
Blocking latency receives probe a single Dart ring directly during spinning.
They share spin and receive timeout checks, reading time once per eight probes.
Empty probes inspect atomic notifications without acquiring receive ownership.
One application handle transfers receive ownership atomically; multiple socket
handles also serialize through a mutex.
Preparing to park still uses the ordinary signal drain and queue recheck.
GATHER and DISH use fanring's ready-lane notifications; only unsignaled inproc
producers require full lane polling.
Each socket admits at most 128 Dart endpoints. CHANNEL admits one peer.
SERVER routing IDs are nonzero 32-bit values. Its monotonic socket counter
does not reuse IDs; exhaustion stops new route admission.

## Diagnostics

`dart_stats` reports approximate socket-wide message, ACK, retransmission,
duplicate, reorder, credit/congestion stall, invalid datagram, pool exhaustion,
IO failure, and ECN counters. Application sequence tags are still needed to
verify end-to-end benchmark delivery.

`dart_capabilities` returns conservative live-endpoint GSO/GRO segment
limits, fragmentation behavior, and IPv4/IPv6 ECN availability. It returns
`None` with no live Dart endpoint. Unsupported offloads fall back to ordinary
UDP. An ECN availability value of `None` means unconfirmed support.
`ecn_unavailable` counts missing receive metadata separately from Not-ECT.

Adaptive mode validates unique reception counts and reacts to CE feedback.
Missing or inconsistent ECN feedback disables marking for that incarnation.
Addresses and identities remain unauthenticated. Fresh session identifiers
reject packets from retired incarnations, but do not protect against an
attacker that observes and injects current-session traffic.
