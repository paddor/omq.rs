# Source receive backpressure

## Current contract

PEER, PULL, and GATHER expose the same API:

```rust
recv_from(source: Option<&ReceiveSource>) -> Result<(ReceiveReceipt, Message)>
try_recv_from(source: Option<&ReceiveSource>) -> Result<(ReceiveReceipt, Message)>
unshift(receipt: ReceiveReceipt, message: Message) -> Result<(), UnshiftError>
```

`None` selects an unpaused source fairly. `Some(source)` selects one physical
connection generation, returning its held message first. A live receipt claims
that source and prevents other socket clones, ordinary receives, and bulk
receives from overtaking it. Drop the receipt after admitting or discarding the
message to resume drainage. `unshift` transfers the claim and message into one
OMQ-owned side slot; the source stays paused until a targeted retry is accepted.

Fanring stores a Boolean, not the popped message. The paused ring fills, the
connection stops reading, and its upstream buffers eventually fill. Resumption
makes the lane eligible for drainage; existing ring release batches return
consumed slots. There are no application credits or capacity grants.

PEER receipts retain the existing per-source memory permit. PULL/GATHER retain
their message-count HWM behavior: a bounded ring, at most one claimed or held
message, and one pending message in each wire driver. Their receipt records the
original backing-storage size so a returned message cannot enlarge the side
slot. Set `max_message_size` to bound individual messages; its default for
PULL/GATHER remains unlimited. No socket-wide admission budget couples peers.

PULL/GATHER use one socket-owned fanring for wire and inproc connections. Wire
lanes use receiver HWM. Inproc lanes use sender HWM plus receiver HWM, rounded
up to a power of two. Inproc sends enqueue from the calling thread and receives
drain from the application thread. No IO task relays a message. Source metadata
lives once per connection; plain slots contain only `Message`. Tagged receives
use fanring's borrowed `with_lane_ids()` view.

Inproc publications use the socket's existing fenced `DataSignal` instead of
also marking fanring's ready index. Blocking wakes reuse that publication fence.
Ready scalar receives skip the signal fence; an empty drain fences and rechecks before
allowing a wait. Scalar drains poll all unpaused lanes every 64 received messages and recheck all lanes before reporting empty. Bulk drains
poll before each bounded batch. Wire drivers retain indexed batch publication.
The readiness poll changes no ring capacity or application admission state.

Targeted waits observe only their source's data, claim, and lifecycle signals.
Unrelated sources do not wake them. Cancellation consumes no message. After
disconnect is observed, targeting or returning to that source fails with
`Closed`; it cannot select a replacement. PULL/GATHER discard a held retry on
disconnect while ordinary receives preserve an already published queue prefix.
Invalid returns preserve the caller's message through `UnshiftError`.

## Feasibility across all 20 socket types

These assessments follow the existing receive paths in `socket/handle.rs`,
`socket/actor/peer_materialize.rs`, `socket/recv.rs`, `routing/mod.rs`, and
`socket/udp.rs`. They describe possible extensions; only PEER, PULL, and GATHER
currently expose physical source claims.

| Receiving type | Feasibility | Work and constraints |
|---|---|---|
| PEER | Implemented | Keep identity routing, generation fencing, and retained-memory permits. |
| PULL / GATHER | Implemented | Wire and direct inproc lanes share one fair drain. GATHER retains single-frame semantics. |
| SUB / XSUB | Straightforward next | Reuse the PULL/GATHER fanring and direct inproc path. Apply filters before admission. Subscription commands must continue while a data lane is paused. Preserve XSUB raw subscription sends. |
| SERVER | Feasible, small structural change | Register native inbound peers in source-aware fanring. Keep `RecvSink::Server` attaching routing IDs; keep replies independent of inbound drainage. Physical source IDs must not alias reused routing IDs. |
| DEALER / CLIENT | Feasible | Register inbound peers in the common source-aware fanring. Preserve CLIENT's single-frame contract. Receive source selection does not change existing outbound round-robin routing. |
| ROUTER | Feasible, more work | Move wire identity receive processing into a per-connection sink/drain, preserving identities, mandatory sends, and handover policy. Adapt direct inproc identity prefixes. Current `IdentitySocket` source receives remain inert on ROUTER. |
| PAIR / CHANNEL | Feasible, limited fan-in benefit | One physical connection only. A held message can still support application admission retries. CHANNEL remains single-frame. |
| REQ / REP | Requires protocol-specific receipts | Coordinate retry and receipt acceptance with request correlation, alternation, REP envelopes, and saved reply routes. Existing `recv` admits protocol state before returning; a generic queue pause alone is insufficient. |
| DISH | Transport-dependent | TCP/inproc could pause per RADIO connection after group filtering, with a blocking sender policy. UDP cannot propagate lossless backpressure to a remote sender. |
| STREAM | Feasible with a dedicated adapter | Pause per TCP connection and preserve chunk FIFO. Keep connection/disconnection notifications reachable separately from paused byte data. A chunk is not a ZMTP message. |
| XPUB | No generic data-lane extension | Application receives subscription notifications. Subscription processing is control traffic and must remain reachable. Holding a notification must not pause processing peer subscriptions/cancellations. |
| PUSH / SCATTER | Not applicable | Application sends only. Existing outbound mute and HWM handling already propagate pressure from receivers. |
| PUB / RADIO | Not applicable to application receive | Application sends only. Their peer commands remain control traffic. Publish admission follows the configured drop or block policy. |

## SUB with a nondropping publisher

Yes: pausing one SUB publisher connection can bound application backlog while
other publisher connections continue. With `xpub_nodrop` enabled on the PUB or
XPUB peer, filling that source's receive path eventually blocks upstream
publication. Default PUB/XPUB drop behavior can lose publications once its
outbound HWM is reached. The receiver cannot change that policy or recover
already dropped messages. This matches the publisher-side
[ZMQ_XPUB_NODROP contract](https://zeromq.github.io/libzmq/zmq_setsockopt.html).

The pause granularity is one physical publisher connection, not one topic. If
one topic is held, other topics multiplexed on that connection also pause.
Separate connections provide separate lanes.

Nondropping fan-out also has a publisher-side consequence: a publication that
matches a stalled subscriber cannot complete until every required delivery
can progress. OMQ services ready peers independently while a blocking send is
pending, but the same sequential caller cannot submit its next publication
until that send completes. Concurrent nonblocking publication reserves all
required targets before publishing, preventing partial delivery on retry.
Per-source receive pausing cannot promise independent publication throughput
for subscribers sharing one publisher and message stream.

## Implementation order

1. PULL/GATHER across every native connection path.
2. SUB/XSUB: enable the same fanring path and test PUB/XPUB both with and without
   `xpub_nodrop`, topic matching, subscription changes, and reconnect replay.
3. SERVER, then DEALER/CLIENT: reuse the adapter while preserving receive-side
   routing metadata and reverse-direction replies under saturation.
4. ROUTER: move identity transforms out of actor data delivery and test both
   handover policies, stale receipts, and `IdentitySocket` behavior.
5. Optional PAIR/CHANNEL and STREAM adapters. Give REQ/REP a separate receipt
   admission design. Keep XPUB subscription handling and UDP outside this
   lossless data-lane contract.

Conflate uses replacement semantics and cannot offer FIFO source claims.
External receive sinks own drainage outside `Socket`; source pausing needs a
separate adapter agreed with that consumer. Neither path is enabled implicitly.
