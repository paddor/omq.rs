# Dart: Datagram Acknowledgment and Repair Transport

- Status: draft
- Author: Patrik Wenger <paddor@gmail.com>
- Protocol version: 1
- URI scheme: `dart://`

Dart transports atomic messages over unicast UDP. It provides ordered
delivery, duplicate suppression, receiver flow control, loss recovery, and
optional Explicit Congestion Notification (ECN). It has no authentication or
encryption.

## 1. Preamble

Copyright (c) 2026 Patrik Wenger. This specification is licensed under the
[ISC license](../LICENSE).

The uppercase terms MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD,
SHOULD NOT, RECOMMENDED, NOT RECOMMENDED, MAY, and OPTIONAL have the meanings
defined in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174).

## 2. Goals

Dart provides ZeroMQ message and socket semantics over UDP, with bounded
retention and independent flow control for each peer. Delivery is reliable
within a live session. Receipt acknowledgment confirms receiver storage,
not application consumption. Delivery across session retirement or process
restart is not guaranteed.

### 2.1 Related Specifications

Dart uses [UDP](https://www.rfc-editor.org/rfc/rfc768) and reuses the socket
names and metadata encoding of [ZMTP 3.1](https://rfc.zeromq.org/spec/37/).
It defines its own framing and connection establishment; it does not carry
a ZMTP greeting, security handshake, or byte stream. Socket patterns are
described by the specifications linked under [Socket Semantics](#4-socket-semantics).

## 3. Implementation

### 3.1 Overall Behavior

Each peer exchanges READY datagrams to establish two receiver-issued session
identifiers. Subsequent traffic identifies the session and direction. Each
direction has an independent sequence starting at zero; one sequence unit
represents a complete message or one fragment.

A sender MUST retain every sequenced unit until cumulative receipt
acknowledgment. Successful UDP submission advances the transmitted prefix
but MUST NOT release retention. Partial batch submission commits only the
accepted prefix. A receiver MUST deliver complete messages in sequence,
without duplicates. Control traffic MUST remain serviceable when data
credit, congestion limits, or application queues prevent data progress.

### 3.2 Formal Grammar

The following [ABNF](https://www.rfc-editor.org/rfc/rfc5234) describes one
complete UDP payload. Length-dependent constraints are specified below.
`OCTET` is defined in RFC 5234. All integers are unsigned and little endian,
except property value lengths (`u32be`).

```abnf
datagram = ready / data / packed / first / cont / status / nak / probe
ready    = %xC0 %x05 %x52.45.41.44.59 *property
property = OCTET 1*255ASCII u32be *OCTET
data     = %x01 session sequence *OCTET
packed   = %x02-80 2*128OCTET session sequence *OCTET
first    = %x81 session sequence u64 1*OCTET
cont     = %x82 session sequence 1*OCTET
status   = %xC1 session 8u64
nak      = %xC2 session sequence u32
probe    = %xC3 session sequence
session  = u64
sequence = u64
u64      = 8OCTET
u32      = 4OCTET
u32be    = 4OCTET
ASCII    = %x00-7F
```

PACKED MUST fit within 1,452 bytes; other datagrams MUST fit within 1,200
bytes, including all headers and metadata. Oversized or truncated datagrams
MUST be discarded. The larger bound requires a path MTU of at least 1,500
bytes for IPv6. Each limit applies to individual UDP offload segments.

On a local MTU error, senders SHOULD reduce PACKED to 1,200 bytes and retry
the retained messages. Implementations MUST preserve complete datagram
boundaries through GSO/GRO, including a shorter final segment.
Implementations SHOULD disable IP fragmentation where available.
Dart does not perform path MTU discovery.

### 3.3 Topology

Endpoints are unicast UDP addresses. A connector initiates establishment;
a listener admits compatible peers. These roles are independent of socket
types. An endpoint MAY serve multiple peers. Peer addresses, identities,
retention, and credit MUST remain isolated. IP multicast is not defined.

### 3.4 Connection Establishment

The connector chooses a fresh, unpredictable, nonzero identifier C and
sends HELLO with `Session=C, Echo=0`. The listener chooses an equivalent
identifier L and replies WELCOME with `Session=L, Echo=C`. The connector
accepts only an echo of C and replies CONFIRM with `Session=C, Echo=L`.
The listener MUST withhold admission and data credit until this confirmation.
The connector MAY admit its local route upon accepting WELCOME.

Participants MUST retry lost establishment messages. A repeated WELCOME
MUST elicit CONFIRM. Identifiers remain unchanged during retries; a new
attempt MUST use fresh identifiers. A changed HELLO MAY challenge a new
candidate but MUST NOT replace a live peer before confirmation. Unmatched
identifiers MUST NOT change session state.

### 3.5 Connection Metadata

READY starts with `0xC0, 0x05, "READY"`, followed by properties. Each property
contains a one-byte name length, that many ASCII name bytes, a four-byte
big-endian value length, and that many value bytes. Names contain 1 to 255
bytes and are case-sensitive. Duplicate names, malformed lengths, missing
required fields, incompatible socket pairs, or unsupported versions MUST
prevent admission. Unknown properties MUST be ignored.

| Required property | Value |
| --- | --- |
| `Socket-Type` | Uppercase socket name from Socket Semantics |
| `DART-Version` | One byte: 1 |
| `Session` | Nonzero u64 receiver-issued identifier |
| `Echo` | u64: zero for HELLO, other participant's identifier otherwise |
| `Phase` | One byte: HELLO=0, WELCOME=1, CONFIRM=2 |
| `Reply-Requested` | One byte: 0 or 1; compatibility metadata |

`Identity` is REQUIRED for PEER and contains 1 to 255 bytes. An unconfigured
PEER identity MUST remain stable for the socket lifetime. Identities and
local SERVER routing IDs MUST NOT appear in message datagrams.

### 3.6 Framing

DATA carries one body after its session and sequence fields. Empty bodies
are permitted. The maximum DATA body is 1,024 bytes. For RADIO/DISH, the
body is preceded by a one-byte group length and 1 to 255 group bytes; its
limit is `min(1024, 1182 - group-length)`. The admitted socket pair determines
whether group metadata is present.
Receivers MUST enforce configured message size limits on DATA and each
packed payload.

PACKED's first byte is the message count, 2 through 128. Exactly that many
one-byte payload lengths follow, then the destination session, first
sequence, and concatenated payloads. Each payload uses DATA's body and group
encoding and occupies 0 to 255 bytes including metadata. The lengths MUST
consume the complete datagram; the implied sequence range MUST NOT wrap.
All lengths and group metadata MUST be validated before admitting any unit.
Larger payloads MUST use DATA or fragmentation. There are no length escapes.
A sender MAY pack already queued messages for one peer but MUST NOT wait
for further messages to fill a datagram. An isolated message uses DATA.

A body exceeding DATA's limits MUST use FIRST followed by CONT. FIRST
contains the full u64 body length, optional group metadata, and the first
chunk. CONT contains only the next chunk. Chunks MUST be nonempty and use
consecutive sequences without interleaving another message. FIRST's chunk
MUST be shorter than the advertised body. Fragment sizes MAY vary within
the datagram limit; the full length determines completion. There is no
offset field or trailing-fragment flag.

Before acknowledging FIRST, the receiver MUST enforce its `max_message_size`
limit, including applicable message metadata, and reserve storage for the
complete advertised body. An unrepresentable length or failed reservation
MUST reject the session. The receiver fills this storage from the ordered
fragment prefix and MUST deliver only the complete body. An orphan CONT,
interleaved message, or chunk exceeding the remaining body invalidates the
session. No transport-wide body limit below u64 is defined.

DATA, PACKED, FIRST, CONT, and PROBE name the destination's receiver-issued
session. STATUS and NAK name the issuer's receiver-issued session. Repairs
MUST preserve original sequences and contents. FIRST and CONT retain their
original framing; a packed message is repaired as individual DATA.

### 3.7 Acknowledgment and Flow Control

STATUS contains, in order, eight u64 fields after the session identifier:
`serial`, `receipt-next`, `credit-edge`, `ect0`, `ect1`, `ce`, `not-ect`, and
`unavailable`. Its total length is 73 bytes. ECN counters describe unique
accepted sequence units; units packed together share the datagram's ECN
classification. Duplicates MUST NOT increment these counters.

`receipt-next` is the first unit outside the contiguous receiver-owned
prefix. `credit-edge` is the exclusive sequence bound for new transmission.
Senders MUST NOT submit new units at or beyond that edge.
The receiver MUST advertise initial credit and MUST NOT retract granted
credit. Each peer has a bounded window of at most 65,536 sequence units.
Gaps reserve capacity even when later units arrive. Duplicates MUST NOT
consume extra capacity or return additional credit.

Credit MUST return only when the associated receive storage is reusable,
including the release of shared application-held clones and views. An
independent inline copy MAY release receive storage upon bounded queue delivery.
Intermediate fragment credit returns after copying into the reserved body.
The final fragment's credit remains charged until the assembled body's last
owner releases it.
This permits a body to span multiple receive windows. Sender admission
counts queued and unacknowledged logical messages against the outbound HWM.
Admission returns only after acknowledgment of a message's final unit.

Fresh STATUS feedback MUST have a strictly increasing serial. Receipt
positions, credit edges, and each counter MUST be monotonic. Receipt MUST
NOT exceed the successfully transmitted prefix; credit MUST be at least
receipt and no more than 65,536 units beyond it. The counter sum MUST be
between receipt and the transmitted prefix without arithmetic overflow.
Stale or invalid feedback MUST NOT release retention or alter granted credit.

### 3.8 Loss Recovery

NAK contains `first-missing:u64` and `count:u32` after the session identifier;
its total length is 21 bytes. Count MUST be nonzero, and the range MUST NOT
overflow or extend beyond the successfully transmitted prefix. The sender
repairs retained units in that range.

PROBE contains `transmitted-next:u64` after the session identifier; its total
length is 17 bytes. It solicits STATUS and exposes missing tail units. Its
position MUST NOT exceed the receiver's granted credit edge. Receivers MUST
repeat needed receipt, credit, and gap feedback. Duplicate sequenced data
MUST solicit current STATUS without redelivery or additional credit.
Senders MUST provide timer recovery when data or control traffic is lost,
using RTT estimates and bounded retransmission backoff.
Advancing cumulative receipt SHOULD restart the retransmission timer.
Unchanged receipt or credit-only feedback MUST NOT restart it.

### 3.9 Congestion Control

Adaptive mode is the default. It uses a per-peer byte congestion window and
RTT-based pacing. The window grows with receipt acknowledgment and decreases
on loss or validated CE feedback. Recovery episodes SHOULD prevent repeated
reductions for the same outstanding prefix. Complete batches count against
the window; only successful submissions advance flight and pacing state.
Repairs MUST be paced. A reduced window MUST permit repair of a missing
prefix while later units remain in flight.

Adaptive mode MAY mark ECT(0) when the carrier supplies ECN metadata. Feedback
MUST be consistent with submitted marked units. ECT(1), Not-ECT, unavailable,
or inconsistent feedback MUST disable marking for that session. New valid
CE feedback MUST invoke congestion response. ECN follows
[RFC 3168](https://www.rfc-editor.org/rfc/rfc3168); a CE counter alone does
not satisfy congestion control.

Explicit LAN mode uses receive credit and local admission limits without
adaptive network congestion control. It MUST NOT mark ECT and SHOULD be
restricted to provisioned private paths. In either mode, a configured byte
rate limit MUST pace new transmissions and repairs. Congestion control is
per peer; no aggregate controller across sockets or endpoints is defined.

### 3.10 Error Handling

Unknown tags, zero session identifiers, malformed lengths, and impossible
positions MUST be discarded without applying their contents. Resource and
protocol failures MAY retire the affected session but MUST NOT impair other
peers. Peer failure MUST NOT produce application send or receive errors.
Connect-before-bind and automatic reconnection are REQUIRED. Wire positions,
STATUS serials, and counters MUST NOT wrap; retire the session first.

## 4. Socket Semantics

| Compatible pair | Behavior | Specification |
| --- | --- | --- |
| CLIENT/SERVER | SERVER replies by local routing ID | [41/CLIENTSERVER](https://rfc.zeromq.org/spec/41/) |
| SCATTER/GATHER | Round-robin send, fair receive | [49/SCATTERGATHER](https://rfc.zeromq.org/spec/49/) |
| PEER/PEER | Identity-routed messages | [51/P2P](https://rfc.zeromq.org/spec/51/) |
| RADIO/DISH | Unicast fan-out, local group filtering | [48/RADIODISH](https://rfc.zeromq.org/spec/48/) |
| CHANNEL/CHANNEL | One live peer | [52/CHANNEL](https://rfc.zeromq.org/spec/52/) |

Each application message MUST be single-part. Identity and group prefixes are
routing metadata. Other socket types and multipart messages MUST be rejected
before enqueueing. Ordinary HWM and mute policies apply. RADIO MAY drop
new unsequenced publications for muted peers but MUST NOT abandon retained
transmissions in a live session. Filtered publications still advance receipt
and return capacity. Slow peers MUST NOT block unrelated peers.

## 5. Connection Heartbeating

Participants MUST send periodic STATUS or PROBE traffic and expire peers
that stop responding. Valid session traffic renews liveness. A closed receive
window or slow application MUST NOT expire a peer that continues responding
to control traffic. Expired peers reconnect through a fresh READY exchange.

## 6. Backwards Interoperability

This specification accepts only Dart protocol version 1. Other versions
and predecessor protocols MUST NOT be admitted or negotiated as a fallback.
`udp://` RADIO/DISH, QUIC, and ZMTP byte-stream transports are separate
protocols.

## 7. Security Considerations

Dart provides no authentication, confidentiality, or cryptographic integrity.
The READY challenge and fresh session identifiers resist blind address
substitution and isolate retired incarnations; an observer can still forge
current-session data or feedback. Applications needing cryptographic
protection must provide it separately.

Implementations MUST bound peer admission, pending setup, retention, and
reordering, and enforce message size limits before reservation. An advertised
u64 length does not authorize unbounded allocation. Repeated setup traffic,
forged feedback, and fragment reservations can consume resources. ECN
feedback is unauthenticated, and LAN mode does not protect shared paths
against excessive aggregate load.
