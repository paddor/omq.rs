# Native OMQ over QUIC

## 1. Scope

`quic://host:port` carries ZMTP 3.x over QUIC v1 and TLS 1.3. One OMQ
peer is one QUIC connection. This native profile uses ALPN `omq-zmtp/1`.
It does not use HTTP/3, WebTransport, QUIC DATAGRAMs, or OMQ compression.

Socket compatibility, identities, subscriptions, multipart boundaries,
and ZMTP mechanisms have their usual meanings. STREAM remains TCP-only.
The profile adds encryption and a separate path for liveness traffic.
All application messages share one ordered stream: this profile does not
remove head-of-line blocking between messages from the same peer.

The underlying transport follows [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000)
and [RFC 9001](https://www.rfc-editor.org/rfc/rfc9001).

## 2. TLS and authentication

The listener supplies a PEM certificate chain and private key. The
connector MUST verify the server's chain and DNS name or IP address.
Trust anchors come from the system store, configured PEM roots, or both.
An explicit server-name override changes the verified name. It MUST NOT
disable certificate verification. ALPN mismatch rejects setup.

0-RTT and TLS client certificates are disabled. A NULL ZMTP handshake does
not authenticate the client. Use PLAIN credentials inside TLS or CURVE
with an admission policy when authenticated clients are required.

## 3. Stream roles

The connector opens exactly two bidirectional streams:

| Stream ID | Role | Contents |
| --- | --- | --- |
| 0 | Data | Unchanged ZMTP greeting, mechanism handshake, and frames |
| 4 | Liveness | Preface followed by fixed-size PING/PONG records |

The listener opens no streams. Neither side opens unidirectional streams.
Additional streams are a protocol violation. Roles follow stream IDs,
regardless of which stream's bytes arrive first.

The connector writes its liveness preface first. The listener validates it
and writes its own preface. Both sides complete this exchange before
starting ZMTP or admitting a READY peer.

The data stream has no extra OMQ preface. Parsing a complete ZMTP message
is the only way to deliver it to the application. FIN, RESET_STREAM, or
connection loss MUST NOT cause a partial message to be delivered.

## 4. Liveness wire format

Each direction begins with this eight-byte preface, exactly once:

```text
4f 4d 51 4c 01 00 00 00       "OMQL", version 1, three reserved zero bytes
```

Each following record is nine bytes:

| Offset | Length | Meaning |
| --- | --- | --- |
| 0 | 1 | Type: `01` PING or `02` PONG |
| 1 | 8 | Unsigned nonce, big-endian |

A PONG echoes the nonce of a received PING. The nonce is informational,
not a delivery acknowledgment. Any valid PING or PONG proves activity.
Unknown record types, an invalid preface, or an ended/reset liveness
stream close the generation. Partial reads and writes preserve their
record state across canceled polls.

Output is bounded. At most one record is being written, with one pending
PING and one pending PONG. A newer pending PONG replaces an older one.
A partly written record MUST finish before another record begins.
Liveness records have no ordering relationship to application messages.

## 5. Heartbeats and backpressure

With a nonzero heartbeat interval, each side sends periodic PINGs. The
reply timeout starts when a complete PING is admitted to the transport,
not when its timer fires. A valid incoming record clears that timeout.
The configured heartbeat timeout defaults to the heartbeat interval.
Without an interval, the peer answers PINGs but initiates no probes.

Delayed timers produce one probe, not a backlog of missed intervals.
Durations beyond the clock's range act as infinite waits. Receive floods
MUST NOT starve output, timeouts, or stream-violation detection. Drains
yield after bounded work.

ZMTP heartbeat generation on the data stream is disabled for QUIC.
Liveness has its own stream credit and higher output priority. A receiver
whose application queue is full continues serving liveness, so local
backpressure does not classify a responding peer as dead.

The data receive window is a byte window, not a maximum message size.
Messages larger than the window advance as the receiver returns credit.
Connection credit includes a liveness reserve above the data window:
`max(data_window / 4, 64 KiB)`. Data must not consume that reserve.

## 6. Setup and admission

One setup deadline covers TLS, stream roles, the liveness preface, the
ZMTP mechanism, and READY. Setup failures retire that generation and
appear on the socket monitor. Connector retry follows the socket's
reconnect policy, including connect-before-bind.

Current defaults and limits:

| Setting | Value |
| --- | --- |
| Setup deadline | 10 seconds |
| Pending setups | 128 per socket, 32 per listener |
| Ready QUIC peers | 1024 per socket; configurable, nonzero |
| Data receive window | 1 MiB; configurable from 16 KiB to 256 MiB |
| Transport idle timeout | 10 seconds |
| Transport keepalive | 2 seconds; nonzero and below idle timeout |
| Maximum UDP payload | 1500 bytes |

Pending setup admission occurs before accepting TLS. Ready-peer admission
is separate; QUIC peers do not consume another transport's ready-peer
limit. Identity handover may replace a route without another ready slot.

## 7. Close and linger

Graceful close drains accepted outbound messages, sends data FIN, waits
for acknowledgment of data and FIN, then waits for the peer to close
after reading EOF. One linger deadline bounds the whole sequence.
Transport acknowledgment alone does not mean application consumption.

Zero linger or an expired finite deadline aborts the generation. Already
delivered messages remain complete; incomplete messages are discarded.
Unlimited linger may wait indefinitely for a blocked peer. Simultaneous
finite closes remain bounded by their deadlines.

| Application code | Meaning |
| --- | --- |
| `00` | Completed close |
| `01` | Invalid setup or expired setup deadline |
| `02` | Invalid or ended liveness stream |
| `03` | Liveness timeout |
| `04` | Unexpected stream |
| `05` | Local abort or incomplete linger |

## 8. Endpoint lifetime and reconnect

Unbind stops new admission and preserves accepted peers. Live peers
retain their UDP endpoint group and assigned IO runtimes. Within one
context/runtime, rebinding reuses that group; new TLS settings apply to
new connections while established peers retain their negotiated state.
Only one listener may admit new peers on a group at a time. A port stays
owned while accepted peers use it; another context cannot take that port
until those peers release it.

Active migration is disabled. Address rebinding is handled by retiring
the old generation and reconnecting. QUIC retransmission handles packet
loss, duplication, and reordering within a generation. OMQ does not add
an end-to-end acknowledgment or exactly-once replay across reconnects.

## 9. Configuration and use

The Rust configuration is `Options::quic`. The C and Python bindings use
`OMQ_QUIC_*` options for certificates, trust, verified name, stream window,
and ready-peer limit. Python fixes these settings before its backend is
materialized. Invalid local configuration fails before network setup.

QUIC requires a usable UDP path. One connection's packet processing runs
on one IO thread; multiple peers can spread across IO threads. It provides
authenticated server transport and liveness independent of data credit.
It does not promise higher throughput or lower latency than TCP. Compare
against encrypted TCP when measuring encryption cost.
