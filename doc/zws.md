# WebSocket transport limits and invariants

This describes implemented WS/WSS behavior in `omq-proto` and `omq-tokio`.
Transport selection, reconnect, message atomicity, socket compatibility,
and HWM block/drop policy remain socket responsibilities.

## Setup and HTTP policy

WS/WSS requires a finite `handshake_timeout`, default 30 s. One absolute
deadline covers DNS/connect, TLS, HTTP upgrade, and ZMTP authentication.
Outbound setup acquires socket admission before DNS; accepted connections
acquire it before TLS/HTTP work. Admission survives the handoff to the actor
and ZMTP driver. Failure, timeout, and cancellation release it. Cancellation
wins over a simultaneously ready dial and drops the in-flight attempt.

| Resource | Enforced bound |
|---|---|
| Pending handshakes | `max_pending_handshakes`, default 128 per socket |
| Listener setup | 32 concurrent peers, also subject to socket admission |
| Named endpoint jobs | 128 background jobs per socket |
| Platform DNS | Four workers, eight queued requests, at most 256 returned addresses |
| HTTP upgrade head | 4096 bytes including terminator; at most 64 fields |
| Offered subprotocols | At most 64 |
| Ready WS/WSS peers | `ws.max_ready_peers`, default 1024 across all socket endpoints; nonzero |

Setup and rejection service yields after 32 operations or 128 KiB of logical
work. Saturated listeners close newly accepted connections without another
setup allocation. Canceling DNS drops the awaiting future; an active platform
lookup keeps its worker until the OS call returns. Stricter socket-type peer
limits take precedence. Other carriers do not consume the WS/WSS ready cap.
Identity replacement may reuse an existing ready slot. Rejection preserves
ordinary reconnect and does not surface peer failures through user send/recv.

Upgrade parsing validates method/version, required fields, singleton duplicates,
canonical keys, list headers, and absence of an HTTP body. Coalesced frame bytes
after the head survive setup. Resource matching compares the exact configured
path and query without decoding or prefix matching. Typed endpoints also reject
whitespace/control injection.

The selected subprotocol must be offered and match the configured mechanism.
Native names are `ZWS2.0/NULL`, `/PLAIN`, and `/CURVE`. The legacy OMQ browser
name `ZWS2.0` is accepted for configured NULL/PLAIN and still runs that ZMTP
handshake. It does not select RFC 45's identity-first bare profile. Slash names
are a narrow native HTTP-token exception and cannot be browser subprotocol
tokens. No WS extensions are negotiated; clients reject a selected extension.

A present Origin must match `options.ws.allowed_origins`, empty by default.
Native peers may omit Origin. Matching normalizes HTTP(S) scheme, host, and port;
`null`, duplicates, lists, wildcards, credentials, and path/query origins fail.
Origin is separate from authentication. Host or proxy headers do not grant trust.
C listeners set newline-separated `OMQ_WS_ALLOWED_ORIGINS` (1009) and
`OMQ_WS_MAX_READY_PEERS` (1010) before backend materialization; later changes
return `EBUSY`.

WSS verifies certificate chains and names by default. Custom trust and hostname
overrides are explicit. The WSS accept-invalid-certificate test option is
separate from verified TLS construction. Mutual TLS is not implemented.

## Framing and parser service

Client masks come from a cryptographic generator. Receivers enforce mask
direction, minimal length encoding, the long-length high bit, RSV/opcode rules,
control-frame size/FIN, fragmentation order, and legal CLOSE status/UTF-8.
Mask offsets remain correct across input chunks. Complete unmasked bodies may
share input storage; spanning or masked bodies need assembly storage.

| State | Enforced bound |
|---|---|
| Multipart receive | 65,536 parts, including empty parts |
| Fragment assembly | 65,536 fragments until FIN, including empty fragments |
| Parser turn | 256 frames or 256 KiB consumed input; elapsed-time check every 64 frames, with a 1 ms target |
| Native PONG output | One immutable staged frame plus one latest pending payload |
| CLOSE output | Queued once; discards the unstarted pending PONG |

Control interleaving does not reset assembly counts. Partial writes never
replace staged control bytes. Tokio enables `ConnectionConfig::ws_input_budget`;
sans-I/O callers can opt in. Buffered work resumes after yielding, with local
control and shutdown checked first. The byte/time service boundary is between
frames, so one large copy, decryption, or assembly growth can exceed a turn.
PONG coalescing bounds backlog, not latency behind ordered data.

## Receive pressure and control service

A full application queue retains one prepared delivery in actor state or one
decoded message in driver state. Queue-space waits remain in the main select
with commands, reverse writes, cancellation, and the original linger deadline.
Retries do not repeat decoding, metadata preparation, or receive-rate admission.
MPSC permits stay pinned across select turns and delivery uses the actual permit.
REP admits its body and saved envelope together. Raw yring consumers retain a
10 ms fallback check for consumer drop without an OMQ space signal.

Large native byte-stream payload reads retain their claimed destination and
read at most 64 KiB per selected operation. The outbound codec direction remains
independent of an inbound payload claim. A full actor mailbox retains one popped
codec event under selected admission; further input and decoded messages pause
until that older event enters the mailbox. Parse failures retain their original
error while this prefix waits. Authentication ERROR output and handshake-event
admission retain the setup deadline.

Codec-event turns stop at 64 events, 64 KiB of logical work, or a 1 ms target.
Driver command drains stop at 64 commands or 64 KiB of added wire output and
poll writes before another turn. Channels for local control stay separate from
data backlog. Activation and subscription/group replay still contain actor
awaits that need broader control-progress coverage.

One locally queued ZMTP heartbeat probe prevents output backlog from accumulating
PINGs. Its silence timer starts after the complete codec prefix reaches the
writer. Actual peer input acknowledges an admitted probe; periodic admitted
keepalives retain the original unanswered deadline. Local receive backpressure
suspends silence accounting while preserving outbound keepalives. WS is ordered:
remote PING/unsubscribe traffic cannot pass unread data, and writer admission
does not establish remote receipt. Slow application queues are not proof of death.

## Close and linger

Socket close stops new sends and retains accepted queues for draining, including
late peers. Drivers receive a drain command carrying the socket's original
absolute deadline. Accepted output includes driver batches, deferred data,
compression offloads, arenas, transmit slots, and partial writes. Fan-out also
includes secondary lanes and batches already removed from rings. Producer EOF
preserves the final batch. Zero linger retains immediate cancellation.

After draining, WS sends CLOSE 1000, waits for peer CLOSE or EOF, and shuts down
the writer, including TLS. A missing reply or stalled shutdown uses the same
deadline. Explicit unlimited linger may wait indefinitely. Peer-initiated close
gets a ten-second reply-flush ceiling, tightened by a subsequent socket-close
deadline. Data after local CLOSE is discarded in bounded chunks without an
assembly allocation. Protocol close and TLS completion do not acknowledge remote
application delivery.

## Driver completion

Socket-owned byte-stream, inproc, and STREAM driver futures publish closure
through one reserved result slot per materialized driver. Publication needs no
data-mailbox space and covers return, abort, and panic unwinding after driver
state drops. Standalone public drivers retain their final `PeerEvent::Closed`
mailbox contract.

Each result records the event prefix actually admitted to the actor. Peer state
and identity remain until that prefix and any prepared receive finish. STREAM's
terminal empty receive follows the prefix and cannot replace another pending
receive. Completion service stops at 16 results, 64 KiB of logical work, or a
1 ms target and registers the actor's actual waker. IO-thread load leases live
from materialization through driver-future drop, including unpolled disposal.

## Codec policy

Effective endpoint profiles are captured before setup and validated before DNS.
WSS and CURVE disable OMQ compression; eligible plain `lz4+ws` remains supported.
Native fan-out codecs group within existing IO lanes and fallback peers preserve
nodrop admission. Encoding/compression stays on drivers or lane workers. Wire
limits and decoded-message accounting are distinct.

## Storage limits

Count HWM, assembly counts, and service budgets do not establish aggregate byte
limits. `max_message_size` remains application-configured, with default `None`;
there is no enforced WS-specific 64-MiB message default or per-peer/socket byte
ledger. One large operation and overlapping retired/draining generations can
retain storage beyond queue counts. Kernel, TLS, and browser buffers are separate.

Allocation accounting must follow retained backing capacity through slices,
clones, pools, codec input, assemblies, metadata, receive rings, transmit arenas,
and partial writes. A small slice can retain a large allocation. Shared storage
has one allocation owner; recipient-specific masking/encryption adds distinct
storage. Admission must let every accepted multipart/fragmented message finish
without partial-credit deadlock and keep control/setup service reachable.

## Performance comparison

Use the [serial baseline runner](../scripts/zws_baseline/README.md) with frozen
reference/candidate binaries and identical workloads. Compare at least five
alternating matched repetitions per case. Throughput must remain at least 90%,
and p50/p99 latency at most 110%, of both the original reference and preceding
implementation. Near-threshold spread needs longer paired runs. Profiles stay
separate from unprofiled comparisons; measured hotspots guide optimization.
Interoperability, memory ownership, and fault-injection checks remain separate
from throughput results.
