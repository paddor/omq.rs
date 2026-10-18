# omq-shm

Linux shared-memory transport for the [OMQ messaging library](https://github.com/paddor/omq.rs).
Applications use it through [`omq-tokio`](https://crates.io/crates/omq-tokio)
sockets with `shm://` endpoints.

Linux-only `shm://path` / `shm://@name`, using NULL authentication.
Every socket type except STREAM is supported. The final frame uses shared
storage; earlier frames use the carrier ring until they reach 4 KiB in total.
Larger messages with up to 256 frames travel as one shared multipart record.

A socket can bind or connect multiple SHM endpoints and mix them with inproc,
IPC, TCP, QUIC, and DART endpoints (with the corresponding features enabled).
CHANNEL admits one peer at a time across all endpoints.

## Data flow

The path directly names the Unix rendezvous socket; `@name` uses the abstract
namespace. Its stream carries setup, FD registration, and disconnect detection.
Bind replaces only a stale socket at the path.
Anonymous, size-sealed memfds are registered with `SCM_RIGHTS`. Payload memfds are
also write-sealed (`F_SEAL_FUTURE_WRITE`): only the sender modifies them, through
its existing mapping. Receivers map payloads read-only and validate coordinates
against generation-tagged leases.

Each direction has a bounded SPSC byte ring for ZMTP framing, control, and
coordinates. Release/acquire cursors publish and return bytes. Parked peers
use coalesced eventfd notifications; actively polling readers need no wake syscall.
New region descriptors are acknowledged before their coordinates are published.

Automatic sender storage is shared across all socket peers. Each SHM endpoint
uses one IO lane for its connections and fan-out worker. Ordinary publications
are copied once before fan-out; subscribers map the same storage without
separate payload pools. Bodies through 55 bytes use the shared carrier ring.
Caller threads only enqueue messages.

`omq_tokio::shm_payload_pool([(bytes, slots), ...])` preallocates producer
storage. Preparation fills existing slots directly. `try_` preparation
never grows the pool; ordinary preparation retains heap fallback.
`pool.message(size, fill)` accepts varying sizes and allocates separately when no
slot fits. Batch producers can fill one buffer, freeze it into `Bytes`, and slice
variable-sized messages; a retained slice pins the whole buffer.

## Batches and leases

Already queued messages with the same envelope (at most one frame, up to 16 KiB)
may share a batch:

| Bodies | Shared layout |
|--------|---------------|
| Ordinary or small pooled | count, little-endian `u64` sizes, contiguous payloads |
| Larger SHM pool | count, little-endian `u64` sizes, region coordinates |

Batch selection copies leading pooled bodies through 256 B for point-to-point
traffic and 1 KiB for fan-out. Single-message sends keep their storage policy.
Multipart records use the reference layout for all frames: pooled frames over
55 bytes stay in place, other frames are copied once after the table.

Limits: 256 messages, 256 KiB shared storage/metadata, 16 KiB per copied body.
Referenced payload bytes do not limit batches. Batching never waits for more messages.
Each recipient holds one lease per body, batch, or multipart record; every
message counts against send and receive HWM. Final owners complete leases
through shared atomics.
Clones and byte views retain ownership; a borrowed body retains the batch lease.
Slots become writable after local uniqueness and all recipient completions.
Receivers validate batches on the IO thread and enqueue one descriptor;
application receive creates the non-inline payload views.

Disconnect cancels unread leases. Borrowed storage remains pinned until final
drop or pidfd-confirmed reader exit; EOF cannot reclaim it. Mappings outlive
socket/context teardown. Futex credit waits leave control and shutdown reachable.

## Memory limit

`Options::shm_max_memory(bytes)` caps automatic outbound region bytes across
the socket, including headers and alignment padding. Default: 64 MiB.
Idle classes are reclaimed; retained owners/leases stay charged. Unavailable
storage uses shared carrier delivery. `Some(0)` disables automatic allocation;
`None` removes the cap.

Explicit producer pools, received mappings, completion/carrier tables, and heap queues
are excluded. Message-count HWM and `max_message_size` are separate limits.

Requires Linux 5.3+, same-UID peers, visible peer PIDs, and `OMQSHM08` on both
peers. Receivers trust the sender and read payloads in place. Receivers cannot
write payloads, but can retain slots until they release them or exit. `omq-shm`
contains the Linux/unsafe boundary. Do not inherit live contexts or borrowed
payloads across `fork`.
