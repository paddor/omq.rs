# Shared-memory transport

Linux-only `shm://path` / `shm://@name`, using NULL authentication.
Supported families: CHANNEL, CLIENT/SERVER, SCATTER/GATHER, PEER, RADIO/DISH.
Bodies are single-part; group and routing envelopes retain normal semantics.

A socket can bind or connect multiple SHM endpoints and mix them with inproc,
IPC, TCP, QUIC, and DART endpoints (with the corresponding features enabled).
CHANNEL admits one peer at a time across all endpoints.

## Data flow

The path directly names the Unix rendezvous socket; `@name` uses the abstract
namespace. Its stream carries setup, FD registration, and disconnect detection.
Anonymous, size-sealed memfds are registered with `SCM_RIGHTS`; receivers map
payloads read-only and validate coordinates against generation-tagged leases.

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
storage. Prepared bodies use existing slots without copying. `try_` preparation
never grows the pool; ordinary preparation retains heap fallback.

## Batches and leases

Already queued messages with the same envelope may share a batch:

| Bodies | Shared layout |
|--------|---------------|
| Ordinary | count, little-endian `u64` sizes, contiguous payloads |
| SHM pool | count, little-endian `u64` sizes, region coordinates |

Limits: 256 messages, 256 KiB shared storage/metadata, 16 KiB per copied body.
Referenced payload bytes do not limit batches. Batching never waits for more messages.
Each recipient holds one lease per body or batch; every message counts against
send HWM. Final owners complete leases through shared atomics. Clones and byte
views retain ownership; a borrowed batch body retains the whole batch lease.
Slots become writable after local uniqueness and all recipient completions.

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

Requires Linux 5.3+, trusted same-UID peers, visible peer PIDs, and `OMQSHM07`
on both peers. `omq-shm` contains the Linux/unsafe boundary. Do not inherit
live contexts or borrowed payloads across `fork`.
