# Benchmarks

Numbers below are from a single sweep on Linux 6.12 (Debian 13) on an
Intel Mac Mini 2018 (i7-8700B, 3.2 GHz, 4 vCPU), Rust 1.95.0,
default features (no priority feature; priority mode trades work-
stealing for per-peer queues - relevant for ordering, not throughput).
Built with `cargo bench --release`. Each cell is one prime + warmup +
0.5 s timed run. Sources: `omq-tokio/benches/` and `omq-compio/benches/`.
Run yourself with `cargo bench` per crate. Bare-metal, modern CPUs, and
tuned tokio runtimes will show different absolute numbers - relative
shape between transports and sizes should hold.

> **One core for the compio numbers.** Every omq-compio bench in this
> document runs PUSH and PULL inside a single `#[compio::main]`
> runtime, which is single-threaded by design. Both peers share one
> CPU; the omq-tokio numbers below have a multi-thread runtime
> spreading work across `num_cpus::get()` worker threads. So the
> compio column is "what one core can do" while the tokio column is
> "what the whole box can do" - the comparison is wildly unfair to
> compio on wire transports.
>
> If your workload can drive sockets from independent task graphs
> (per-shard ingestion, per-tenant dispatch, etc.), instantiate one
> `compio::runtime::Runtime` per worker thread and pin it via
> `RuntimeBuilder::thread_affinity(...)`. A two-runtime PUSH/PULL
> probe on this hardware lifts TCP / IPC small-message rates by
> roughly 20-40%; inproc is unchanged since there's no kernel work
> to overlap. Multiple runtimes also unlock the natural thread-per-
> core pattern (one acceptor + N pinned workers, sharded by identity
> or hash) that libzmq and Seastar use to scale past one core.

## PUSH/PULL throughput by transport, single peer (omq-compio, one core)

Numbers below are single 0.5 s timed runs per cell; the small-size
wire columns vary ±10 % run-to-run (cache / scheduling jitter on a
single core). Larger sizes vary more once kernel send-buffer behaviour
kicks in - take ±25 % at 8 KiB+ as a rough envelope.

| Size    | inproc                  | ipc                  | tcp                  | lz4+tcp              | zstd+tcp             |
|---------|-------------------------|----------------------|----------------------|----------------------|----------------------|
| 128 B   | **3.20M / 410 MB/s**    | 979k / 125 MB/s      | 962k / 123 MB/s      | 708k / 90.6 MB/s     | 95.7k / 12.2 MB/s    |
| 512 B   | **3.16M / 1.62 GB/s**   | 876k / 448 MB/s      | 880k / 451 MB/s      | 496k / 254 MB/s      | 97.1k / 49.7 MB/s    |
| 2 KiB   | **3.17M / 6.50 GB/s**   | 722k / 1.48 GB/s     | 742k / 1.52 GB/s     | 420k / 861 MB/s      | 333k / 682 MB/s      |
| 8 KiB   | **3.18M / 26.0 GB/s**   | 381k / 3.12 GB/s     | 364k / 2.98 GB/s     | 269k / 2.21 GB/s     | 221k / 1.81 GB/s     |
| 32 KiB  | **3.18M / 104 GB/s**    | 123k / 4.04 GB/s     | 113k / 3.70 GB/s     | 111k / 3.65 GB/s     | 94.8k / 3.11 GB/s    |
| 128 KiB | **2.87M / 376 GB/s**    | 31.8k / 4.17 GB/s    | 28.4k / 3.72 GB/s    | 33.3k / 4.36 GB/s    | 29.4k / 3.85 GB/s    |

Note: large-payload "GB/s" on inproc reflects the zero-copy refcount-
clone path - bytes never traverse the kernel. lz4 / zstd are wins above
~8 KiB (compression amortizes its frame overhead and ships fewer wire
bytes); below that the encoding cost dominates over the gain.

## Compression on realistic JSON payloads (omq-compio, 1 peer)

Payload is a JSON event-log record (timestamps, trace ids, repeated
field names - typical eventing-pipeline traffic). Each cell shows
**three rates**: `msgs/s · wire MB/s · virtual MB/s`, where wire MB/s
is what the network actually carries (post-compression) and virtual
MB/s is what the application sees (pre-compression). For plain `tcp`,
wire == virtual.

Compression ratios on this template:

| size    | lz4     | zstd     |
|---------|---------|----------|
| 128 B   | 0.97×*  | 0.97×*   |
| 256 B   | 0.98×*  | 0.98×*   |
| 512 B   | 1.57×   | 1.62×    |
| 1 KiB   | 2.60×   | 2.84×    |
| 2 KiB   | 3.76×   | 4.47×    |
| 4 KiB   | 4.92×   | 7.41×    |
| 16 KiB  | 6.47×   | **12.87×** |

\* Below 512 B the transform doesn't even attempt compression - frame
envelope overhead doesn't amortize at small sizes, so both lz4 and
zstd fall back to plaintext (0.97-0.98× reflects the 4-byte
`SENTINEL_PLAIN` framing tax). A pre-trained dictionary moves that
cutoff way down: 32 B for lz4, 64 B for zstd. See the next section.

Loopback throughput (msgs/s · wire MB/s · virtual MB/s):

| size  | tcp                          | lz4+tcp                            | zstd+tcp                           |
|-------|------------------------------|------------------------------------|------------------------------------|
| 128 B | 982k / 126 MB/s              | 739k / 97.5 MB/s / 94.6 MB/s       | 136k / 17.9 MB/s / 17.4 MB/s       |
| 256 B | 951k / 243 MB/s              | 725k / 188 MB/s / 186 MB/s         | 138k / 35.9 MB/s / 35.4 MB/s       |
| 512 B | 906k / 464 MB/s              | 415k / 135 MB/s / 213 MB/s         | 101k / 31.9 MB/s / 51.7 MB/s       |
| 1 KiB | 815k / 835 MB/s              | 382k / 151 MB/s / 392 MB/s         | 269k / 96.8 MB/s / 275 MB/s        |
| 2 KiB | 760k / 1.56 GB/s             | 317k / 173 MB/s / 650 MB/s         | 198k / 90.8 MB/s / 406 MB/s        |
| 4 KiB | 588k / 2.41 GB/s             | 244k / 203 MB/s / 1.00 GB/s        | 125k / 69.0 MB/s / 511 MB/s        |
| 16 KiB| 218k / 3.57 GB/s             | 106k / 268 MB/s / 1.73 GB/s        | 47.6k / 60.6 MB/s / 780 MB/s       |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~268 MB/s wire while
delivering ~1.73 GB/s of application data. On a 1 Gbps WAN (~125 MB/s
wire ceiling) plain `tcp` would deliver 125 MB/s of application data
total - `lz4+tcp` would deliver ~810 MB/s and `zstd+tcp` ~1.6 GB/s.
That's where compression earns its keep.

### With a pre-trained dict (small messages)

For small messages, per-frame codec overhead (header bytes + cold
codebook) can leave compression underwater. A pre-trained dictionary
primes the codec with byte sequences from your message family, so
even a 128 B record compresses heavily. Pass via
`Options::compression_dict(Bytes)`; the dict is shipped to the peer
on the first connection and reused for every subsequent frame.

Compression ratios on the same JSON template, with a trained dict
(zstd: 1.6 KiB trained from 200 sample records; lz4: 4 KiB
representative buffer):

| size  | lz4 (no dict) | lz4 (with dict) | zstd (no dict) | zstd (with dict) |
|-------|---------------|-----------------|----------------|------------------|
| 128 B | 0.97× (skip)  | **5.82×**       | 0.97× (skip)   | **5.12×**        |
| 256 B | 0.98× (skip)  | **11.64×**      | 0.98× (skip)   | **9.85×**        |
| 512 B | 1.57×         | **22.26×**      | 1.62×          | **19.69×**       |
| 1 KiB | 2.60×         | **11.25×**      | 2.84×          | **35.31×**       |
| 2 KiB | 3.76×         | **8.50×**       | 4.47×          | **16.93×**       |

"(skip)" marks sizes below the 512-byte attempt threshold - the
transform doesn't even try to compress, so the no-dict ratio is just
the framing tax. With a dict, the threshold drops to 32 B (lz4) /
64 B (zstd) and small messages compress meaningfully.

Loopback throughput with the same dict (msgs/s · wire MB/s · virt MB/s):

| size  | lz4+tcp                          | zstd+tcp                       |
|-------|----------------------------------|--------------------------------|
| 128 B | 387k / 8.5 MB/s / 49.5 MB/s     | 150k / 3.7 MB/s / 19.2 MB/s   |
| 256 B | 381k / 8.4 MB/s / 97.6 MB/s     | 150k / 3.9 MB/s / 38.3 MB/s   |
| 512 B | 364k / 8.4 MB/s / 186 MB/s      | 148k / 3.8 MB/s / 75.7 MB/s   |
| 1 KiB | 329k / 30.0 MB/s / 337 MB/s     | 147k / 4.3 MB/s / 150 MB/s    |
| 2 KiB | 277k / 66.9 MB/s / 568 MB/s     | 123k / 14.9 MB/s / 252 MB/s   |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

| transport | size  | omq-compio    | omq-tokio      |
|-----------|-------|---------------|----------------|
| inproc    | 128 B | 5.3 µs (190k) | 31 µs (32.4k)  |
| inproc    | 2 KiB | 5.3 µs (188k) | 30 µs (32.8k)  |
| ipc       | 128 B | 21 µs (47.5k) | 57 µs (17.6k)  |
| ipc       | 2 KiB | 23 µs (44.4k) | 59 µs (17.1k)  |
| tcp       | 128 B | 30 µs (33.3k) | 77 µs (12.9k)  |
| tcp       | 2 KiB | 31 µs (31.9k) | 75 µs (13.4k)  |

µs is round-trip wall time; parenthesized number is full request+reply
pairs per second. compio wins inproc by ~6× (single-thread, no
syscall, recv-direct fast path); on wire transports compio's RTT runs
roughly 2.5-3× below tokio's at small messages (see "compio IPC
latency: hop-reduction history" below). Cells are single 0.5 s runs;
small-message wire RTTs jitter ±15-25% on a 4-vCPU VM, so read the
trend, not any single cell. The RTT win comes from Stage 5's
recv-direct path: on the inbound side, `Socket::recv` reads the FD
inline instead of waiting for the driver task to forward parsed
messages over a flume hop.

### compio IPC latency: hop-reduction history

Three structural changes on the compio path cut substantially off
REQ/REP RTT vs. the original actor-shaped implementation. A fourth
change (Stage 4) was tried and reverted; it's listed last as the
"what we tried and threw out" entry because the trade-off it
revealed shaped the final design.

1. **Single-wire-peer send bypass.** Round-robin sockets (REQ/REP/PAIR/
   DEALER 1:1) skip the socket-wide `shared_send_tx` and submit
   directly to the peer's per-driver `cmd_tx` when only one wire peer
   is connected. Multi-peer wire still uses the shared queue for
   work-stealing. Falls back to the shared queue if the per-peer
   channel is disconnected (driver died, reconnect in flight) so the
   libzmq "buffer up to send_hwm with no live peer" semantic holds.
   Implemented in `omq-compio/src/socket/send.rs`.

2. **`PollFd::read_ready` in the driver select instead of a dedicated
   read task.** Previously each connection spawned a read task that
   ferried filled buffers via a flume channel - one task wake per
   inbound chunk. The driver's `select_biased!` now races
   `PollFd::read_ready` (cancellation-safe; backed by io_uring's
   `PollOnce`). When it fires, the driver does an inline
   `reader.read(buf).await`; the kernel data is already queued so the
   read SQE completes immediately. Implemented in
   `omq-compio/src/transport/driver.rs`.

3. **Stage 5 - recv-direct fast path.** `Socket::recv` on
   single-peer eligible sockets (Pull / Sub / Rep / Pair / Req)
   reads the FD inline instead of waiting on the driver's `in_rx`
   hop. The reader, codec, writer, and transform live in a
   `SharedPeerIo` behind an `async_lock::Mutex`; a per-connection
   `DirectIoState` arbitrates FD ownership via a one-shot
   `recv_claim` atomic and `recv_state_changed` /  `eof_signal`
   `event_listener::Event`s. The driver re-checks `recv_claim`
   under the lock before any read so it can't steal kernel data
   from a recv caller that claimed the FD between iterations.
   `recv()` flushes codec output (auto-PONG, etc.) inline so
   heartbeats keep flowing while the claim is held. ROUTER, XPUB,
   XSUB, DISH stay on the slow path. The reader / writer halves
   live in `WireReader` / `WireWriter` enums (one variant per
   transport); static-dispatched `match` inside the async methods
   means no `Box<dyn Future>` per call - which mattered after
   benchmarking showed boxed futures dominating the small-message
   throughput path. Cancellation note: dropping a `recv()` future
   after `read_ready` has fired but before the read SQE returns
   may forfeit a small amount of in-flight bytes (~5 µs window);
   the codec stays consistent and the connection remains usable.
   Implemented in `omq-compio/src/socket/handle.rs`,
   `omq-compio/src/socket/inner.rs`,
   `omq-compio/src/transport/peer_io.rs`, and the driver loop in
   `omq-compio/src/transport/driver.rs`. Cuts REQ/REP IPC RTT
   roughly in half (recv side is one of two hops per RTT).

#### Stage 4 (tried, reverted): direct-write fast path

Stage 4 put the writer in `SharedPeerIo` and let `Socket::send`
encode + `write_vectored` inline, skipping the `cmd_tx` hop on the
send side. RTT went from ~165 µs to ~85 µs in the original
measurements - a clean 2× win on paper. **PUSH/PULL throughput
collapsed by 4-7×** at small message sizes (TCP 128 B: ~830k → ~115k
msg/s). Cause: the pre-Stage-4 driver got cross-message batching
"for free" - producers pushed into `cmd_tx` and returned
immediately, the driver drained N queued messages on its next
iteration and issued ONE `writev` for all of them. Stage 4
collapsed that into per-call inline encoding + writev, so a hot
single-producer loop did one syscall per message instead of one
syscall per N messages. The recv-side win from Stage 5 was kept
because RTT reduction there doesn't depend on changing the send
path; the send-side fast path was reverted in favour of restoring
producer/writer pipelining. The lesson: latency wins on bypass-the-
hop optimisations can mask big throughput regressions when the hop
was implicitly batching.

The omq-tokio IPC numbers are still untouched. Tokio's send path goes
through the SocketDriver actor + per-send submit task + per-peer pump
+ ConnectionDriver — more hops than compio, but the multi-thread
runtime hides some of the cost by overlapping send/recv on different
workers. Stage 1's single-wire-peer bypass would port to tokio's
`routing/round_robin.rs`; tracked as a follow-up.

## Backend comparison: PUSH/PULL throughput, single peer

| Size    | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---------|---------------|--------------|------------|-----------|------------|-----------|
| 128 B   | **3.20M**     | 423k         | **979k**   | 239k      | **962k**   | 118k      |
| 512 B   | **3.16M**     | 423k         | **876k**   | 215k      | **880k**   | 120k      |
| 2 KiB   | **3.17M**     | 430k         | **722k**   | 227k      | **742k**   | 116k      |
| 8 KiB   | **3.18M**     | 440k         | **381k**   | 117k      | **364k**   | 109k      |
| 32 KiB  | **3.18M**     | 430k         | **123k**   | 49.4k     | **113k**   | 52.3k     |
| 128 KiB | **2.87M**     | 417k         | **31.8k**  | 18.9k     | **28.4k**  | 24.9k     |

Numbers are msg/s. compio wins at every size on every transport on
this hardware: io_uring + the direct-routing path beats tokio's
mio/epoll syscall path even where syscall overhead amortizes at
large sizes. Wins narrow at very-large sizes where syscall cost is
the same on both backends. **Note that compio here is one core
versus tokio's whole box** - see the caveat at the top of this
document. Tokio's lead grows on multi-peer fan-in (its multi-thread
runtime overlaps senders across cores); a multi-runtime compio
deployment lifts wire throughput another 20-40%.

## Mechanism per-frame cost (sans-I/O)

Per-frame cryptographic cost of sealing one ZMTP frame payload, as
measured by `omq-proto/benches/mechanism_frame.rs`. Numbers are
plaintext throughput in MB/s or GB/s (decimal, 10^6 / 10^9); higher
is better.

|  size   |   NULL (memcpy) | CURVE (XSalsa20Poly1305) | BLAKE3ZMQ (ChaCha20-BLAKE3) |
|--------:|----------------:|-------------------------:|----------------------------:|
|    64 B |    4.27 GB/s    |               49 MB/s    |                  152 MB/s   |
|   256 B |    14.2 GB/s    |              154 MB/s    |                  381 MB/s   |
| 1 KiB   |   42.7 GB/s     |              330 MB/s    |                  666 MB/s   |
| 4 KiB   |   63.0 GB/s     |              477 MB/s    |                  904 MB/s   |
|16 KiB   |   54.1 GB/s     |              533 MB/s    |             **1.22 GB/s**   |
|64 KiB   |   36.0 GB/s     |              545 MB/s    |             **1.41 GB/s**   |

> **Security note on BLAKE3ZMQ.** This mechanism is omq-native and has
> **not been independently security audited.** It's modelled on Noise
> XX with BLAKE3 transcript hashing, X25519 key exchange, and
> ChaCha20-BLAKE3 AEAD, but novel cryptographic constructions need
> third-party review before they should be trusted for anything that
> matters. If you have security or compliance requirements, use
> **CURVE** (RFC 26 / NaCl XSalsa20Poly1305 - well-reviewed and what
> libzmq ships). Independent audits of BLAKE3ZMQ are very welcome - if
> you or your organisation can fund or conduct one, please open an
> issue on the repo.

Numbers are stock `cargo bench` (no `-C target-cpu=native`). omq-proto
pins a fork of `chacha20-blake3` adding `#[target_feature(enable =
"avx2")]` annotations that let LLVM auto-vectorize the loops
surrounding the explicit intrinsic calls. Without that patch,
BLAKE3ZMQ runs ~20× slower (~50 MiB/s at bulk sizes) unless every
downstream consumer rebuilds with `-C target-cpu=native`. `crypto_box`
(CURVE) plateaus around ~545 MB/s either way: its salsa20
implementation has no SIMD path. Reproduce with:

```sh
cargo bench -p omq-proto --bench mechanism_frame --features 'curve blake3zmq'
```

## Reproducing

```sh
cargo bench -p omq-compio --bench push_pull
cargo bench -p omq-tokio  --bench push_pull
cargo bench -p omq-compio --bench req_rep
# Override transports / sizes / peer counts via env:
OMQ_BENCH_TRANSPORTS=tcp,lz4+tcp,zstd+tcp \
OMQ_BENCH_PEERS=3 \
OMQ_BENCH_SIZES=128,2048,32768 \
  cargo bench -p omq-compio --bench push_pull
```
