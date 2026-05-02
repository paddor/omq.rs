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
| 128 B   | **3.05M / 390 MB/s**    | 1.51M / 193 MB/s     | 1.48M / 190 MB/s     | 893k / 114 MB/s      | 91.3k / 11.7 MB/s    |
| 512 B   | **2.97M / 1.52 GB/s**   | 1.17M / 599 MB/s     | 1.22M / 624 MB/s     | 349k / 179 MB/s      | 93.5k / 47.8 MB/s    |
| 2 KiB   | **2.91M / 5.96 GB/s**   | 802k / 1.64 GB/s     | 818k / 1.68 GB/s     | 334k / 685 MB/s      | 291k / 595 MB/s      |
| 8 KiB   | **3.00M / 24.6 GB/s**   | 392k / 3.21 GB/s     | 366k / 3.00 GB/s     | 264k / 2.16 GB/s     | 210k / 1.72 GB/s     |
| 32 KiB  | **2.96M / 97.0 GB/s**   | 122k / 4.01 GB/s     | 113k / 3.71 GB/s     | 113k / 3.71 GB/s     | 91.8k / 3.01 GB/s    |
| 128 KiB | **2.94M / 386 GB/s**    | 30.8k / 4.04 GB/s    | 28.4k / 3.72 GB/s    | 33.5k / 4.39 GB/s    | 28.9k / 3.79 GB/s    |

Note: large-payload "GB/s" on inproc reflects the zero-copy refcount-
clone path - bytes never traverse the kernel. lz4 / zstd on
incompressible payloads (random bytes) cross from overhead to net win
around 32 KiB, where the smaller WRITEV calls outweigh the codec cost.
On compressible traffic (e.g. JSON events), the crossover is much
earlier - see the JSON compression bench below.

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
| 128 B | 1257k / 160.9 MB/s           | 935k / 123.5 MB/s / 119.7 MB/s     | 136k / 18.0 MB/s / 17.4 MB/s       |
| 256 B | 1212k / 310.3 MB/s           | 894k / 232.4 MB/s / 228.8 MB/s     | 138k / 35.9 MB/s / 35.4 MB/s       |
| 512 B | 1080k / 552.9 MB/s           | 502k / 163.8 MB/s / 257.2 MB/s     | 99.4k / 31.4 MB/s / 50.9 MB/s      |
| 1 KiB | 918k / 939.6 MB/s            | 451k / 177.6 MB/s / 461.6 MB/s     | 277k / 99.9 MB/s / 284.1 MB/s      |
| 2 KiB | 766k / 1.57 GB/s             | 357k / 194.4 MB/s / 732.0 MB/s     | 202k / 92.3 MB/s / 412.7 MB/s      |
| 4 KiB | 553k / 2.27 GB/s             | 260k / 216.4 MB/s / 1.07 GB/s      | 124k / 68.6 MB/s / 507.9 MB/s      |
| 16 KiB| 201k / 3.30 GB/s             | 103k / 260.6 MB/s / 1.69 GB/s      | 45.5k / 57.9 MB/s / 744.7 MB/s     |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~261 MB/s wire while
delivering ~1.69 GB/s of application data. On a 1 Gbps WAN (~125 MB/s
wire ceiling) plain `tcp` would deliver 125 MB/s of application data
total - `lz4+tcp` would deliver ~808 MB/s and `zstd+tcp` ~1.61 GB/s.
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
| 128 B | 421k / 9.3 MB/s / 53.9 MB/s     | 152k / 3.8 MB/s / 19.5 MB/s   |
| 256 B | 450k / 9.9 MB/s / 115.2 MB/s    | 151k / 3.9 MB/s / 38.6 MB/s   |
| 512 B | 418k / 9.6 MB/s / 214.1 MB/s    | 150k / 3.9 MB/s / 76.8 MB/s   |
| 1 KiB | 370k / 33.6 MB/s / 378.4 MB/s   | 147k / 4.3 MB/s / 150.8 MB/s  |
| 2 KiB | 307k / 74.1 MB/s / 629.4 MB/s   | 121k / 14.6 MB/s / 246.9 MB/s |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

| transport | size  | omq-compio     | omq-tokio      |
|-----------|-------|----------------|----------------|
| inproc    | 128 B | 5.5 µs (181k)  | 27 µs (37.5k)  |
| inproc    | 2 KiB | 5.5 µs (182k)  | 31 µs (32.5k)  |
| ipc       | 128 B | 20 µs (48.9k)  | 57 µs (17.5k)  |
| ipc       | 2 KiB | 22 µs (46.1k)  | 60 µs (16.8k)  |
| tcp       | 128 B | 28 µs (35.2k)  | 75 µs (13.3k)  |
| tcp       | 2 KiB | 30 µs (33.9k)  | 90 µs (11.2k)  |

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

4. **EncodedQueue send bypass.** `Socket::send` on single-peer wire
   connections (no transform) encodes ZMTP frames directly into a
   `VecDeque<Bytes>` in `DirectIoState` via a sync `Mutex::try_lock`,
   bypassing the codec's async mutex entirely. This eliminates
   `clone_transmit_chunks` + `advance_transmit` on the hot path and
   removes N `Arc` reference-count bumps per `write_vectored` call
   (chunks move into the iovec rather than being cloned). The driver
   drains the queue in step 3b, after flushing the codec in step 3a.
   A `driver_in_select: AtomicBool` flag lets the sender issue
   `transmit_ready.notify(1)` only when the driver is parked in
   `select_biased!` — no spurious wakeups while the driver is
   actively looping. Race-free in compio's cooperative single-threaded
   runtime: no task switch between `store(true)` and the first
   `await` inside `select_biased!`. Transform paths (lz4+tcp, zstd+tcp)
   fall back to the codec mutex path unchanged. Implemented in
   `omq-compio/src/socket/send.rs` and `omq-compio/src/socket/inner.rs`.
   Lifts 128 B TCP PUSH/PULL from 1.30M to 1.48M msg/s; large
   messages see 2-3× wins vs. libzmq (see libzmq comparison below).

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

## libzmq vs omq-compio (two-process TCP, one core each)

Two separate processes on the same machine, each pinned to one core.
`bench_peer push` binds a TCP port and sends forever; `bench_peer pull`
connects, warms up for 500 ms, then counts for 3 seconds. The libzmq
peer is a minimal C binary compiled against the system libzmq (5.2.5).

The omq process is single-threaded (push loop + driver share one
compio runtime). libzmq spawns a dedicated I/O thread alongside the
app thread - two threads vs. one, which gives libzmq a small edge
at small messages where the app loop and I/O thread can overlap.
omq's advantage at large messages comes from `write_vectored` batching
multi-chunk frames in a single `writev` call, while libzmq issues
separate `send()` calls for the frame header and each payload segment.

| Size  | omq msg/s | omq MB/s | zmq msg/s | zmq MB/s | ratio   |
|-------|-----------|----------|-----------|----------|---------|
| 128 B | 2,568k    | 329      | 2,960k    | 380      | 0.87×   |
| 512 B | 2,116k    | 1,083    | 2,010k    | 1,029    | 1.05×   |
| 2 KiB | 1,442k    | 2,952    | 679k      | 1,390    | **2.1×** |
| 8 KiB | 540k      | 4,424    | 186k      | 1,524    | **2.9×** |
| 16 KiB| 309k      | 5,062    | 92k       | 1,508    | **3.4×** |

At 128 B, omq-compio is 13% slower than libzmq; at 512 B they are at
parity; beyond that omq pulls ahead by 2-3.4×. The crossover is around
512 B - roughly the TCP MSS threshold where large-message batching pays
off. The 128 B deficit narrows or closes with two compio runtimes (one
push + one pull in separate processes is already two processes; within
a single process the same pinned-runtime pattern applies).

## Backend comparison: PUSH/PULL throughput, single peer

| Size    | inproc compio | inproc tokio | ipc compio | ipc tokio | tcp compio | tcp tokio |
|---------|---------------|--------------|------------|-----------|------------|-----------|
| 128 B   | **3.05M**     | 408k         | **1.51M**  | 226k      | **1.48M**  | 121k      |
| 512 B   | **2.97M**     | 404k         | **1.17M**  | 175k      | **1.22M**  | 103k      |
| 2 KiB   | **2.91M**     | 404k         | **802k**   | 86.5k     | **818k**   | 114k      |
| 8 KiB   | **3.00M**     | 397k         | **392k**   | 73.8k     | **366k**   | 88.4k     |
| 32 KiB  | **2.96M**     | 399k         | **122k**   | 34.7k     | **113k**   | 43.7k     |
| 128 KiB | **2.94M**     | 373k         | **30.8k**  | 13.1k     | **28.4k**  | 18.2k     |

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
|    64 B |    4.57 GB/s    |               48 MB/s    |                  153 MB/s   |
|   256 B |    15.1 GB/s    |              154 MB/s    |                  380 MB/s   |
| 1 KiB   |   42.7 GB/s     |              334 MB/s    |                  663 MB/s   |
| 4 KiB   |   64.0 GB/s     |              483 MB/s    |                  919 MB/s   |
|16 KiB   |   54.2 GB/s     |              541 MB/s    |             **1.25 GB/s**   |
|64 KiB   |   47.1 GB/s     |              557 MB/s    |             **1.43 GB/s**   |

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
(CURVE) plateaus around ~557 MB/s either way: its salsa20
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
# Two-process libzmq vs omq comparison (requires libzmq installed):
# build: gcc scripts/libzmq_bench_peer.c -o scripts/libzmq_bench_peer -lzmq
# then run scripts/bench_compare.sh
```
