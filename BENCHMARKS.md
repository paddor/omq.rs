# Benchmarks

Numbers below are from a single sweep on Linux 6.12 (Debian 13) running
in a VM on a 2019 MacBook Pro (Intel), Rust 1.93.1, default features (no
priority feature; priority mode trades work-stealing for per-peer queues
- relevant for ordering, not throughput). Built with `cargo bench
--release`. Each cell is one prime + warmup + 0.3 s timed run. Sources:
`omq-tokio/benches/` and `omq-compio/benches/`. Run yourself with `cargo
bench` per crate. Bare-metal, modern CPUs, and tuned tokio runtimes will
show different absolute numbers - relative shape between transports and
sizes should hold.

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

Numbers below are medians of 4–6 runs per cell; the small-size wire
columns vary ±10 % run-to-run (cache / scheduling jitter on a single
core). Larger sizes vary more once kernel send-buffer behaviour kicks
in - take ±25 % at 8 KiB+ as a rough envelope.

| Size    | inproc                  | ipc                  | tcp                  | lz4+tcp              | zstd+tcp             |
|---------|-------------------------|----------------------|----------------------|----------------------|----------------------|
| 128 B   | **2.40M / 308 MB/s**    | 748k / 96 MB/s       | 728k / 93 MB/s       | 535k / 69 MB/s       | 81.4k / 10.4 MB/s    |
| 512 B   | **2.26M / 1.16 GB/s**   | 645k / 330 MB/s      | 624k / 320 MB/s      | 368k / 188 MB/s      | 82.1k / 42.0 MB/s    |
| 2 KiB   | **2.27M / 4.65 GB/s**   | 527k / 1.08 GB/s     | 461k / 945 MB/s      | 324k / 664 MB/s      | 256k / 524 MB/s      |
| 8 KiB   | **2.40M / 19.7 GB/s**   | 245k / 2.01 GB/s     | 255k / 2.09 GB/s     | 197k / 1.61 GB/s     | 185k / 1.51 GB/s     |
| 32 KiB  | **2.36M / 77.3 GB/s**   | 97.0k / 3.18 GB/s    | 57.0k / 1.87 GB/s    | 65.8k / 2.16 GB/s    | 80.3k / 2.63 GB/s    |
| 128 KiB | **2.38M / 312 GB/s**    | 24.0k / 3.15 GB/s    | 14.3k / 1.87 GB/s    | 24.5k / 3.21 GB/s    | 22.8k / 2.99 GB/s    |

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
| 128 B | 758k / 97.0 MB/s             | 513k / 67.7 MB/s / 65.6 MB/s       | 114k / 15.0 MB/s / 14.6 MB/s       |
| 256 B | 694k / 178 MB/s              | 559k / 145 MB/s / 143 MB/s         | 108k / 28.2 MB/s / 27.7 MB/s       |
| 512 B | 726k / 372 MB/s              | 333k / 109 MB/s / 171 MB/s         | 66.6k / 21.0 MB/s / 34.1 MB/s      |
| 1 KiB | 651k / 666 MB/s              | 209k / 82.3 MB/s / 214 MB/s        | 192k / 69.0 MB/s / 196 MB/s        |
| 2 KiB | 473k / 969 MB/s              | 229k / 125 MB/s / 469 MB/s         | 143k / 65.3 MB/s / 292 MB/s        |
| 4 KiB | 338k / 1.38 GB/s             | 200k / 166 MB/s / 819 MB/s         | 96.8k / 53.5 MB/s / 396 MB/s       |
| 16 KiB| 137k / 2.24 GB/s             | 88.3k / 224 MB/s / 1.45 GB/s       | 37.0k / 47.1 MB/s / 606 MB/s       |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~224 MB/s wire while
delivering ~1.45 GB/s of application data. On a 1 Gbps WAN (~125 MB/s
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
| 128 B | 0.97× (skip)  | **5.8×**        | 0.97× (skip)   | **5.1×**         |
| 256 B | 0.98× (skip)  | **11.6×**       | 0.98× (skip)   | **9.9×**         |
| 512 B | 1.57×         | **22.3×**       | 1.62×          | **19.7×**        |
| 1 KiB | 2.60×         | **11.3×**       | 2.84×          | **35.3×**        |
| 2 KiB | 3.76×         | **8.5×**        | 4.47×          | **16.9×**        |

"(skip)" marks sizes below the 512-byte attempt threshold - the
transform doesn't even try to compress, so the no-dict ratio is just
the framing tax. With a dict, the threshold drops to 32 B (lz4) /
64 B (zstd) and small messages compress meaningfully.

Loopback throughput with the same dict (msgs/s · wire MB/s · virt MB/s):

| size  | lz4+tcp                          | zstd+tcp                       |
|-------|----------------------------------|--------------------------------|
| 128 B | 251k / 5.5 MB/s / 32.1 MB/s      | 119k / 3.0 MB/s / 15.2 MB/s    |
| 256 B | 262k / 5.8 MB/s / 67.1 MB/s      | 120k / 3.1 MB/s / 30.6 MB/s    |
| 512 B | 264k / 6.1 MB/s / 135 MB/s       | 120k / 3.1 MB/s / 61.2 MB/s    |
| 1 KiB | 232k / 21.1 MB/s / 238 MB/s      | 81.0k / 2.3 MB/s / 83.0 MB/s   |
| 2 KiB | 222k / 53.6 MB/s / 455 MB/s      | 69.1k / 8.4 MB/s / 142 MB/s    |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

| transport | size  | omq-compio    | omq-tokio      |
|-----------|-------|---------------|----------------|
| inproc    | 128 B | 35 µs (28.5k) | 73 µs (13.8k)  |
| inproc    | 2 KiB | 32 µs (31.0k) | 67 µs (14.9k)  |
| ipc       | 128 B | 68 µs (14.7k) | 133 µs (7.5k)  |
| ipc       | 2 KiB | 59 µs (17.0k) | 140 µs (7.2k)  |
| tcp       | 128 B | 76 µs (13.1k) | 147 µs (6.8k)  |
| tcp       | 2 KiB | 76 µs (13.2k) | 146 µs (6.8k)  |

µs is round-trip wall time; parenthesized number is full request+reply
pairs per second. compio wins inproc by ~2× (single-thread, no
syscall); compio's wire-transport RTT is ~50% lower than tokio's on
IPC (see "compio IPC latency: hop-reduction history" below). The
RTT win comes from Stage 5's recv-direct path: on the inbound side,
`Socket::recv` reads the FD inline instead of waiting for the driver
task to forward parsed messages over a flume hop.

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
| 128 B   | **2.40M**     | 274k         | **748k**   | 192k      | **728k**   | 118k      |
| 512 B   | **2.26M**     | 297k         | **645k**   | 161k      | **624k**   | 128k      |
| 2 KiB   | **2.27M**     | 253k         | **527k**   | 158k      | **461k**   | 105k      |
| 8 KiB   | **2.40M**     | 305k         | **245k**   | 26.8k     | **255k**   | 92.2k     |
| 32 KiB  | **2.36M**     | 313k         | **97.0k**  | 22.5k     | **57.0k**  | 57.0k     |
| 128 KiB | **2.38M**     | 283k         | **24.0k**  | 12.7k     | **14.3k**  | 11.1k     |

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
measured by `omq-proto/benches/mechanism_frame.rs`. Numbers are MiB/s
of plaintext throughput; higher is better.

|  size   |   NULL (memcpy) | CURVE (XSalsa20Poly1305) | BLAKE3ZMQ (ChaCha20-BLAKE3) |
|--------:|----------------:|-------------------------:|----------------------------:|
|    64 B |            1526 |                       41 |                         119 |
|   256 B |            5549 |                      129 |                         304 |
| 1 KiB   |           20778 |                      287 |                         522 |
| 4 KiB   |           44389 |                      430 |                         785 |
|16 KiB   |           44014 |                      407 |                    **1075** |
|64 KiB   |           41091 |                      489 |                    **1246** |

Numbers are stock `cargo bench` (no `-C target-cpu=native`). omq-proto
pins a fork of `chacha20-blake3` adding `#[target_feature(enable =
"avx2")]` annotations that let LLVM auto-vectorize the loops
surrounding the explicit intrinsic calls. Without that patch,
BLAKE3ZMQ runs ~20× slower (~50 MiB/s at bulk sizes) unless every
downstream consumer rebuilds with `-C target-cpu=native`. `crypto_box`
(CURVE) is steady at ~470 MiB/s either way: its salsa20 implementation
has no SIMD path. Reproduce with:

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
