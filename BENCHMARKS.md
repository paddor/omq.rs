# Benchmarks

Numbers below are from a single sweep on Linux 6.12 (Debian 13) running
in a VM on a 2019 MacBook Pro (Intel i7-9750H, 4 vCPU), Rust 1.95.0,
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
| 128 B   | **2.31M / 296 MB/s**    | 736k / 94.2 MB/s     | 742k / 95.0 MB/s     | 404k / 51.8 MB/s     | 84.0k / 10.7 MB/s    |
| 512 B   | **2.51M / 1.29 GB/s**   | 650k / 333 MB/s      | 679k / 347 MB/s      | 280k / 144 MB/s      | 90.9k / 46.6 MB/s    |
| 2 KiB   | **2.50M / 5.11 GB/s**   | 578k / 1.18 GB/s     | 563k / 1.15 GB/s     | 237k / 485 MB/s      | 280k / 573 MB/s      |
| 8 KiB   | **2.17M / 17.8 GB/s**   | 313k / 2.57 GB/s     | 234k / 1.92 GB/s     | 164k / 1.35 GB/s     | 197k / 1.61 GB/s     |
| 32 KiB  | **1.96M / 64.2 GB/s**   | 98.0k / 3.21 GB/s    | 76.5k / 2.51 GB/s    | 69.3k / 2.27 GB/s    | 84.8k / 2.78 GB/s    |
| 128 KiB | **2.24M / 294 GB/s**    | 26.7k / 3.50 GB/s    | 16.8k / 2.21 GB/s    | 21.0k / 2.75 GB/s    | 26.1k / 3.42 GB/s    |

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
| 128 B | 594k / 76.0 MB/s             | 520k / 68.6 MB/s / 66.6 MB/s       | 116k / 15.4 MB/s / 14.9 MB/s       |
| 256 B | 539k / 138 MB/s              | 513k / 133 MB/s / 131 MB/s         | 122k / 31.6 MB/s / 31.1 MB/s       |
| 512 B | 640k / 328 MB/s              | 322k / 105 MB/s / 165 MB/s         | 83.4k / 26.4 MB/s / 42.7 MB/s      |
| 1 KiB | 560k / 573 MB/s              | 313k / 123 MB/s / 321 MB/s         | 192k / 69.2 MB/s / 197 MB/s        |
| 2 KiB | 452k / 926 MB/s              | 262k / 143 MB/s / 537 MB/s         | 162k / 74.1 MB/s / 331 MB/s        |
| 4 KiB | 332k / 1.36 GB/s             | 202k / 168 MB/s / 828 MB/s         | 92.5k / 51.1 MB/s / 379 MB/s       |
| 16 KiB| 122k / 2.01 GB/s             | 80.0k / 203 MB/s / 1.31 GB/s       | 38.3k / 48.8 MB/s / 628 MB/s       |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~203 MB/s wire while
delivering ~1.31 GB/s of application data. On a 1 Gbps WAN (~125 MB/s
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
| 128 B | 260k / 5.7 MB/s / 33.3 MB/s      | 122k / 3.0 MB/s / 15.6 MB/s    |
| 256 B | 320k / 7.0 MB/s / 82.0 MB/s      | 130k / 3.4 MB/s / 33.3 MB/s    |
| 512 B | 304k / 7.0 MB/s / 156 MB/s       | 119k / 3.1 MB/s / 60.9 MB/s    |
| 1 KiB | 274k / 25.0 MB/s / 281 MB/s      | 123k / 3.6 MB/s / 126 MB/s     |
| 2 KiB | 212k / 51.1 MB/s / 434 MB/s      | 108k / 13.1 MB/s / 222 MB/s    |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

| transport | size  | omq-compio    | omq-tokio      |
|-----------|-------|---------------|----------------|
| inproc    | 128 B | 29 µs (34.1k) | 64 µs (15.7k)  |
| inproc    | 2 KiB | 30 µs (33.0k) | 58 µs (17.2k)  |
| ipc       | 128 B | 53 µs (18.8k) | 153 µs (6.5k)  |
| ipc       | 2 KiB | 87 µs (11.5k) | 121 µs (8.3k)  |
| tcp       | 128 B | 66 µs (15.1k) | 146 µs (6.9k)  |
| tcp       | 2 KiB | 66 µs (15.3k) | 146 µs (6.8k)  |

µs is round-trip wall time; parenthesized number is full request+reply
pairs per second. compio wins inproc by ~2× (single-thread, no
syscall); on wire transports compio's RTT runs roughly half tokio's at
small messages (see "compio IPC latency: hop-reduction history"
below). Cells are single 0.5 s runs; small-message wire RTTs jitter
±15-25% on a 4-vCPU VM, so read the trend, not any single cell. The
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
| 128 B   | **2.31M**     | 337k         | **736k**   | 184k      | **742k**   | 135k      |
| 512 B   | **2.51M**     | 352k         | **650k**   | 202k      | **679k**   | 130k      |
| 2 KiB   | **2.50M**     | 350k         | **578k**   | 137k      | **563k**   | 122k      |
| 8 KiB   | **2.17M**     | 325k         | **313k**   | 51.6k     | **234k**   | 110k      |
| 32 KiB  | **1.96M**     | 357k         | **98.0k**  | 16.5k     | **76.5k**  | 37.5k     |
| 128 KiB | **2.24M**     | 311k         | **26.7k**  | 11.5k     | **16.8k**  | 12.3k     |

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
|    64 B |    1.68 GB/s    |               44 MB/s    |                  125 MB/s   |
|   256 B |    5.22 GB/s    |              121 MB/s    |                  290 MB/s   |
| 1 KiB   |   22.8 GB/s     |              306 MB/s    |                  579 MB/s   |
| 4 KiB   |   44.5 GB/s     |              447 MB/s    |                  823 MB/s   |
|16 KiB   |   48.0 GB/s     |              504 MB/s    |             **1.07 GB/s**   |
|64 KiB   |   43.4 GB/s     |              532 MB/s    |             **1.28 GB/s**   |

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
BLAKE3ZMQ runs ~20× slower (~52 MB/s at bulk sizes) unless every
downstream consumer rebuilds with `-C target-cpu=native`. `crypto_box`
(CURVE) plateaus around ~530 MB/s either way: its salsa20
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
