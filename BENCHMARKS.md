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

| Size    | inproc                  | ipc                  | tcp                  | lz4+tcp              | zstd+tcp             |
|---------|-------------------------|----------------------|----------------------|----------------------|----------------------|
| 128 B   | **2.71M / 347 MB/s**    | 774k / 99 MB/s       | 713k / 91 MB/s       | 455k / 58 MB/s       | 51.5k / 6.6 MB/s     |
| 512 B   | **2.84M / 1.45 GB/s**   | 728k / 373 MB/s      | 695k / 356 MB/s      | 402k / 206 MB/s      | 53.9k / 27.6 MB/s    |
| 2 KiB   | **2.68M / 5.48 GB/s**   | 482k / 987 MB/s      | 448k / 918 MB/s      | 331k / 678 MB/s      | 241k / 493 MB/s      |
| 8 KiB   | **2.82M / 23.1 GB/s**   | 162k / 1.32 GB/s     | 164k / 1.34 GB/s     | 235k / 1.92 GB/s     | 152k / 1.24 GB/s     |
| 32 KiB  | **2.72M / 89.1 GB/s**   | 52.1k / 1.71 GB/s    | 47.3k / 1.55 GB/s    | 51.1k / 1.67 GB/s    | 50.1k / 1.64 GB/s    |
| 128 KiB | **2.70M / 354 GB/s**    | 13.8k / 1.81 GB/s    | 11.6k / 1.52 GB/s    | 13.3k / 1.74 GB/s    | 10.9k / 1.42 GB/s    |

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
| 1 KiB   | 2.59×   | 2.84×    |
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
| 128 B | 822k / 105 MB/s              | 531k / 70 MB/s / 68 MB/s           | 87k / 11 MB/s / 11 MB/s            |
| 256 B | 766k / 196 MB/s              | 564k / 147 MB/s / 144 MB/s         | 107k / 28 MB/s / 27 MB/s           |
| 512 B | 553k / 283 MB/s              | 269k / 88 MB/s / 138 MB/s          | 78k / 25 MB/s / 40 MB/s            |
| 1 KiB | 458k / 469 MB/s              | 267k / 106 MB/s / 274 MB/s         | 166k / 60 MB/s / 170 MB/s          |
| 2 KiB | 360k / 737 MB/s              | 217k / 118 MB/s / 444 MB/s         | 162k / 74 MB/s / 332 MB/s          |
| 4 KiB | 267k / 1.09 GB/s             | 153k / 128 MB/s / 628 MB/s         | 104k / 58 MB/s / 427 MB/s          |
| 16 KiB| 91k / 1.50 GB/s              | 60k / 151 MB/s / 978 MB/s          | 42k / 54 MB/s / 695 MB/s           |

On loopback, plain TCP wins msgs/s - compression's CPU cost has no
offsetting wire-bandwidth payoff because there's no bandwidth scarcity.
**Look at the wire MB/s column** to predict behavior on a bandwidth-
bounded link: at 16 KiB messages, lz4+tcp ships ~130 MB/s wire while
delivering ~840 MB/s of application data. On a 1 Gbps WAN (~125 MB/s
wire ceiling) plain `tcp` would deliver 125 MB/s of application data
total - `lz4+tcp` would deliver ~840 MB/s and `zstd+tcp` ~636 MB/s.
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
| 128 B | 0.97× (skip)  | **5.6×**        | 0.97× (skip)   | **5.1×**         |
| 256 B | 0.98× (skip)  | **11.1×**       | 0.98× (skip)   | **9.9×**         |
| 512 B | 1.57×         | **21.3×**       | 1.62×          | **19.7×**        |
| 1 KiB | 2.59×         | **10.8×**       | 2.84×          | **35.3×**        |
| 2 KiB | 3.76×         | **8.4×**        | 4.47×          | **16.9×**        |

"(skip)" marks sizes below the 512-byte attempt threshold - the
transform doesn't even try to compress, so the no-dict ratio is just
the framing tax. With a dict, the threshold drops to 32 B (lz4) /
64 B (zstd) and small messages compress meaningfully.

Loopback throughput with the same dict (msgs/s · wire MB/s · virt MB/s):

| size  | lz4+tcp                          | zstd+tcp                       |
|-------|----------------------------------|--------------------------------|
| 128 B | 274k / 6.3 MB/s / 35 MB/s        | 132k / 3.3 MB/s / 17 MB/s      |
| 256 B | 276k / 6.3 MB/s / 71 MB/s        | 130k / 3.4 MB/s / 33 MB/s      |
| 512 B | 306k / 7.4 MB/s / 157 MB/s       | 121k / 3.1 MB/s / 62 MB/s      |
| 1 KiB | 240k / 23 MB/s / 245 MB/s        | 126k / 3.7 MB/s / 129 MB/s     |
| 2 KiB | 183k / 45 MB/s / 375 MB/s        | 104k / 13 MB/s / 213 MB/s      |

Same loopback caveat: CPU cost without bandwidth payoff. On a
bandwidth-bounded link the wire-MB/s column is the actual link
load and the virt-MB/s column is what the application gets out
the other end. The auto-train mode (default on for `zstd+tcp`)
reaches similar ratios after the first ~1000 messages or 100 KiB
of plaintext.

## REQ/REP round-trip latency (single peer)

| transport | size  | omq-compio    | omq-tokio     |
|-----------|-------|---------------|---------------|
| inproc    | 128 B | 31 µs (32.2k) | 58 µs (17.3k) |
| inproc    | 2 KiB | 31 µs (32.1k) | 53 µs (19.0k) |
| ipc       | 128 B | 173 µs (5.8k) | 134 µs (7.5k) |
| ipc       | 2 KiB | 176 µs (5.7k) | 130 µs (7.7k) |

µs is round-trip wall time; parenthesized number is full request+reply
pairs per second. compio wins inproc by ~2× (single-thread, no syscall);
tokio wins ipc by ~30% (multi-thread keeps the request and reply
threads on separate workers, hiding the unix-domain syscall round-trip).
Pick the backend that matches your dominant transport.

The IPC latencies (130–180 µs) are higher than they should be -
expect single-digit-tens-of-µs for a unix-domain round trip. The
likely culprit: actor / task scheduling between the request-side
socket driver, the stream driver, and the reply path adds ~4 cross-
task hops per round trip. Worth profiling for sub-msec request/reply
workloads; see `omq-compio/benches/req_rep.rs` for the bench harness.
Tracked as a follow-up.

## Backend comparison: PUSH/PULL throughput, single peer

| Size    | inproc compio        | inproc tokio  | ipc compio  | ipc tokio | tcp compio | tcp tokio |
|---------|----------------------|---------------|-------------|-----------|------------|-----------|
| 128 B   | **2.71M**            | 290k          | **774k**    | 171k      | **713k**   | 110k      |
| 512 B   | **2.84M**            | 328k          | **728k**    | 180k      | **695k**   | 90k       |
| 2 KiB   | **2.68M**            | 303k          | **482k**    | 106k      | **448k**   | 121k      |
| 8 KiB   | **2.82M**            | 304k          | **162k**    | 86k       | **164k**   | 88k       |
| 32 KiB  | **2.72M**            | 314k          | **52.1k**   | 22.5k     | 47.3k      | **53.0k** |
| 128 KiB | **2.70M**            | 278k          | **13.8k**   | 12.4k     | 11.6k      | **12.8k** |

Numbers are msg/s. compio dominates on small messages where the io_uring
+ direct-routing path shines; tokio catches up at large sizes where
syscall overhead is amortized and tokio's multi-thread runtime overlaps
sender / receiver across cores. **Note that compio here is one core
versus tokio's whole box** - see the caveat at the top of this
document. A multi-runtime compio deployment lifts wire throughput
another 20-40%.

## Mechanism per-frame cost (sans-I/O)

Per-frame cryptographic cost of sealing one ZMTP frame payload, as
measured by `omq-proto/benches/mechanism_frame.rs`. Numbers are MiB/s
of plaintext throughput; higher is better.

|  size   |   NULL (memcpy) | CURVE (XSalsa20Poly1305) | BLAKE3ZMQ (ChaCha20-BLAKE3) |
|--------:|----------------:|-------------------------:|----------------------------:|
|    64 B |            1299 |                       37 |                         108 |
|   256 B |            5194 |                      124 |                         291 |
| 1 KiB   |           18426 |                      255 |                         477 |
| 4 KiB   |           39457 |                      304 |                         711 |
|16 KiB   |           43163 |                      423 |                     **905** |
|64 KiB   |           37313 |                      412 |                    **1093** |

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
