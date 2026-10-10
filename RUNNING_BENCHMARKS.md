# Running Benchmarks

Benchmark and chart commands for omq.rs. Build, test, and CI notes live in
[DEVELOPMENT.md](DEVELOPMENT.md).

Run one benchmark at a time. Stop if a benchmark prints warnings or
timeouts, fix the cause, and rerun before charting. Unless stated otherwise,
the system is quiet during benchmarks, so a bad cell is not noise.

## Cargo Benchmarks

```sh
cargo bench -p omq-tokio --bench omq_push_pull
cargo bench -p omq-tokio --bench omq_inproc_threads
cargo bench -p omq-tokio --bench omq_pull_bulk_fanin
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`,
`OMQ_BENCH_ARENA_THRESHOLD`.
Results append to `$XDG_CACHE_HOME/omq/` (default `~/.cache/omq/`)
unless `OMQ_BENCH_NO_WRITE=1`.

- Throughput: no per-message clocks, logging, or shared measurement counters.
  Use bounded message/byte batches; check deadlines while blocked.
- Exclude warmup and boundary-crossing batches; use actual elapsed time.
  Test clock-read bounds and window boundaries. Per-operation timing is for latency.
- Profile instrumentation and library code. Compare revisions with identical
  harness, flags, workload, warmup, duration, and CPU placement. Measure harness
  changes against the same library.
- Use `--measure-only` for diagnosis; do not lower thresholds to pass a regression.

## Cross-implementation Comparison Benchmarks

All benchmark executables, including the `omq_bench` runner and copied
experiment binaries, MUST start with `omq_` so they are identifiable in `top`.
The Cargo package remains `omq-bench`. The Aeron runner launches Java through
an `omq_aeron_peer` executable symlink.

`omq_bench run comparisons` drives standalone peer binaries:

| binary | source | impls |
|--------|--------|-------|
| `omq_bench_peer_tokio` | `omq-tokio/src/bin/bench_peer_tokio.rs` | omq-tokio-ct |
| `omq_bench_peer_blocking` | `omq-tokio/src/bin/bench_peer_blocking.rs` | omq-tokio-1t, omq-tokio-2t |
| `omq_libzmq_baseline_peer` | `scripts/libzmq_bench_peer.c` | libzmq, libzmq-2t |
| `omq_tmq_bench_peer` | `scripts/tmq_bench_peer/` | tmq |
| `omq_r0z_bench_peer` | `scripts/r0z_bench_peer/` | r0z-async |
| `omq_monocoque_bench_peer` | `scripts/monocoque_bench_peer/` | monocoque-tokio-ct |
| `omq_zmqrs_bench_peer` | `scripts/zmqrs_bench_peer/` | zmq.rs |
| `omq_rzmq_bench_peer` | `scripts/rzmq_bench_peer/` | rzmq, rzmq-iouring |

The `tmq` and `r0z-async` baselines use current-thread Tokio runtimes and one
libzmq IO thread. `tmq` uses its own libzmq bindings; `r0z-async` uses r0z.
Both remain available for direct comparison.

The `monocoque-tokio-ct` baseline uses Monocoque's current-thread Tokio
runtime. PUSH/PULL uses 64 KiB read buffers, write coalescing, and reusable
receive vectors. REQ/REP disables write coalescing. PUB uses one worker;
`MONOCOQUE_PUB_WORKERS` overrides the worker count for separate experiments.

Both peers are standalone Cargo workspaces. Monocoque requires Rust 1.95;
r0z-async supports Unix. Neither dependency enters the OMQ or pyomq builds.

For direct blocking-peer experiments, `OMQ_BENCH_RECV_SPIN_US=50` sets
`Options::recv_spin(Duration::from_micros(50))` on both endpoints. Unset or
zero disables spinning, including for REQ/REP's default latency profile.
Use separate experiment data files when comparing spin budgets.

`omq-tokio-2ut-blocking` measures inproc with two application threads using
blocking send and receive, without spinning. `OMQ_BENCH_HWM` sets both
endpoints' send and receive HWM (default 1000). The peer also accepts HWM
as its optional final argument. Its measured interval excludes setup and
shutdown. On Unix, JSONL rows include CPU seconds, context switches, and
context switches per message under `blocking_inproc`, alongside the HWM
and rounded ring capacity. Other platforms omit unsupported usage counters.

```sh
OMQ_BENCH_HWM=8 cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-2ut-blocking --transport inproc --sizes 64 \
  --no-latency --no-pubsub --id blocking-hwm8
cargo run --release -p omq-tokio --bin omq_bench_peer_blocking -- \
  inproc-2ut-blocking lwm 64 3 1000
```

The comparison runner also exposes `--impl omq-tokio-1t-spin50` as a latency-only
variant, alongside `omq-tokio-1t` and `omq-tokio-ct`. Its 50 us spin budget applies
to both receiving endpoints. Pair and profile selection is available for OMQ
peers:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-1t --impl omq-tokio-1t-spin50 --impl omq-tokio-ct \
  --no-throughput --no-pubsub --transport tcp --transport inproc \
  --latency-pairs req-rep,router-dealer,router-router,pair,client-server,peer,channel \
  --latency-profiles default,latency --sizes 16,64,256,1024,4096
```

`router-dealer` binds ROUTER and connects DEALER; `client-server` binds SERVER
and connects CLIENT. The connecting socket initiates every exchange. ROUTER
and PEER preserve identity frames; SERVER preserves `routing_id` when echoing.
Body sizes exclude routing envelopes. Both endpoints validate a full exchange
before warmup and timing. JSONL latency rows record `latency_pair`,
`workload_profile`, and `recv_spin_us`; other pairs/profiles do not replace
REQ/REP chart points. The libzmq peer also supports ROUTER/DEALER,
ROUTER/ROUTER, and PAIR with `--latency-profiles default`. Its draft socket
types are not enabled in this harness. Other external implementations support
REQ/REP defaults only. Latency tables print p99; cache rows retain p50, p99,
and p99.9.

Each binary speaks a subcommand protocol:

- `push <addr> <size>`: bind PUSH, send forever.
- `pull <addr> <size> <duration>`: connect PULL, count for duration.
- `pub <addr> <size>` / `sub <addr> <size> <duration>`: PUB/SUB throughput.
- `inproc <name> <size> <duration>`: in-process PUSH/PULL.
- `rep <addr> <size>` / `req <addr> <size> <iters> <warmup>`: latency.

The `grpc-rust` baseline uses plaintext gRPC over TCP between separate
processes. PUSH/PULL uses one server-streaming RPC carrying opaque byte
blobs. REQ/REP uses unary `Echo` RPCs. TLS, compression, retries, and
application-level QoS are disabled.

Results go to `~/.cache/omq/comparisons.jsonl`. APPEND-ONLY!

OMQ PUSH/PULL sends reuse immutable bodies; tiny messages stay inline. Each send
submits one message. Receive turns use a 128-message, 64 KiB budget, permitting
one larger message. Each turn has one clock check; boundary-crossing turns are
excluded.

## Updating Charts

Main Rust/comparison latency panels plot p99 round-trip latency, with whiskers
from p50 to p99.9. Each point uses all three percentiles from the same measured
run, and the Y axis includes the full whisker range.

Chart subtitles come from `.chart_hw` in the repo root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 cores, performance governor, turbo off
```

`omq_bench` and binding chart scripts read the repo-root `.chart_hw`
automatically.

Run throughput benchmarks with `OMQ_BENCH_TASKSET=1`. It pins the measured
peer to CPUs 1-2 and the other peer to CPUs 3-4. Unpinned multi-peer runs are
bimodal: the scheduler can stack several IO threads on one CPU. Run latency
benchmarks unpinned: pinned REQ/REP runs show more p99 spikes.

### Main TCP Charts

Refreshes `doc/charts/main_pushpull_tcp.svg` (PUSH/PULL throughput),
`doc/charts/main_pubsub_tcp.svg` (PUB/SUB throughput), and
`doc/charts/main_reqrep_tcp.svg` (REQ/REP latency), TCP only.
Rebench omq impls only for PUSH/PULL and REQ/REP, then regenerate:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-ct --impl omq-tokio-1t --transport tcp --no-pubsub
cargo run --release -p omq-bench -- chart main
```

Rebench all impls for all three main TCP charts:

```sh
cargo run --release -p omq-bench -- run comparisons --transport tcp
cargo run --release -p omq-bench -- chart main
```

### QUIC Charts

Produces `doc/charts/quic/{pushpull,reqrep,pubsub,fanout,fanin}.svg`, OMQ
over QUIC against OMQ with CURVE over TCP. Both sides encrypt:

```sh
OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run comparisons \
  --impl omq-curve-1t --impl omq-curve-2t --transport tcp \
  --no-latency --fanout --fanin \
  --pubsub-peers 4,8 --fanout-peers 4,8 --fanin-peers 4,8
OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-1t --impl omq-tokio-2t --transport quic \
  --no-latency --fanout --fanin \
  --pubsub-peers 4,8 --fanout-peers 4,8 --fanin-peers 4,8
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-curve-1t --impl omq-curve-2t --transport tcp \
  --no-throughput --no-pubsub
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-1t --impl omq-tokio-2t --transport quic \
  --no-throughput --no-pubsub
cargo run --release -p omq-bench -- chart quic
```

CURVE impls run the multi-peer benches only when named with `--impl`.

The [lossy-link chart](doc/charts/dart/lossy.svg) compares QUIC streams with
Dart LAN/adaptive using matched socket pairs and simulated link conditions.

QUIC bench peers set 4 MiB UDP socket buffers through `recv_buffer_size` and
`send_buffer_size`. Linux caps them at `net.core.rmem_max` and
`net.core.wmem_max`, so check both are at least 4 MiB. With smaller buffers,
multi-peer QUIC runs drop packets on loopback (`RcvbufErrors` in
`/proc/net/snmp`) and QUIC congestion control limits throughput.

### Standalone Quinn DATAGRAM

This measures Quinn's DATAGRAM API directly between two Linux processes,
without OMQ sockets, framing, or queues. Verified TLS 1.3/AES-128-GCM,
Quinn's default Cubic controller, pacing, and GSO/GRO remain enabled.
The Rust runner is part of `omq-bench`.
The pinned datagram runners require Linux and `sha256sum`; the Aeron runner
also requires Java and `unzip` to verify the JAR version.

```sh
cargo build --release -p omq-tokio --features quic --example omq_quinn_datagram_peer
cargo run --release -p omq-bench -- run quinn-datagram --binary path/to/omq_quinn_datagram_peer
cargo run --release -p omq-bench -- run quinn-datagram --binary path/to/omq_quinn_datagram_peer --io-spin 50
```

The default is three serial runs at 16 B and 1 KiB: 3-second throughput
windows after 200 ms warmup, and 100,000 echo RTT samples after 20,000 warmup
exchanges. `inline` uses one current-thread runtime per process, with the
application and Quinn's drivers on CPU slots 1 and 3. `--layout split` uses
application/IO slots 0/1 and 5/3. `--layout multi` uses two Tokio workers per
process on those same pairs of CPUs. Application spin is only available with
the split layout; repeatedly polling Quinn's receive API contends for its
connection lock. `--io-spin 50` selects a benchmark socket adapter that probes
UDP for at most 50 us after activity, scheduling a fresh endpoint turn after
each empty probe, then uses ordinary Tokio readiness waits when idle.
Quinn's packet protection and congestion control are unchanged.

Throughput counts application receives, including drops separately.
`send_datagram_wait()` prevents eviction from the sender queue; the receiver
can still discard DATAGRAMs when its own queue fills. Bodies are reusable
immutable `Bytes`, deliberately excluding per-message sender allocation from
this transport ceiling. Both byte queues are bounded at 8 MiB. Drain turns
are limited to `--batch` messages (256 by default) and 64 KiB. The runner
records actual kernel socket buffer sizes, which can be smaller than requested.
RTT exchanges validate unique sequence tags and the full echoed body.

Results append to `~/.cache/omq/quinn-datagram.jsonl`, including binary
digests and protocol statistics.
See [the measured results](doc/quinn-datagram-benchmark.md).

Profile a single case serially:

```sh
cargo run --release -p omq-bench -- run quinn-datagram --binary path/to/omq_quinn_datagram_peer \
  --repeats 1 --kind throughput --sizes 1024 \
  --profile /mnt/bench/tmp/quinn-datagram-profile --profile-side send
perf report --stdio --no-inline --no-children -g none --percent-limit 1 \
  -i /mnt/bench/tmp/quinn-datagram-profile/send-1024.data
```

### Dart Charts

SCATTER/GATHER measures messages received by the application. CLIENT/SERVER
measures RTT with the latency profile on both endpoints. Both transports use
the same body sizes, application spin, and CPU placement. Dart initializes an
8192-buffer, 2 KiB standalone send pool, separate from its internal receive
pool. Bodies up to 55 bytes stay inline; larger bodies use pooled or owned storage.
Dart also has an independent IO spin budget. Bounded spins are at most 50 us.
Use `--continuous-spin` and `--continuous-io-spin` to compare continuous
application and Dart IO polling independently. These select `Duration::MAX`
instead of the corresponding bounded budget and are excluded from charts.
Continuous IO polling requires `--transport dart`. Save experiments with
`--output` to keep the normal benchmark cohorts separate.
Throughput enables `recv_batching` for both transports. Latency uses ordinary
single-message receives. The sender submits one message per `try_send` call
and reuses its pool handle. Datagram packing happens inside Dart.
Sender capacity retries use the application spin limit, then yield;
successful sends reset that wait window. Throughput checks time every 64
successful sends. Empty pool/capacity probes share a timestamp for up to
eight attempts; capacity waits compare an absolute deadline.
Latency timestamps each measured round trip before send and after receive
to retain individual percentiles.
`--window-messages` varies Dart's receive/retention slots per peer (default
256). Both peers report the actual window, and the runner checks it. Save
window experiments with `--output`; the Dart transport charts require 256 slots.

Build the peer, then pass its executable path to the Linux runner:

```sh
cargo build --release -p omq-tokio --features dart --bin omq_dart_bench_peer
cargo run --release -p omq-bench -- run dart --binary path/to/omq_dart_bench_peer \
  --cpus 0,1,2,3,4,5 --spin 50 --io-spin 50 --congestion lan --repeats 3
cargo run --release -p omq-bench -- chart dart
```

The runner verifies six distinct cores. Send application/IO use CPU slots
0/1; receive IO/application use slots 3/5. Each side runs in its own process
with one owned IO thread. Throughput excludes a 200 ms warmup and batches
completed after the measurement deadline. A bounded 2-second drain records delivery
after the window separately and requires acknowledgment of native sends. RTT excludes the configured warmup iterations.
`--warmup-seconds` changes throughput warmup; `--duration` changes its
measurement window; `--drain-seconds` changes the bounded delivery/ACK drain
(default 2 seconds). Save nonstandard runs with `--output`; loopback charts
require 200 ms warmup, 3-second measurements, and the default drain.
Warnings, IO failures, and timeouts stop the run.

Results append to `~/.cache/omq/dart.jsonl`. Rows include offered and received
counts, missing/excess counts, local overflow, pool exhaustion, RTT tails,
revision, binary SHA-256, Dart wire version, congestion mode, RTT iteration
counts, spin budgets, receive batching, receive/retention windows, and CPU IDs.
Monotonic sequence tags and their complements verify ordered, unique,
uncorrupted delivery. Missing
messages or acknowledgments after the drain stop the run.
Dart packs up to 128 already queued messages per datagram without waiting,
using a count byte, byte-length table, session header, and concatenated
payloads. Payloads over 255 bytes use individual DATA datagrams with no
length escapes. GSO/GRO additionally batch complete datagrams.
Report RTT p50, p99, and p99.9 separately.
For tail investigations, `--kind latency --latency-samples path/to/samples`
saves every RTT in chronological order as CSV after timing completes. The
result also records Dart retransmission and stall counters. These diagnostic
runs are excluded from charts; keep them separate from ordinary cohorts.
The runner defaults to three independent runs, 3-second throughput windows,
and 200,000 warmup plus 100,000 measured RTT exchanges. It rotates the size
order between repeats to distribute startup effects; `--order fixed` supports
order investigations and is excluded from RTT charts. It prints each result
and the median/minimum/maximum for each size. `--check-gates` requires LAN
medians of at least 5 million 16-byte messages/s, 1 GB/s of 1024-byte payloads,
and RTT p99 at most 25 us. These experiment gates are separate from the RFC.
Run `--congestion adaptive` separately; it is excluded from LAN gate checks.

Charts require three eligible runs from the same binary, matching workload
profiles, batched throughput receives, and Dart protocol version 1. Migrated
experimental versions 6 through 8 retain their original version in
`experimental_wire_version`; large bodies require original version 7 or newer.
They plot LAN and
adaptive separately; historical unreliable rows are excluded.
Outputs: `doc/charts/dart/{scattergather,clientserver}.svg`.
RTT sizes are 16 B, 32 B, 64 B, 128 B, 256 B, 512 B, 1 KiB, 2 KiB, 4 KiB,
8 KiB, and 16 KiB. The RTT chart uses a linear Y axis from 1 to 100 us with
10 us ticks. A triangle and measured value
identify p99.9 whiskers that extend above the axis limit.
Throughput adds 32 KiB, 64 KiB, 256 KiB, 1 MiB, 4 MiB, and 8 MiB. Its two
panels show messages/s through 1 KiB on the left and GB/s from 256 B on the
right, matching the main TCP chart's overlap.
With the peer built using `--features 'dart quic'`, `--transport quic` runs
the same socket pairs and verification over TLS-authenticated QUIC streams.
The runner generates trusted benchmark credentials. Use `--output` for
simulated-link measurements; they do not belong in loopback chart cohorts.
`cargo run --release -p omq-bench -- chart lossy` generates
[`doc/charts/dart/lossy.svg`](doc/charts/dart/lossy.svg) from `lossy.jsonl`.
It shows received Mbps at 1 KiB/16 KiB and p99 RTT at 1 KiB across random
loss rates 0%, 0.1%, 1%, and 5%. The link shares 100 Mbps between directions
with 1 ms delay each way and MTU 1500; UDP ingress netem runs on IFB with
segmentation offloads disabled. Rows add `socket_pair` (`scatter-gather` or
`client-server`) and a `netem` object with `loss_ppm`, `loss_model: "random"`,
`rate_mbps: 100`, `shared_rate: true`, `delay_us: 1000`, `mtu: 1500`,
`ecn: false`, `segmentation_offloads: false`, and `placement: "udp-ingress-ifb"`.
Large OMQ throughput bodies reuse a bounded cache of prepared `Bytes` on both
transports. TCP also uses it at 1 KiB; smaller TCP bodies and Dart bodies up
to 1 KiB use the fixed pool. Retag only unique bodies, preserving every
in-flight reference. The cache holds at least 64 bodies, targeting 1 MiB. RTT
prepares each outgoing body before the timestamp. Dart's IO task fragments
larger bodies and the receiver validates the assembled message.
Throughput verifies every payload byte using a vectorizable word reduction,
plus per-message sequence and complement checks.

The runner also supports latency comparisons with no owned IO thread:

```sh
cargo run --release -p omq-bench -- run dart --binary path/to/omq_dart_bench_peer \
  --transport dart --kind latency --runtime current-poll --spin 0 --io-spin 0
```

`current` uses the ordinary current-thread Tokio runtime and may sleep in the
reactor. `current-poll` keeps a cooperative task ready throughout measurement
and polls the reactor after every scheduled task. Both use the async Socket
API and `Context::current()`, with application and transport sharing CPU slot
0 or 5 in each process. They retain socket queues and driver tasks; they are
not Exclusive mode, which currently supports TCP only. These modes require
zero application/transport spin budgets. Their explicit runtime and polling
metadata keep them separate from the charts' owned-IO series.

The Aeron baseline uses 1.53.3, an exclusive publication, 64 MiB terms,
and one embedded SHARED Media Driver per process, labeled `1 IO (SHARED)`.
Congestion control uses Aeron's default static receive window. Each JVM starts on its
application core; the runner pins the shared driver to its separate IO core
before releasing the start barrier. CPU slots match the OMQ runner.
The JVM uses 3-second throughput warmup and 200,000 RTT warmup exchanges;
each independent run measures 3 seconds or 100,000 RTT samples. Bodies carry
sequence tags and complements, and the receiver validates their contents.
Clock reads bound throughput batches rather than timing every message.
Delivery after throughput ends must drain within 2 seconds. Other Aeron
settings, including its default congestion control and idle strategy, remain
at their defaults.

```sh
curl -fLsS https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar \
  -o /tmp/aeron-all-1.53.3.jar
javac -cp /tmp/aeron-all-1.53.3.jar -d /tmp/aeron-dart-classes \
  scripts/aeron_dart_peer/AeronUdpPeer.java
TMPDIR=/tmp cargo run --release -p omq-bench -- run aeron-dart \
  --jar /tmp/aeron-all-1.53.3.jar --classes /tmp/aeron-dart-classes
```

Aeron rows append to `~/.cache/omq/dart-aeron.jsonl`. Its chart points use
three independent JVM pairs with the same class/JAR digest and CPU placement.
Latency whiskers come from the median-p99 run, as for OMQ. Warmup is recorded
separately and does not count toward the measured samples.

Profile one side at a time. Profiled rows are excluded from charts:

```sh
cargo run --release -p omq-bench -- run dart --binary path/to/omq_dart_bench_peer \
  --transport dart --kind throughput --sizes 16 \
  --profile /mnt/bench/tmp/dart-profile --profile-side receive
perf report --stdio --no-inline -g none \
  -i /mnt/bench/tmp/dart-profile/gather-16.data
```

### Cross-library Comparison Charts

Produces `doc/charts/{pushpull,pubsub,reqrep}/*.svg`,
`doc/charts/pushpull/fan{out,in}/tcp.svg`,
`doc/charts/main_pushpull_tcp.svg`, `doc/charts/main_pubsub_tcp.svg`,
`doc/charts/main_reqrep_tcp.svg`:

```sh
cargo run --release -p omq-bench -- run comparisons --omq
cargo run --release -p omq-bench -- run comparisons --omq \
  --transport tcp --no-latency --no-pubsub \
  --sizes 32,128,512,2048,8192,32768 --allow-non-chart-sizes
cargo run --release -p omq-bench -- chart comparison
cargo run --release -p omq-bench -- chart main
```

Full refresh after omq/rzmq changes (all impls, all transports):

```sh
test -f .chart_hw
cargo run --release -p omq-bench -- run comparisons \
  --transport tcp --transport ipc --transport inproc \
  --fanout --fanin --pubsub-peers 4,32
cargo run --release -p omq-bench -- chart comparison
cargo run --release -p omq-bench -- chart main
cargo run --release -p omq-bench -- chart fanio
```

**CPU% charting rule.** Charts show only the "interesting" process's
CPU, not the sum of all processes:

| benchmark | charted process | JSONL field |
|-----------|----------------|-------------|
| PUSH/PULL throughput | sender (PUSH) | `push_cpu_time` |
| PUB/SUB | sender (PUB) | `pub_cpu_time` |
| fan-out (1 PUSH to N PULL) | sender (PUSH) | `push_cpu_time` |
| fan-in (N PUSH to 1 PULL) | receiver (PULL) | `pull_cpu_time` |
| REQ/REP latency | sender (REQ) | `req_cpu_time` |

The combined `cpu_time` field (sum of all processes) is still recorded
for backwards compatibility. Chart loaders prefer the per-process
field and fall back to `cpu_time` when it is absent.

### CURVE PUB/SUB Chart

Refreshes `doc/charts/pubsub/curve_tcp.svg`. The `--curve` flag
auto-includes CURVE impls from the same family as the selected impls
(e.g. `--omq --curve` adds `omq-curve-1t` and `omq-curve-2t`):

```sh
cargo run --release -p omq-bench -- run comparisons \
  --omq --curve --transport tcp --no-throughput --no-latency
cargo run --release -p omq-bench -- chart pubsub
```

### Mechanism Chart

```sh
cargo run --release -p omq-bench -- run mechanism tokio --chart-sizes
cargo run --release -p omq-bench -- chart mechanism
```

### PUB/SUB LZ4 Compression Chart

```sh
cargo run --release -p omq-bench -- run pubsub-lz4 --chart
```

Or bench and chart separately:

```sh
cargo run --release -p omq-bench -- run pubsub-lz4
cargo run --release -p omq-bench -- chart pubsub-lz4
```

### Compression Chart

The LZ4 and Zstd charts use measured Linux netem links. Each invocation
creates a private user/network namespace; it needs `unshare`, `ip`, `tc`,
and `ethtool`, with unprivileged user namespaces enabled.

```sh
for rate in 1000 100 10; do
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-lz4 --link-mbps "$rate"
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-zstd --level 1 --link-mbps "$rate"
done
cargo run --release -p omq-bench -- chart lz4
cargo run --release -p omq-bench -- chart zstd
```

Rates include both directions' IP traffic; delay is 1 ms each way and MTU
is 1500. Segmentation offloads are disabled. Three repeats use 0.5 s active
warmup and at least a 2 s receive interval, extended for large messages on
slow links. Payloads are checked; boundary-crossing
batches are excluded. Sender CPU covers the measured interval only.

### pyomq Bindings Charts

```sh
cd bindings/pyomq
maturin develop --release
python scripts/update_perf.py --impl pyomq
python scripts/update_perf.py --chart-only
```

For a local smoke run that does not append JSONL or update docs:

```sh
python scripts/update_perf.py --quick --impl pyomq
```

### Ruby Binding Comparison Chart

```sh
cd bindings/ruby
ruby -Ilib scripts/update_perf.rb
```
