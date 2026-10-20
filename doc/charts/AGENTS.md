# Chart generation rules

Generated SVGs. Do not hand-edit. Data source: `~/.cache/omq/*.jsonl`.

## Regeneration

```sh
cargo run --release -p omq-bench -- chart          # all charts
cargo run --release -p omq-bench -- chart main      # main overview only
cargo run --release -p omq-bench -- chart comparison # per-transport
cargo run --release -p omq-bench -- chart pubsub    # PUB/SUB + CURVE
cargo run --release -p omq-bench -- chart fanio     # fan-out/fan-in
cargo run --release -p omq-bench -- chart compression # LZ4 and Zstd
```

A chart refresh without new benchmarks just re-renders existing data.
Benchmark processes must not run in parallel.
After a performance change, rebenchmark and regenerate the affected charts
before presenting the next decision to the user.

## Main charts (3 files)

Data: `comparisons.jsonl`. External impls required.

| file | impls |
|------|-------|
| `main_pushpull_tcp.svg` | libzmq 1IO, omq 1IO, omq CT, omq MT, tmq, r0z-async, monocoque CT, zmq.rs, rzmq-iouring |
| `main_reqrep_tcp.svg` | libzmq 1IO, omq 1IO, omq 1IO with 50 μs receive spin, omq CT, tmq, r0z-async, monocoque CT, zmq.rs, rzmq-iouring |
| `main_pubsub_tcp.svg` | libzmq 1IO, libzmq 2IO, omq 1IO/2IO, tmq, r0z-async, monocoque CT + 1 worker, zmq.rs, rzmq-iouring |

PUSH/PULL sizes: 16B..8MiB (15 points). PUB/SUB sizes: 16B..16KiB
(6 points, 32 peers). REQ/REP latency sizes: 16B, 64B, 256B, 1KiB, 4KiB.

MT uses a standard Tokio multithread runtime with one worker per available CPU
in each sender and receiver (6 on this VM). MT peers retain the full CPU set,
including when `OMQ_BENCH_TASKSET` pins other runtime modes. The legend uses
recorded workers per process when available.

The main REQ/REP latency chart uses a 600 px panel with a Y axis from
0 to 250 μs in 25 μs steps.

The `omq-tokio-1t-spin50` latency series sets `recv_spin` to 50 μs on both
endpoints, with one owned IO thread per process. Main and comparison REQ/REP
charts exclude other socket pairs and explicit non-default profile runs.
Older cache rows without pair/profile metadata remain valid REQ/REP defaults.

On `main`, only the three `main_*.svg` overview charts include other
implementations, except the DART charts, which also include Aeron.
All other secondary charts in `doc/charts/` show OMQ and libzmq only.

## Secondary comparison charts (6 files)

Data: `comparisons.jsonl`. OMQ vs libzmq only.

**Throughput** (`pushpull/{tcp,ipc,inproc}.svg`):
- TCP/IPC: libzmq 1IO, omq 1IO
- Inproc: libzmq 2 UT, omq CT, omq 2 UT. GB/s panel uses log scale.

**Latency** (`reqrep/{tcp,ipc,inproc}.svg`):
- TCP: libzmq 1IO, omq 1IO, omq 1IO with 50 μs receive spin, omq CT, omq EXCL
- IPC: libzmq 1IO, omq 1IO, omq 1IO with 50 μs receive spin, omq CT
- Inproc: libzmq 2 UT, omq 2 UT, omq CT

Main and comparison latency charts plot p99 round-trip latency with whiskers
from p50 to p99.9. Each point and its whiskers come from the same cached run.
Comparison Y axes include the full whisker range. All latency charts include CT.
Sizes: 16B, 64B, 256B, 1KiB, 4KiB.
The IPC refresh uses three runs per cell, 100,000 measured exchanges after
20,000 warmup exchanges, retaining the median-p99 run.

## TCP byte-limit chart

`pushpull/tcp_rate_limit.svg` reads `pushpull-rate-limit.jsonl`.
Two TCP processes, 1 IO each; uncapped OMQ versus sender egress of
100,000,000 bytes/s with a 1,000,000-byte burst. Use all 15 main PUSH/PULL
sizes, a 1000 Mbit/s shared netem link, 1 ms/direction, MTU 1500, and offloads
off. Require three verified repeats per cell, 1 s warmup, at least 3 s
measurement (extended for 110 large messages), and sender CPUs 1-2 / receiver
3-4. Select one complete campaign/binary cohort; plot median throughput with
its matching CPU samples using the standard dual-panel layout.

## PUB/SUB charts (3 files)

Data: `comparisons.jsonl`, kind `pub_sub`.

| file | panels | impls |
|------|--------|-------|
| `pubsub/tcp.svg` | 4, 32 subscribers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO |
| `pubsub/ipc.svg` | 4, 32 subscribers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO |
| `pubsub/curve_tcp.svg` | 16 peers | libzmq-curve 1IO/2IO, omq-curve 1IO/2IO |

IO labels refer to the publisher. IPC uses one receiver process with a fixed
3 IO threads on CPUs 0,3-5; publishers use CPUs 1-2. Receive workers own
disjoint socket subsets: 4 workers through 1 KiB, 1 for larger messages.
Each cell retains the median of three runs. Receiver CPU usage is recorded
separately.

## SHM comparisons (3 files)

Generate with `omq_bench chart shm`; runner commands are in
[`RUNNING_BENCHMARKS.md`](../../RUNNING_BENCHMARKS.md).
Data: `shm-{pubsub,pushpull,reqrep}.jsonl`. Series: inproc pool, IPC, SHM, SHM pool.
Require HWM 1000, 4 KiB slabs for tiny throughput, and source pools of
64 MiB below 1 MiB, 256 MiB with direct memset filling at or above 1 MiB.
SHM throughput records copy cutoffs of 256 B (point-to-point) or 1 KiB (fan-out).
Reject cached rows with different settings.

| file | workload |
|------|----------|
| `shm/pubsub.svg` | RADIO/DISH, 4/32 readers |
| `shm/pushpull.svg` | SCATTER/GATHER, one sender/receiver |
| `shm/reqrep.svg` | CLIENT/SERVER, 0/50 us receive spin |

Use complete verified cohorts; exclude ordinary inproc from the pooled baseline.
GB/s and msg/s axes are linear. Latency plots p99 with p50-p99.9
whiskers, non-spinning above spinning, on 0-120 us axes with 20 us ticks.
Use the usual subtitle without hostnames or CPU counts.

## Fan-out / fan-in charts (2 files)

Data: `comparisons.jsonl`, kind `fan_out`/`fan_in`.

| file | panels | impls |
|------|--------|-------|
| `pushpull/fanout/tcp.svg` | 4, 32 peers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO |
| `pushpull/fanin/tcp.svg` | 4, 32 peers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO |

No CT. 2IO omq must outperform 1IO omq. Do not publish if it does not.

**Fairness whiskers+baskets.** Fan-out and fan-in charts show per-peer
fairness as box-and-whisker overlays at each data point. The whiskers
show the projected aggregate range based on per-peer throughput spread:
`projected = aggregate * (peer_quantile / peer_median)`. Whiskers span
min to max, boxes span p25 to p75. Only impls with `peer_min`..`peer_max`
data in the JSONL get whiskers (currently omq impls only). Data fields:
`peer_min`, `peer_p25`, `peer_median`, `peer_p75`, `peer_max`.

## Compression charts (2 files)

Data: `results_pushpull_lz4.jsonl` and `results_pushpull_zstd.jsonl`,
patterns `pushpull_{lz4,zstd}` and `pushpull_{lz4,zstd}_dict`.

`pushpull/compression_tcp.svg`: full-speed PUSH/PULL over private Linux netem
links (1 Gbps, 100 Mbps, 10 Mbps shared between directions). Each row: single
panel with dual Y-axes (dashed msg/s left, solid GB/s right) across all sizes.
Series: tcp (from the LZ4 runs), lz4+tcp (blue), zstd+tcp (red). No dictionary
series: queued messages share arenas at full speed. Sizes: 16B, 32B,
64B..256KiB (9 points). Thin dotted lines show measured sender CPU% at each
datapoint, on a fixed 0–200% panel scale.
Bench: `omq_bench run pushpull-{lz4,zstd} --link-mbps RATE` (1IO). MTU 1500,
1 ms/direction, offloads off. Exclude rows with `send_rate`.

`pushpull/compression_fixed_load.svg`: one row per paced send rate (1k, 10k,
100k, 1M msg/s) over plain loopback. Left axis: wire ratio, payload bytes over
TCP payload bytes received (`received_wire_bytes`); right axis: sender CPU%.
Series: lz4+tcp, lz4+tcp + dict, zstd+tcp, zstd+tcp + dict. Hollow markers:
received rate under 95% of the send rate.
Bench: `omq_bench run pushpull-{lz4,zstd} --send-rate RATE --dict-sizes 2048`.
Exclude rows with `netem`.

Both: payload `structured-varied-v1`, seed 4242. Cycle 4096 distinct messages,
reduced to fit 64 MiB at large sizes: medium-entropy binary at 16/32 B, JSON
with recurring keys and varying values at >=64 B. Dict: 2 KiB, trained on
separate seed-1 samples. Require recorded corpus size and arena cap (16 KiB
LZ4, 32 KiB Zstd); exclude repeated-payload cohorts. Zstd uses the adaptive
level (-7..4); exclude fixed-level rows. Warmup 0.5 s (1 s Zstd), at least 2 s
receive interval starting and ending on message arrivals, three retained
repeats. Charts require complete measured cohorts. Use `OMQ_BENCH_TASKSET=1`
for sender CPUs 1-2, receiver 3-4.

Historical caveat: before `d6f07c40a` (2026-07-31), the blocking sender ignored
the JSON and dictionary settings. Its throughput used repeated `x` bytes,
while wire-size probes used JSON. Do not compare those old throughput rows
with current JSON/dictionary runs.

## Reliable DART charts

`dart/radiodish.svg` reads `dart-radiodish.jsonl`: DART-LAN, DART-adaptive,
and TCP with 4/32 subscribers, 16 B through 16 KiB, HWM 1000, window 256,
yield-poll receivers, and no IO spin. Require one complete campaign, one binary
digest, and verified delivery to every reader; exclude profiles. Plot per-subscriber
throughput, median and range of three rounds, using the usual dual panels.
Use the SHM harness's sender pools/slabs and `--recv-pool` on all transports:
1024 slots per DISH, `max(body size, 64)` bytes per slot, batched buffer returns.
Exclude unpooled diagnostics from the chart cohort; label the pool policy.

Data: `dart.jsonl`. Outputs: `dart/{scattergather,clientserver}.svg`.
Select DART protocol version 1, LAN/adaptive as separate series, with TCP from the
matched runner. Exclude profiled rows, other spin budgets, verification
failures, diagnostic sample captures, borrowed-runtime experiments, and
mismatched workload profiles. DART requires the default 256-message
receive/retention window; other windows are recorded experiments.
Throughput requires 3-second windows and a 2-second delivery/ACK drain; RTT
requires 100,000 measured and 200,000 warmup exchanges, with size order
rotated between repeats so the first size does not always absorb startup.
Use the median of the latest three eligible runs from the same binary SHA-256.
Latency whiskers use p50/p99.9 from the selected median-p99 run. The runner
prints all individual values and each range. Migrated rows retain the predecessor
version in `experimental_wire_version`. Original versions 1-5 are incompatible
experiments and have no DART wire version. Versions 6-8 are mapped to DART
version 1 for chart selection; their original binaries and measurements remain
unchanged. Original version 6 supports the current small-message format;
large sizes require original version 7 or newer. Each size still requires
its own complete three-run cohort.
RTT sizes are 16 B, 32 B, 64 B, 128 B, 256 B, 512 B, 1 KiB, 2 KiB, 4 KiB,
8 KiB, and 16 KiB. The RTT Y axis is linear from 1 to 100 μs, with ticks at
1 μs and multiples of 10 μs. Whiskers above 100 μs
end at the axis limit and show a triangle and their measured p99.9 value.
When p99 itself exceeds the limit, clip its plotted position and label all
three measured percentiles. Labels face inward near the boundary and use
separate rows when they overlap.
Labels are `OMQ / TCP` (red), `OMQ / DART-LAN` and
`OMQ / DART-adaptive` (different orange shades), and `Aeron v1.53.3 / UDP`.
Thread labels are `1 IO`, with `1 IO (SHARED)` for Aeron's Media Driver.
The RTT legend shows 50 μs application receive spin for OMQ; only DART
also uses 50 μs IO spin.
Throughput adds 32 KiB, 64 KiB, 256 KiB, 1 MiB, 4 MiB, and 8 MiB. Use the
main TCP chart layout: messages/s on the left through 1 KiB, GB/s on the
right from 256 B. The panels overlap at 256 B, 512 B, and 1 KiB.

The Aeron series reads `dart-aeron.jsonl`: Aeron 1.53.3, SHARED driver,
exclusive publication, default static-window congestion control,
verified delivery, and explicitly pinned IO/app cores matching OMQ. Require
three independent JVM pairs with the same class/JAR digest and placement.
Throughput measures 3 seconds after 3 seconds of JVM warmup, with a 2-second
verified drain. RTT measures 100,000 exchanges after 200,000 JVM warmup
exchanges. Select the median throughput or median-p99 run; latency whiskers
stay with that run. JVM warmup is excluded from the measurements.

## Lossy-link chart

`cargo run --release -p omq-bench -- chart lossy` reads `lossy.jsonl` and
generates `dart/lossy.svg`: DART LAN/adaptive and QUIC streams, 1 IO,
SCATTER/GATHER throughput at 1 KiB and 16 KiB, CLIENT/SERVER p99 RTT at 1 KiB.
Use 0%, 0.1%, 1%, and 5% independent random loss, 100 Mbps shared between
directions, 1 ms delay each way, MTU 1500, UDP ingress netem on IFB, and
segmentation offloads disabled. ECN marking runs are separate conditions.
Cache rows retain raw runner fields plus `socket_pair` and `netem` settings.
Require three verified runs with matching binary digest and CPU placement.
Dots show medians; whiskers show min-max across repeats. Throughput measures
3 seconds after 200 ms warmup; delivery/ACK drains are 2 seconds, or 10 seconds
at 5% loss, excluded from rates. RTT uses 100 warmup and 1000 measured exchanges.

## OMQ runtime modes

- `omq-tokio-1t`: blocking API, 1 dedicated background IO thread.
- `omq-tokio-ct`: `Context::current()`, no background IO thread.
  App and IO share one current-thread runtime.
- `omq-tokio-2t`: 2 dedicated current-thread IO runtimes.

Legend thread labels:
- `IO`: OMQ-owned background IO thread.
- `CT`: current-thread runtime; app and IO share the caller runtime.
- `UT`: user thread. Used for inproc peer threads, not OMQ-owned IO threads.

## Style

Dual-panel throughput: msg/s left (dashed, sizes <= 1KiB), GB/s right
(solid, sizes >= 256B). Legend table below with impl/threads/CPU%.
Line width 2, dot radius 2.5 (post-processed). Grid: light gray major
lines, dark gray panel outlines.
