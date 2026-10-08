# Chart generation rules

Generated SVGs. Do not hand-edit. Data source: `~/.cache/omq/*.jsonl`.

## Regeneration

```sh
cargo run --release -p omq-bench -- chart          # all charts
cargo run --release -p omq-bench -- chart main      # main overview only
cargo run --release -p omq-bench -- chart comparison # per-transport
cargo run --release -p omq-bench -- chart pubsub    # PUB/SUB + CURVE
cargo run --release -p omq-bench -- chart fanio     # fan-out/fan-in
cargo run --release -p omq-bench -- chart lz4       # LZ4 compression
cargo run --release -p omq-bench -- chart zstd      # Zstd compression
```

A chart refresh without new benchmarks just re-renders existing data.
Benchmark processes must not run in parallel.

## Main charts (3 files)

Data: `comparisons.jsonl`. External impls required.

| file | impls |
|------|-------|
| `main_pushpull_tcp.svg` | libzmq 1IO, omq 1IO, omq CT, omq MT, tmq, r0z-async, monocoque CT, zmq.rs, rzmq-iouring |
| `main_reqrep_tcp.svg` | libzmq 1IO, omq 1IO, omq 1IO with 50 μs receive spin, omq CT, tmq, r0z-async, monocoque CT, zmq.rs, rzmq-iouring |
| `main_pubsub_tcp.svg` | libzmq 1IO, libzmq 2IO, omq 1IO/2IO/3IO, tmq, r0z-async, monocoque CT + 1 worker, zmq.rs, rzmq-iouring |

PUSH/PULL sizes: 16B..8MiB (15 points). PUB/SUB sizes: 16B..16KiB
(6 points, 32 peers). REQ/REP latency sizes: 16B, 64B, 256B, 1KiB, 4KiB.

MT uses Tokio's multithread application runtime with one worker per available
CPU. The legend shows workers per process (6 on the current chart VM).

The main REQ/REP latency chart uses a 600 px panel with a Y axis from
0 to 250 μs in 25 μs steps.

The `omq-tokio-1t-spin50` latency series sets `recv_spin` to 50 μs on both
endpoints, with one owned IO thread per process. Main and comparison REQ/REP
charts exclude other socket pairs and explicit non-default profile runs.
Older cache rows without pair/profile metadata remain valid REQ/REP defaults.

## Secondary comparison charts (6 files)

Data: `comparisons.jsonl`. OMQ vs libzmq only.

**Throughput** (`pushpull/{tcp,ipc,inproc}.svg`):
- TCP/IPC: libzmq 1IO, omq 1IO
- Inproc: libzmq 2 UT, omq CT, omq 2 UT. GB/s panel uses log scale.

**Latency** (`reqrep/{tcp,ipc,inproc}.svg`):
- TCP: libzmq 1IO, omq 1IO, omq 1IO with 50 μs receive spin, omq CT, omq EXCL
- IPC: libzmq 1IO, omq 1IO, omq CT
- Inproc: libzmq 2 UT, omq 2 UT, omq CT

Main and comparison latency charts plot p99 round-trip latency with whiskers
from p50 to p99.9. Each point and its whiskers come from the same cached run.
Comparison Y axes include the full whisker range. All latency charts include CT.
Sizes: 16B, 64B, 256B, 1KiB, 4KiB.

## PUB/SUB charts (2 files)

Data: `comparisons.jsonl`, kind `pub_sub`.

| file | panels | impls |
|------|--------|-------|
| `pubsub/tcp.svg` | 4, 32 subscribers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO, monocoque CT + 1 worker |
| `pubsub/curve_tcp.svg` | 16 peers | libzmq-curve 1IO/2IO, omq-curve 1IO/2IO |

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

## LZ4 chart (1 file)

Data: `results_pushpull_lz4.jsonl`, patterns `pushpull_lz4` and
`pushpull_lz4_dict`.

`pushpull/lz4_tcp.svg`: measured PUSH/PULL over private Linux netem links
(1 Gbps, 100 Mbps, 10 Mbps shared between directions). Each row: single panel with
dual Y-axes (dashed msg/s left, solid GB/s right) across all sizes.
Series: tcp, lz4+tcp, lz4+tcp+dict. Sizes: 16B..256KiB (8 points).
Thin dotted lines show measured sender CPU% at each datapoint, on a fixed 0–200%
panel scale.
Payload: structural JSON (`OMQ_BENCH_PAYLOAD=json`, seed 4242). Dict: 2 KiB,
trained on diverse seeded samples (`json_payload_seeded`, seeds 1..N).
Bench: `omq_bench run pushpull-lz4 --link-mbps RATE` (1IO). MTU 1500,
1 ms/direction, offloads off; 0.5 s active warmup, at least 2 s receive interval,
three retained repeats. Charts require complete measured cohorts and exclude
old projections. Use `OMQ_BENCH_TASKSET=1` for sender CPUs 1-2, receiver 3-4.

Historical caveat: before `d6f07c40a` (2026-07-31), the blocking sender ignored
the JSON and dictionary settings. Its throughput used repeated `x` bytes,
while wire-size probes used JSON. Do not compare those old throughput rows
with current JSON/dictionary runs.

## Zstd chart (1 file)

Data: `results_pushpull_zstd.jsonl`, patterns `pushpull_zstd` and
`pushpull_zstd_dict`.

`pushpull/zstd_tcp.svg` uses the same sizes, measured links, structural JSON,
and 2 KiB dictionary setup as the LZ4 chart, at Zstd level 1.
Bench: `omq_bench run pushpull-zstd --level 1 --link-mbps RATE`.

## Reliable Dart charts

Data: `dart.jsonl`. Outputs: `dart/{scattergather,clientserver}.svg`.
Select Dart protocol version 1, LAN/adaptive as separate series, with TCP from the
matched runner. Exclude profiled rows, other spin budgets, verification
failures, diagnostic sample captures, borrowed-runtime experiments, and
mismatched workload profiles. Dart requires the default 256-message
receive/retention window; other windows are recorded experiments.
Throughput requires 3-second windows; RTT
requires 100,000 measured and 200,000 warmup exchanges, with size order
rotated between repeats so the first size does not always absorb startup.
Use the median of the latest three eligible runs from the same binary SHA-256.
Latency whiskers use p50/p99.9 from the selected median-p99 run. The runner
prints all individual values and each range. Migrated rows retain the predecessor
version in `experimental_wire_version`. Original versions 1-5 are incompatible
experiments and have no Dart wire version. Versions 6-8 are mapped to Dart
version 1 for chart selection; their original binaries and measurements remain
unchanged. Original version 6 supports the current small-message format;
large sizes require original version 7 or newer. Each size still requires
its own complete three-run cohort.
Use sizes 16 B, 64 B, 256 B, 512 B, 1 KiB, 4 KiB, and 16 KiB. The RTT Y axis is linear from
1 to 100 us, with ticks at 1 us and multiples of 10 us. Whiskers above 100 us
end at the axis limit and show a triangle and their measured p99.9 value.
When p99 itself exceeds the limit, clip its plotted position and label all
three measured percentiles. Boundary labels face inward so they stay visible.
Labels are `OMQ / TCP` (red), `OMQ / Dart-LAN` and
`OMQ / Dart-adaptive` (different orange shades), and `Aeron v1.53.3 / UDP`.
Every series uses the thread label `1 IO`.
Throughput uses one panel across all seven sizes, with two lines per
implementation: dashed messages/s on the left axis and solid GB/s on the
right axis.

The Aeron series reads `dart-aeron.jsonl`: Aeron 1.53.3, shared driver,
verified delivery, and explicitly pinned IO/app cores matching OMQ. Require
three independent JVM pairs with the same class/JAR digest and placement.
Throughput measures 3 seconds after 3 seconds of JVM warmup, with a 2-second
verified drain. RTT measures 100,000 exchanges after 200,000 JVM warmup
exchanges. Select the median throughput or median-p99 run; latency whiskers
stay with that run. JVM warmup is excluded from the measurements.

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
