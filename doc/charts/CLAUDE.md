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
```

A chart refresh without new benchmarks just re-renders existing data.
Benchmark processes must not run in parallel.

## Main charts (3 files)

Data: `comparisons.jsonl`. External impls required.

| file | impls |
|------|-------|
| `main_pushpull_tcp.svg` | libzmq 1IO, omq 1IO, omq CT, monocoque CT, zmq.rs, rzmq, rzmq-iouring, gRPC Rust |
| `main_reqrep_tcp.svg` | libzmq 1IO, omq 1IO, omq CT, monocoque CT, zmq.rs, rzmq, rzmq-iouring, gRPC Rust |
| `main_pubsub_tcp.svg` | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO, monocoque CT + 1 worker, zmq.rs, rzmq, rzmq-iouring |

PUSH/PULL sizes: 16B..4MiB (14 points). PUB/SUB sizes: 16B..16KiB
(6 points, 64 peers). REQ/REP latency sizes: 16B..16KiB (6 points).

## Secondary comparison charts (6 files)

Data: `comparisons.jsonl`. OMQ vs libzmq only.

**Throughput** (`pushpull/{tcp,ipc,inproc}.svg`):
- TCP/IPC: libzmq 1IO, omq 1IO
- Inproc: libzmq 2 UT, omq CT, omq 2 UT. GB/s panel uses log scale.

**Latency** (`reqrep/{tcp,ipc,inproc}.svg`):
- TCP/IPC: libzmq 1IO, omq 1IO, omq CT
- Inproc: libzmq 2 UT, omq 2 UT, omq CT

All latency charts include CT. Sizes: 16B, 64B, 256B, 1KiB, 4KiB, 16KiB.

## PUB/SUB charts (2 files)

Data: `comparisons.jsonl`, kind `pub_sub`.

| file | panels | impls |
|------|--------|-------|
| `pubsub/tcp.svg` | 4, 32 subscribers | libzmq 1IO, libzmq 2IO, omq 1IO, omq 2IO |
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

`pushpull/lz4_tcp.svg`: PUSH/PULL single-peer, 3-row link-speed
projection (1 Gbps, 100 Mbps, 10 Mbps). Each row: single panel with
dual Y-axes (dashed msg/s left, solid GB/s right) across all sizes.
Series: tcp, lz4+tcp, lz4+tcp+dict. Sizes: 16B..256KiB (8 points).
Thin dotted lines show compression CPU% at each datapoint, on a fixed 0–200%
panel scale.
Payload: structural JSON (`OMQ_BENCH_PAYLOAD=json`). Dict: 2 KiB,
trained on diverse seeded samples (`json_payload_seeded`, seeds 1..N).
Bench: `omq-bench run pushpull-lz4` (uses `bench_peer_blocking`, 1IO).

## OMQ runtime modes

- `omq-tokio-1t`: blocking API, 1 dedicated background IO thread.
- `omq-tokio-ct`: `Context::current()`, no background IO thread.
  App and IO share one current-thread runtime.
- `omq-tokio-2t`: 2 dedicated current-thread IO runtimes.

## Monocoque runtime mode

- `monocoque-tokio-ct`: `monocoque-rs` 0.4.0 with `runtime-tokio`.
  `LocalRuntime` is a Tokio current-thread runtime wrapped in a `LocalSet`.
- PUSH/PULL and REQ/REP use one OS thread per peer process. Application and
  socket I/O share that thread. No background I/O thread.
- PUB uses the same current-thread runtime plus exactly one `pub-worker-0`
  background worker. SUB remains current-thread with no worker.
- Throughput PUSH uses the 64 KiB read slab, 64 KiB write buffer, and write
  coalescing. Throughput PULL and SUB use the 64 KiB read slab, 8 KiB write
  buffer, no coalescing, and `recv_into()` to reuse the receive frame vector.
  REQ/REP leaves Monocoque's default buffers in place, disables coalescing, and
  reuses the reply vector with `recv_into()` on the request side.

## Style

Dual-panel throughput: msg/s left (dashed, sizes <= 1KiB), GB/s right
(solid, sizes >= 256B). Legend table below with impl/threads/CPU%.
Line width 2, dot radius 2.5 (post-processed). Grid: light gray major
lines, dark gray panel outlines.
