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

`omq-bench run comparisons` drives standalone `bench_peer` binaries:

| binary | source | impls |
|--------|--------|-------|
| `omq_bench_peer_tokio` | `omq-tokio/src/bin/bench_peer_tokio.rs` | omq-tokio-ct |
| `omq_bench_peer_blocking` | `omq-tokio/src/bin/bench_peer_blocking.rs` | omq-tokio-1t, omq-tokio-2t |
| `libzmq_bench_peer` | `scripts/libzmq_bench_peer.c` | libzmq, libzmq-2t |
| `r0z_bench_peer` | `scripts/r0z_bench_peer/` | r0z-async |
| `monocoque_bench_peer` | `scripts/monocoque_bench_peer/` | monocoque-tokio-ct |
| `zmqrs_bench_peer` | `scripts/zmqrs_bench_peer/` | zmq.rs |
| `rzmq_bench_peer` | `scripts/rzmq_bench_peer/` | rzmq, rzmq-iouring |
| `grpc_bench_peer` | `omq-bench/src/bin/grpc_bench_peer.rs` | grpc-rust |

The `r0z-async` baseline uses a current-thread Tokio runtime and one libzmq
IO thread. It replaces the tmq baseline and uses r0z for native bindings.

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

## Updating Charts

Main Rust/comparison latency panels plot p99 round-trip latency, with whiskers
from p50 to p99.9. Each point uses all three percentiles from the same measured
run, and the Y axis includes the full whisker range.

Chart subtitles come from `.chart_hw` in the repo root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 cores, performance governor, turbo off
```

`omq-bench` and binding chart scripts read the repo-root `.chart_hw`
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

QUIC bench peers set 4 MiB UDP socket buffers through `recv_buffer_size` and
`send_buffer_size`. Linux caps them at `net.core.rmem_max` and
`net.core.wmem_max`, so check both are at least 4 MiB. With smaller buffers,
multi-peer QUIC runs drop packets on loopback (`RcvbufErrors` in
`/proc/net/snmp`) and QUIC congestion control limits throughput.

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

```sh
cargo run --release -p omq-bench -- run compression --chart
```

Or bench and chart separately:

```sh
cargo run --release -p omq-bench -- run compression
cargo run --release -p omq-bench -- chart compression
```

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
