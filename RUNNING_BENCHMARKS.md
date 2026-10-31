# Running Benchmarks

Benchmark and chart commands for omq.rs. Build, test, and CI notes live in
[DEVELOPMENT.md](DEVELOPMENT.md).

## Rules

- Run one benchmark at a time on a quiet system. Stop on any warning or
  timeout, fix the cause, and rerun before charting.
- Results append to `~/.cache/omq/*.jsonl` (`$XDG_CACHE_HOME/omq/`). Never
  edit them. `OMQ_BENCH_NO_WRITE=1` skips writing.
- Throughput loops use bounded batches with no per-message clocks or shared
  counters. Exclude warmup and boundary-crossing batches; use actual elapsed
  time. Time individual operations only for latency.
- Compare revisions with identical harness, flags, workload, warmup,
  duration, and CPU placement. Use `--measure-only` to diagnose; never lower
  a threshold to pass.
- Run throughput with `OMQ_BENCH_TASKSET=1`, which pins the measured peer to
  CPUs 1-2 and the other to CPUs 3-4. Run latency unpinned.
- Chart subtitles come from `.chart_hw` in the repo root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 cores, performance governor, turbo off
```

## Cargo benchmarks

```sh
cargo bench -p omq-tokio --bench omq_push_pull
cargo bench -p omq-tokio --bench omq_inproc_threads
cargo bench -p omq-tokio --bench omq_pull_bulk_fanin
```

Knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`, `OMQ_BENCH_PEERS`,
`OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`, `OMQ_BENCH_ARENA_THRESHOLD`.
`OMQ_LZ4_ARENA_BYTES` / `OMQ_ZSTD_ARENA_BYTES` set the compression arena
(defaults 16384 / 32768, max 65536, 0 = per-part encoding).

## Comparison benchmarks

`omq_bench run comparisons` drives standalone peers and writes
`comparisons.jsonl`. Every benchmark executable starts with `omq_` so it is
easy to spot in `top`.

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

External peers are standalone Cargo workspaces and never enter the OMQ build.
Peer subcommands: `push`/`pull`, `pub`/`sub`, `inproc`, and `rep`/`req`.

Useful variants:

- `omq-tokio-1t-spin50`: latency only, 50 us receive spin on both ends
  (`OMQ_BENCH_RECV_SPIN_US` for direct peer runs).
- `omq-tokio-2ut-blocking`: inproc with two blocking application threads;
  `OMQ_BENCH_HWM` sets both HWMs (default 1000).
- `--latency-pairs` and `--latency-profiles` cover more socket pairs on OMQ
  peers. Only default REQ/REP rows feed the REQ/REP charts.
- `--pubsub-receiver-io-threads`, `--pubsub-receiver-workers`, and
  `--pubsub-receiver-cpus` give the PUB/SUB receiver a fixed larger budget.

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-1t --impl omq-tokio-1t-spin50 --impl omq-tokio-ct \
  --no-throughput --no-pubsub --transport tcp --transport inproc \
  --latency-pairs req-rep,router-dealer,router-router,pair,client-server,peer,channel \
  --latency-profiles default,latency --sizes 16,64,256,1024,4096
OMQ_BENCH_HWM=8 cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-2ut-blocking --transport inproc --sizes 64 \
  --no-latency --no-pubsub --id blocking-hwm8
```

Latency panels plot p99 with p50-p99.9 whiskers from the same run. Charts
show only the interesting process's CPU:

| benchmark | charted process | JSONL field |
|-----------|----------------|-------------|
| PUSH/PULL throughput | sender (PUSH) | `push_cpu_time` |
| PUB/SUB | sender (PUB) | `pub_cpu_time` |
| fan-out (1 PUSH to N PULL) | sender (PUSH) | `push_cpu_time` |
| fan-in (N PUSH to 1 PULL) | receiver (PULL) | `pull_cpu_time` |
| REQ/REP latency | sender (REQ) | `req_cpu_time` |

## Updating charts

Remoc comparison (requires Rust 1.95):

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl remoc --impl omq-tokio-ct --transport tcp --no-pubsub \
  --sizes 64,1024,4096 --duration 3 --rounds 3 --id remoc-YYYYMMDD
cargo run --release -p omq-bench -- chart remoc
```

The `grpc-rust` baseline uses plaintext gRPC over TCP between separate
processes. PUSH/PULL uses one server-streaming RPC carrying opaque byte
blobs. REQ/REP uses unary `Echo` RPCs. TLS, compression, retries, and
application-level QoS are disabled.

### Main TCP charts

`doc/charts/main_{pushpull,pubsub,reqrep}_tcp.svg`. Rebench OMQ only, or all
impls:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-ct --impl omq-tokio-1t --transport tcp --no-pubsub
cargo run --release -p omq-bench -- run comparisons --transport tcp
cargo run --release -p omq-bench -- chart main
```

### Cross-library comparison charts

`doc/charts/{pushpull,pubsub,reqrep}/*.svg` and
`doc/charts/pushpull/fan{out,in}/tcp.svg`. Full refresh:

```sh
test -f .chart_hw
cargo run --release -p omq-bench -- run comparisons \
  --transport tcp --transport ipc --transport inproc \
  --fanout --fanin --pubsub-peers 4,32
cargo run --release -p omq-bench -- run comparisons --omq \
  --transport tcp --no-latency --no-pubsub \
  --sizes 32,128,512,2048,8192,32768 --allow-non-chart-sizes
cargo run --release -p omq-bench -- chart comparison
cargo run --release -p omq-bench -- chart main
cargo run --release -p omq-bench -- chart fanio
```

IPC PUB/SUB with 4 and 32 subscribers (repeat with
`--sizes 4096,16384 --pubsub-receiver-workers 1` for large messages):

```sh
OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run comparisons \
  --impl libzmq --impl libzmq-2t --impl omq-tokio-1t --impl omq-tokio-2t \
  --transport ipc --no-throughput --no-latency --pubsub-peers 4,32 --rounds 3 \
  --sizes 16,64,256,1024 \
  --pubsub-receiver-io-threads 3 --pubsub-receiver-workers 4 \
  --pubsub-receiver-cpus 0,3-5
cargo run --release -p omq-bench -- chart pubsub
```

IPC REQ/REP with the 50 us spin series:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl libzmq --impl omq-tokio-1t --impl omq-tokio-1t-spin50 --impl omq-tokio-ct \
  --transport ipc --no-throughput --no-pubsub --sizes 16,64,256,1024,4096 \
  --latency-iterations 100000 --latency-warmup 20000 --rounds 3
cargo run --release -p omq-bench -- chart comparison
```

### CURVE PUB/SUB chart

`doc/charts/pubsub/curve_tcp.svg`. `--curve` adds the CURVE impls of the
selected families:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --omq --curve --transport tcp --no-throughput --no-latency
cargo run --release -p omq-bench -- chart pubsub
```

### Shared-memory charts

`doc/charts/shm/{pubsub,pushpull,reqrep}.svg` from `shm-*.jsonl`. All series
use HWM 1000. For SHM-only refreshes, run with `--impls shm,shm-pool` and
chart with `--cached-baselines`.

```sh
export CARGO_TARGET_DIR=/mnt/bench/tmp/cargo-target
cargo build --release --manifest-path scripts/shm_bench_peer/Cargo.toml
ruby scripts/run_shm_pubsub.rb
ruby scripts/run_shm_pubsub.rb --pattern scatter-gather
ruby scripts/run_shm_pubsub.rb --pattern client-server
cargo run --release -p omq-bench -- chart shm
```

### TCP byte-limit chart

`doc/charts/pushpull/tcp_rate_limit.svg`: uncapped versus a 100 MB/s egress
cap over a 1 Gbit/s netem link. `OMQ_BENCH_EGRESS_BYTE_LIMIT=RATE,BURST` sets
the cap on direct peer runs.

```sh
OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-rate-limit
cargo run --release -p omq-bench -- chart rate-limit
```

### QUIC charts

`doc/charts/quic/{pushpull,reqrep,pubsub,fanout,fanin}.svg`: QUIC against
CURVE over TCP. Linux needs `net.core.rmem_max` and `net.core.wmem_max` of at
least 4 MiB, or multi-peer QUIC runs drop packets on loopback.

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

### DART charts

RADIO/DISH (`doc/charts/dart/radiodish.svg`, `dart-radiodish.jsonl`) with 4
and 32 subscribers and receive pools. Set `CARGO_TARGET_DIR` to the peer's
target directory:

```sh
cargo build --release --manifest-path scripts/shm_bench_peer/Cargo.toml --features dart
ruby scripts/run_shm_pubsub.rb --dart --recv-pool
cargo run --release -p omq-bench -- chart dart
```

SCATTER/GATHER throughput and CLIENT/SERVER RTT
(`doc/charts/dart/{scattergather,clientserver}.svg`, `dart.jsonl`):

```sh
cargo build --release -p omq-tokio --features dart --bin omq_dart_bench_peer
cargo run --release -p omq-bench -- run dart --binary path/to/omq_dart_bench_peer \
  --cpus 0,1,2,3,4,5 --spin 50 --io-spin 50 --congestion lan --repeats 3
cargo run --release -p omq-bench -- chart dart
```

- The runner needs six distinct cores and stops on warnings, IO failures,
  timeouts, or missing messages after the drain.
- Charts need three runs from one binary with the defaults: 200 ms warmup,
  3 s windows, 2 s drain, window 256, and the default size rotation
  (`--order fixed` is excluded). Run `--congestion adaptive` separately.
- Save every nonstandard run (`--continuous-spin`, `--window-messages`,
  `--latency-samples`, `--profile`, simulated links) with `--output`.
- Build the peer with `--features 'dart quic'` and pass `--transport quic` to
  compare QUIC streams.
- `--runtime current` and `--runtime current-poll` with zero spin budgets
  measure DART on `Context::current()` without an owned IO thread.

Lossy links (`doc/charts/dart/lossy.svg` from `lossy.jsonl`): 0-5% random
loss on a 100 Mbit/s netem link with 1 ms delay.

```sh
cargo run --release -p omq-bench -- chart lossy
```

Aeron baseline (1.53.3, embedded SHARED driver, `dart-aeron.jsonl`):

```sh
curl -fLsS https://repo.maven.apache.org/maven2/io/aeron/aeron-all/1.53.3/aeron-all-1.53.3.jar \
  -o /tmp/aeron-all-1.53.3.jar
javac -cp /tmp/aeron-all-1.53.3.jar -d /tmp/aeron-dart-classes \
  scripts/aeron_dart_peer/AeronUdpPeer.java
TMPDIR=/tmp cargo run --release -p omq-bench -- run aeron-dart \
  --jar /tmp/aeron-all-1.53.3.jar --classes /tmp/aeron-dart-classes
```

Profile one side at a time; profiled rows are excluded from charts:

```sh
cargo run --release -p omq-bench -- run dart --binary path/to/omq_dart_bench_peer \
  --transport dart --kind throughput --sizes 16 \
  --profile /mnt/bench/tmp/dart-profile --profile-side receive
perf report --stdio --no-inline -g none \
  -i /mnt/bench/tmp/dart-profile/gather-16.data
```

### MOM Charts

- `omq_bench chart mom` writes six network/local throughput, RTT, and PUB/SUB
  SVGs in `doc/charts/moms/`.
- RTT uses request/reply workloads and complete rival runs. Network axes are
  logarithmic; local axes are linear with 20 μs top and 5 μs bottom ticks.
  Network panels separate no-spin and spin results; all network lines are
  solid. The bottom panel includes Aeron busy polling. DART also spins IO.
- Local RTT separates no-spin and spin results into two panels. Iceoryx2
  polling is in the bottom panel; all lines are solid.
- PUB/SUB rates are per subscriber. Left msg/s lines are dashed; right GB/s
  lines are solid.

Build `omq_dart_bench_peer` with `dart quic`, then collect both OMQ network
latency cohorts. Local IPC/SHM RTT comes from the SHM REQ/REP sweep above.
The six CPU IDs place sender caller/IO on 1/2 and receiver IO/caller on 3/4.

```sh
for spin in 0 50; do
  omq_bench run dart --binary path/to/omq_dart_bench_peer \
    --transport tcp,quic,dart --congestion adaptive --kind latency \
    --spin "$spin" --io-spin "$spin" --cpus 1,2,0,3,5,4 \
    --sizes 16,32,64,256,1024,4096 --iterations 10000 --warmup 2000 --repeats 3 \
    --output "$HOME/.cache/omq/mom-omq.jsonl"
done
omq_bench run dart --binary path/to/omq_dart_bench_peer \
  --transport dart --congestion adaptive --kind throughput --window-messages 512 \
  --spin 50 --io-spin 50 --cpus 1,2,0,3,5,4 --warmup-seconds 1 --repeats 3 \
  --sizes 16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,262144,4194304,8388608 \
  --output "$HOME/.cache/omq/mom-omq.jsonl"
omq_bench chart mom
```

The optional `omq_mom_bench` executable provides broker adapters. Run it with
`cargo run --release -p omq-bench --features mom-bench --bin omq_mom_bench -- --help`.
Rival adapters: [scripts/rivals](scripts/rivals/README.md).

Brokers run one at a time in Podman with host networking, pinned to CPUs 3-4
through `taskset`; rootless Podman has no cpuset controller.
`OMQ_MOM_BENCH_PIN=1` places brokered clients on CPUs 1 and 2, and the gRPC
client and server on CPUs 1-2 and 3-4. NATS needs `max_payload: 8MB` for the
largest sizes.

```sh
printf 'listen: 127.0.0.1:4222\nmax_payload: 8MB\n' > /mnt/bench/tmp/nats.conf
taskset -c 3-4 podman run -d --name omq-bench-nats --network host \
  -v /mnt/bench/tmp/nats.conf:/etc/nats/nats.conf:ro \
  docker.io/library/nats:2-alpine -c /etc/nats/nats.conf
taskset -c 3-4 podman run -d --name omq-bench-redis --network host \
  docker.io/library/redis:8-alpine redis-server --bind 127.0.0.1 --save '' --appendonly no
taskset -c 3-4 podman run -d --name omq-bench-rabbit --network host \
  docker.io/library/rabbitmq:4-alpine
OMQ_MOM_BENCH_PIN=1 omq_mom_bench --impl nats --mode throughput --run-id nats-throughput
OMQ_MOM_BENCH_PIN=1 omq_mom_bench --impl nats --mode latency \
  --latency-iterations 10000 --latency-warmup 2000 --run-id nats-latency-r1
```

Broker and rival RTT points come from three latency runs; each size keeps the
run with the middle p99. Derived rows record `source_run_id` and
`cohort_run_ids`.

- `scripts/rivals/run_fanout.rb` writes `~/.cache/omq/mom-fanout.jsonl`.
- Each of three rounds (at least 3 seconds of publishing) verifies delivery to all 1/4/32
  subscribers. The score includes the slowest subscriber's drain.
- OMQ uses HWM 1000 and blocking receive. SHM uses RADIO/DISH; the pooled
  series fills producer slots without allocation fallback.
- Zenoh uses one session per subscriber and blocking FIFO receive over TCP or
  Unix sockets. RabbitMQ uses a fanout exchange with one queue per subscriber;
  start the RabbitMQ container above without `taskset` first.
- Publisher CPUs: 1-2. Subscriber CPUs: 3-4. Optional sizes: 128 KiB, 1 MiB,
  8 MiB. Incomplete cohorts fail chart generation.

Refresh verified fan-out:

```sh
ruby scripts/rivals/run_fanout.rb --impls omq-tcp,omq-quic,omq-dart,omq-ipc,omq-shm,omq-shm-pool
ruby scripts/rivals/run_fanout.rb --impls zenoh-tcp,rabbitmq-tcp,zenoh-unix
omq_bench chart mom-fanout
```

### Quinn DATAGRAM baseline

Raw Quinn DATAGRAM between two processes, without OMQ, appended to
`quinn-datagram.jsonl`. `--layout split|multi` and `--io-spin 50` vary thread
placement and UDP polling.

```sh
cargo build --release -p omq-tokio --features quic --example omq_quinn_datagram_peer
cargo run --release -p omq-bench -- run quinn-datagram --binary path/to/omq_quinn_datagram_peer
cargo run --release -p omq-bench -- run quinn-datagram --binary path/to/omq_quinn_datagram_peer \
  --repeats 1 --kind throughput --sizes 1024 \
  --profile /mnt/bench/tmp/quinn-datagram-profile --profile-side send
```

### Compression charts

Saturation runs use private netem namespaces (`unshare`, `ip`, `tc`,
`ethtool`, unprivileged user namespaces). Fixed-load runs pace the sender over
loopback. Both use the `structured-varied-v1` corpus; Zstd adapts its level
within `--level-range` (fixed `--level` rows are excluded).

```sh
for rate in 1000 100 10; do
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-lz4 --link-mbps "$rate"
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-zstd --link-mbps "$rate"
done
for rate in 1000 10000 100000 1000000; do
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-lz4 --send-rate "$rate" --transports lz4+tcp --dict-sizes 2048
  OMQ_BENCH_TASKSET=1 cargo run --release -p omq-bench -- run pushpull-zstd --send-rate "$rate" --dict-sizes 2048
done
cargo run --release -p omq-bench -- chart compression
```

### Binding charts

```sh
(cd bindings/pyomq && maturin develop --release && \
  python scripts/update_perf.py --impl pyomq && \
  python scripts/update_perf.py --chart-only)
(cd bindings/pyomq && python scripts/update_perf.py --quick --impl pyomq)  # smoke, no writes
(cd bindings/ruby && ruby -Ilib scripts/update_perf.rb)
```
