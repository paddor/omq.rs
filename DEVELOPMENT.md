# Development

## Building And Linting

```sh
cargo build --workspace
cargo clippy --workspace --all-targets
```

Release, soak, and benchmark builds use the local CPU:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "target-cpu=native"]
```

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Unit And Integration Tests

```sh
cargo test -p omq-tokio
cargo test -p omq-proto
cargo test -p yring
cargo test -p omq-tokio --test req_rep -- some_test_name
```

Feature-gated tests:

```sh
cargo test -p omq-tokio --features plain     --test plain
cargo test -p omq-tokio --features curve     --test curve
cargo test -p omq-tokio --features lz4       --test lz4_tcp --test lz4_pub_sub
```

Miri target for unsafe internals:

```sh
cargo +nightly miri test -p yring --features async
```

Roughly two minutes for the 30 tests. Miri can also interpret for a
foreign target, which is how to reproduce a CI failure from a
different host:

```sh
cargo +nightly miri test -p yring --features async \
  --target x86_64-unknown-linux-gnu
```

Loom checks:

```sh
cargo test -p omq-tokio --test omq_loom_signal
RUSTFLAGS="--cfg loom" cargo test -p yring --features async --test loom
```

The `omq-tokio` Loom test models `StateSignal` and `DataSignal` lost-wake
races. The `yring` Loom suite is behind `cfg(loom)` and covers SPSC cursor
ordering, wraparound, producer drop, async `push_async` wakeups, and upper-layer
readiness patterns.

Full sweep:

```sh
./scripts/test-all.sh
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
OMQ_SKIP_PERF=1 ./scripts/test-all.sh
OMQ_LOOM=1 ./scripts/test-all.sh
```

The full sweep runs `omq_perf_verify` locally. With `.perf_hw` present,
it uses machine-specific hardware thresholds. Without `.perf_hw`, it
runs a smaller smoke gate for obvious TCP/inproc regressions. The perf
gate skips when `CI` or `GITHUB_ACTIONS` is set.

GitHub CI runs `cargo fmt` and clippy on Linux, macOS Intel, macOS
ARM64, and Windows. It runs workspace tests on Linux x86_64, Linux
ARM64, macOS Intel, macOS ARM64, Windows, and MSRV 1.93 on Linux.
Feature jobs cover CURVE and LZ4 on Linux, macOS Intel, macOS ARM64,
and Windows. macOS test jobs run serially with `--test-threads=1`.

## Continuous Integration

`.github/workflows/ci.yml` gates every PR. Beyond fmt/clippy/tests it
runs, on Linux only:

| job | what |
|-----|------|
| `interop` | pyzmq PLAIN + CURVE (pyzmq wheels bundle libzmq) |
| `loom` | `yring` loom suite under `--cfg loom`, release |
| `miri` | `cargo miri test -p yring --features async`, nightly |
| `fuzz-smoke` | parsers at 1M iters, socket actions at 200 |

`.github/workflows/extended.yml` runs nightly at 03:00 UTC and on
`workflow_dispatch` (with `soak_duration_secs` / `fuzz_scale` inputs):
the fuzz targets at 100M / 2000 iters, the soak suite in five groups,
the `--ignored` stress tests, libzmq `ws://` interop, and Miri under
`-Zmiri-many-seeds`.

The interop tests skip when their peer is missing so local runs stay
green without pyzmq or the `zmq_ws_peer` helper. CI sets
`OMQ_INTEROP_REQUIRED=1`, which turns a missing peer into a failure so
the job cannot pass without testing anything.

The libzmq `ws://` interop job builds libzmq from source with
`ENABLE_DRAFTS=ON` (distro packages omit it, and without drafts libzmq
has no `ws://` transport) and caches the install.

## Fuzz Tests

The hand-rolled fuzz suites are off by default. Enable with the `fuzz`
feature. Set `OMQ_FUZZ_ITERS=<n>` and `OMQ_FUZZ_SEED=<u64>` for long or
reproducible runs.

`OMQ_FUZZ_ITERS` counts a different unit per target, so one value
cannot serve both. A `fuzz_parsers` iteration is a buffer parse
(default 10M, ~7s per 1M); a `fuzz_socket_actions` iteration drives a
live socket (default 200, ~90ms each). Setting it globally silently
turns the socket suite into an hours-long run.

```sh
cargo test -p omq-tokio --features fuzz
OMQ_FUZZ_ITERS=500000000 cargo test -p omq-tokio --features fuzz --release -- --nocapture
```

## Soak Tests

Soak tests cover peer churn, reconnect storms, reconnect all types,
PUB/SUB churn, ROUTER/DEALER churn, HWM reconnect, WebSocket
throughput, WebSocket reconnect, large-message throughput, compression
with lz4, PLAIN, CURVE, multi-socket, inproc cross-thread,
and cancel safety.

Set duration with `OMQ_SOAK_DURATION_SECS` (default 600s). `Context::new()`
uses one dedicated background IO thread. `OMQ_IO_THREADS=N` selects N
dedicated IO threads; `Context::current()` is the explicit current-thread
Tokio integration mode.

```sh
FEATURES="soak lz4 plain curve ws"
cargo test -p omq-tokio --features "$FEATURES" --release --no-run
OMQ_SOAK_DURATION_SECS=600 cargo test -p omq-tokio \
  --features "$FEATURES" --release --test omq_soak_peer_churn -- --nocapture
```

### pyomq Soak Tests

```sh
cd bindings/pyomq
maturin develop --release
OMQ_SOAK_DURATION_SECS=120 python3 -m pytest tests/soak/ -v --tb=short
```

## Stress Tests

```sh
cargo test -p omq-tokio --test stress_connect_before_bind -- --test-threads=1
```

## Benchmarks

```sh
cargo bench -p omq-tokio --bench push_pull
cargo bench -p omq-tokio --bench inproc_threads
```

Env knobs: `OMQ_BENCH_TRANSPORTS`, `OMQ_BENCH_SIZES`,
`OMQ_BENCH_PEERS`, `OMQ_BENCH_ROUND_MS`, `OMQ_BENCH_ROUNDS`.
Results append to `$XDG_CACHE_HOME/omq/` (default `~/.cache/omq/`)
unless `OMQ_BENCH_NO_WRITE=1`.

Unless stated otherwise, user ensures system is NOT noisy during benchmarks. So
when a measured cell looks bad, don't hand-wave it as noise.

### Cross-implementation Comparison Benchmarks

`omq-bench run comparisons` drives standalone `bench_peer` binaries:

| binary | source | impls |
|--------|--------|-------|
| `omq_bench_peer_tokio` | `omq-tokio/src/bin/bench_peer_tokio.rs` | omq-tokio-ct |
| `omq_bench_peer_blocking` | `omq-tokio/src/bin/bench_peer_blocking.rs` | omq-tokio-1t, omq-tokio-2t |
| `libzmq_bench_peer` | `scripts/libzmq_bench_peer.c` | libzmq, libzmq-2t |
| `zmqrs_bench_peer` | `scripts/zmqrs_bench_peer/` | zmq.rs |
| `rzmq_bench_peer` | `scripts/rzmq_bench_peer/` | rzmq, rzmq-iouring |

Each binary speaks a subcommand protocol:

- `push <addr> <size>`: bind PUSH, send forever.
- `pull <addr> <size> <duration>`: connect PULL, count for duration.
- `pub <addr> <size>` / `sub <addr> <size> <duration>`: PUB/SUB throughput.
- `inproc <name> <size> <duration>`: in-process PUSH/PULL.
- `rep <addr> <size>` / `req <addr> <size> <iters> <warmup>`: latency.

Results go to `~/.cache/omq/comparisons.jsonl`. APPEND-ONLY!

## Updating Charts

Chart subtitles show hardware info auto-detected from `/proc/cpuinfo`
and sysfs. On machines where sysfs is absent, create `.chart_hw` in the
repo root:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=performance governor, turbo off
```

`omq-bench` reads `.chart_hw` automatically.

### Main TCP Charts

Refreshes `doc/charts/main_pushpull_tcp.svg` (PUSH/PULL throughput)
and `doc/charts/main_reqrep_tcp.svg` (REQ/REP latency), TCP only.
Rebench omq impls only, then regenerate:

```sh
cargo run --release -p omq-bench -- run comparisons \
  --impl omq-tokio-ct --impl omq-tokio-1t --transport tcp --no-pubsub
cargo run --release -p omq-bench -- chart main
```

Rebench all impls (when external baselines are stale):

```sh
cargo run --release -p omq-bench -- run comparisons --transport tcp --no-pubsub
cargo run --release -p omq-bench -- chart main
```

### Cross-library Comparison Charts

Produces `doc/charts/{pushpull,pubsub,reqrep}/*.svg`,
`doc/charts/pushpull/fan{out,in}/tcp.svg`,
`doc/charts/main_pushpull_tcp.svg`, `doc/charts/main_reqrep_tcp.svg`:

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

Stop if any benchmark prints warnings or timeouts. Fix the bench peer
first, then rerun before charting.

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
export OMQ_HW_EXTRAS="performance governor, turbo off"
maturin develop --release
python scripts/update_perf.py --impl pyomq
python scripts/update_perf.py --chart-only
```

For a local smoke run that does not append JSONL or update docs:

```sh
python scripts/update_perf.py --quick --impl pyomq
```

## Releasing

### Automation

`release-plz` runs on every push to `main`
(`.github/workflows/release-plz.yml`). It opens or updates a release PR,
creates annotated tags after merge, publishes to crates.io, and creates
GitHub releases. Configuration lives in `release-plz.toml`.

### Steps

1. **Review the release-plz PR.** Verify semver bumps.

2. **Curate changelogs.** For each bumped crate, insert a new
   `## [x.y.z]` section below `## [Unreleased]`. Never modify existing
   versioned sections.

3. **Update zguide examples.** Bump `omq-tokio` versions in
   `examples/zguide-tokio/*/Cargo.toml`.

4. **Merge the release PR.** release-plz tags and publishes to
   crates.io automatically.

5. **pyomq** if changed: bump `bindings/pyomq/Cargo.toml` and
   `bindings/pyomq/pyproject.toml`, run `cargo update -p pyomq` inside
   `bindings/pyomq`, add a changelog entry, then push a `pyomq-v*` tag.

### Crates To Check

`omq-proto`, `yring`, `omq-tokio`, `omq-libzmq`, `pyomq`.
