# Development

Benchmarks and charts: [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md).
Fuzz tests, soak tests, and releases: [RELEASING.md](RELEASING.md).

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
cargo test -p omq-tokio --test omq_req_rep -- some_test_name
./scripts/test-cppzmq.sh
```

Feature-gated tests:

```sh
cargo test -p omq-tokio --features plain     --test omq_plain
cargo test -p omq-tokio --features curve     --test omq_curve
cargo test -p omq-tokio --features lz4       --test omq_lz4_tcp --test omq_lz4_pub_sub
cargo test -p omq-tokio --features dart     --test omq_dart_transport --test omq_dart_pool
```

Loom checks:

```sh
cargo test -p omq-tokio --test omq_loom_signal
cargo test -p omq-tokio --features dart --test omq_loom_dart_pool
RUSTFLAGS="--cfg loom" cargo test -p omq-proto --features dart --test omq_proto_loom_dart
```

The `omq-tokio` Loom test models `StateSignal` and `DataSignal` lost-wake
races. The reusable queue crates are maintained in
[fanring.rs](https://github.com/paddor/fanring.rs). Run their unit tests,
Loom models, and Miri checks in that workspace.

For coordinated local changes, override published queue dependencies explicitly:

```sh
cargo test --config 'patch.crates-io.yring.path="../fanring/yring"' \
  --config 'patch.crates-io.fanring.path="../fanring"' -p omq-tokio
```

OMQ requires `fanring` 0.3.9 and `yring` 0.3.20 for
`Consumer::release_with_full()` and `AsyncProducer::poll_ready()`.
Workspace and binding builds resolve these APIs from the registry. Put
`--config` after the Cargo subcommand so clippy and nextest forward local
overrides. In `bindings/pyomq`, use paths
`../../../fanring/yring` and `../../../fanring` for the same overrides.

Keep these overrides local. Registry dependencies must be published before OMQ
CI or packaging can resolve them without overrides. Refresh binding lockfiles
against the registry after publishing a new queue version. PR CI covers native
and Python binding tests on Windows as well as Linux and macOS.

Full sweep:

```sh
./scripts/test-all.sh
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
OMQ_SKIP_PERF=1 ./scripts/test-all.sh
OMQ_LOOM=1 ./scripts/test-all.sh
```

The full sweep runs `omq_perf_verify` locally. `.perf_hw` supplies hardware
thresholds; if absent, a smaller smoke gate runs. CI skips the gate.

Use `--list`, `--case NAME`, `--repeat N`, or `--measure-only` for focused checks.
`OMQ_PERF_WARMUP_MS` and `OMQ_PERF_MEASURE_MS` set durations;
`OMQ_PERF_CPUS` sets an ordered Linux CPU list. The verifier excludes warmup,
times bounded batches, and reports rates using actual elapsed time.
See [performance verification](doc/perf-verification.md) for threshold settings,
profile contracts, and the copy budget.

### Ruby Binding Tests

```sh
bundle install --gemfile bindings/ruby/Gemfile
scripts/test-ruby.sh
```

Set `OMQ_RUBY=/path/to/ruby` when Ruby is not on `PATH`.

## Stress Tests

```sh
cargo test -p omq-tokio --test omq_stress_connect_before_bind -- --test-threads=1
```

## Continuous Integration

GitHub CI runs `cargo fmt` and clippy on Linux, macOS Intel, macOS
ARM64, and Windows. It runs workspace tests on Linux x86_64 with MSRV
1.93, macOS ARM64, and Windows. Feature jobs cover CURVE and LZ4 on
Linux, macOS ARM64, and Windows. macOS test jobs run serially with
`--test-threads=1`. PR CI also runs 32-bit Linux cross-checks.
Extended CI adds Ubuntu ARM64.

`.github/workflows/ci.yml` gates every PR. Beyond fmt/clippy/tests it
runs, on Linux only:

| job | what |
|-----|------|
| `interop` | pyzmq NULL/STREAM + PLAIN + CURVE |
| `cppzmq` | `cppzmq` API tests against `libomq_zmq` |
| `fuzz-smoke` | parsers at 1M iters, socket actions at 200 |

`.github/workflows/extended.yml` runs Sundays at 03:00 UTC and on
`workflow_dispatch` (with `soak_duration_secs` / `fuzz_scale` inputs):
the fuzz targets at 100M / 2000 iters, the soak suite in five groups,
the `--ignored` stress tests, libzmq draft interop (`ws://`,
RADIO/DISH, and draft socket types).

The interop tests skip when their peer is missing so local runs stay
green without pyzmq or the libzmq helper binaries. CI sets
`OMQ_INTEROP_REQUIRED=1`, which turns a missing peer into a failure so
the job cannot pass without testing anything.

The libzmq draft interop job builds libzmq from source with
`ENABLE_DRAFTS=ON` (distro packages omit it, and without drafts libzmq
has no `ws://` transport or draft socket API) and caches the install.
