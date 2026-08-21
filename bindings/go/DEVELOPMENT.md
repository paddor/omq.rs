# OMQ.go Development

Run commands from the repository root unless shown otherwise.

## Prerequisites

- Go 1.25 or newer.
- cgo enabled.
- Rust toolchain with Cargo.
- `python3` for performance chart generation.
- libzmq development files when generating `zmq4` comparison rows.

## Normal Test Pass

Build the native library and run the Go tests:

```sh
./scripts/test-go.sh
```

Include the Go race detector:

```sh
OMQ_GO_RACE=1 ./scripts/test-go.sh
```

Manual equivalent:

```sh
cargo build --release --manifest-path bindings/go/native/Cargo.toml
export LD_LIBRARY_PATH="$PWD/bindings/go/native/target/release:$PWD/bindings/go/native/target/debug:${LD_LIBRARY_PATH:-}"
(cd bindings/go && go test -count=1 ./...)
```

On macOS, set `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH`.

## Soak Tests

Use the soak wrapper:

```sh
bindings/go/scripts/soak.sh
```

Defaults:

- durations: `300 600 1800 3600` seconds
- workers: `nproc`
- `GOMAXPROCS`: same as workers

Run one 10 minute pass on all CPUs:

```sh
OMQ_GO_SOAK_DURATIONS=600 bindings/go/scripts/soak.sh
```

Run 10m, 30m, and 2h with 12 workers:

```sh
OMQ_GO_SOAK_DURATIONS="600 1800 7200" OMQ_GO_SOAK_WORKERS=12 bindings/go/scripts/soak.sh
```

Run one scenario:

```sh
OMQ_GO_SOAK_DURATIONS=1800 \
OMQ_GO_SOAK_SCENARIOS=context-churn \
bindings/go/scripts/soak.sh
```

Skip scenarios:

```sh
OMQ_GO_SOAK_SKIP_SCENARIOS=curve,compression bindings/go/scripts/soak.sh
```

Scenarios:

- `tcp`: TCP PUSH/PULL churn against a shared receiver.
- `curve`: CURVE TCP PUSH/PULL churn.
- `compression`: `lz4+tcp` and `zstd+tcp`, including dictionary use.
- `inproc`: inproc REQ/REP loop.
- `poller`: inproc fan-in through `Poller`.
- `pubsub`: TCP PUB/SUB subscriber churn.
- `protocol-mix`: IPC multipart and large messages, TCP REQ/REP, and
  bidirectional PAIR traffic.
- `context-churn`: repeated context/socket create, use, close cycles.

Soak logs include progress counters, Go heap, RSS, smaps rollup, FD count,
goroutine count, OS thread count, cgo calls, native wrapper live counters, and
scenario create/close counters.

Useful leak-check knobs:

- `OMQ_GO_SOAK_MAX_FD_GROWTH`
- `OMQ_GO_SOAK_MAX_FINAL_FD_GROWTH`
- `OMQ_GO_SOAK_HEAP_SLOPE_LIMIT_KIB_S`
- `OMQ_GO_SOAK_RSS_SLOPE_LIMIT_KIB_S`
- `OMQ_GO_SOAK_FD_SLOPE_LIMIT_PER_SEC`
- `OMQ_GO_SOAK_RSS_TAIL_GROWTH_PERCENT`
- `OMQ_GO_SOAK_HEAP_SLOPE_MIN_GROWTH_MB`
- `OMQ_GO_SOAK_RSS_SLOPE_MIN_GROWTH_MB`
- `OMQ_GO_SOAK_FD_SLOPE_MIN_GROWTH`
- `OMQ_GO_SOAK_HEAP_RESIDUAL_FLOOR_MB`
- `OMQ_GO_SOAK_RSS_TAIL_GROWTH_MIN_MB`

The native live counters must return to their baseline after shutdown.

## Benchmarks

Run perf work on an otherwise quiet machine. Do not run benchmark or profiler
jobs in parallel.

Build native first, then run Go microbenchmarks:

```sh
cargo build --release --manifest-path bindings/go/native/Cargo.toml
export LD_LIBRARY_PATH="$PWD/bindings/go/native/target/release:$PWD/bindings/go/native/target/debug:${LD_LIBRARY_PATH:-}"
(cd bindings/go && go test -run '^$' -bench=. -benchmem -count=5)
```

Run selected hot-path benchmarks:

```sh
(cd bindings/go && go test -run '^$' -bench='InprocPushPull.*128B' -benchmem -count=10)
```

The two-process TCP benchmarks use an internal peer mode driven by the test
binary. Do not set `OMQ_GO_BENCH_PEER` manually.

## Performance Chart

The chart script runs two-process TCP PUSH/PULL throughput and REQ/REP latency
over fixed time windows. Implementations include OMQ.go, `zmq4`, and
plaintext `grpc-go`. gRPC uses protobuf `bytes` blobs with TLS, compression,
retries, and application QoS disabled. Results append to:

```text
~/.cache/omq.go/bindings.jsonl
```

Override the cache root:

```sh
OMQ_GO_CACHE_DIR=/tmp/omq-go-cache bindings/go/scripts/update_perf.py
```

Quick local run:

```sh
bindings/go/scripts/update_perf.py --quick
```

Full default run:

```sh
bindings/go/scripts/update_perf.py
```

Defaults are 3 measured rounds per size and implementation. Throughput uses
2.5 second windows; latency uses 1.5 second windows and only measures sizes up
to 4 KiB. Each round uses a 0.5 second warmup window, and the median round is
kept. `--quick` uses one 0.5 second measured round and 0.1 second warmup.

Default sizes are:

```text
16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768
```

Run specific sizes:

```sh
bindings/go/scripts/update_perf.py --sizes 16,128,1k,8k,32k
```

Regenerate only the SVG from cached rows:

```sh
bindings/go/scripts/update_perf.py --chart-only
```

Output chart:

```text
bindings/go/doc/charts/bindings.svg
```

Useful chart script flags:

- `--throughput-only`
- `--latency-only`
- `--rounds N`
- `--warmup-rounds N`
- `--duration SECONDS`
- `--warmup-duration SECONDS`
- `--latency-duration SECONDS`
- `--latency-warmup-duration SECONDS`
- `--no-save`
- `--no-chart`
- `--no-build`
- `--no-harness-build`

`--chart-only` uses the latest cached row for each implementation, benchmark
kind, and message size. The cache is append-only; remove or archive the JSONL
file when old rows should no longer be considered.
