# OMQ.zig Development

Run commands from repository root unless shown otherwise.

## Prerequisites

- Zig 0.16.
- Rust toolchain with Cargo.
- `python3` for benchmark chart generation.
- `git` and network access when running comparison benchmarks.

## Normal Test Pass

Build native C ABI first:

```sh
cargo build --release -p omq-libzmq
```

Run Zig tests:

```sh
(cd bindings/zig && zig build test)
```

Run one filtered test group:

```sh
(cd bindings/zig && zig build test -Dtest-filter=pub)
```

Run soak tests. Default script duration is one hour:

```sh
bindings/zig/scripts/soak.sh
bindings/zig/scripts/soak.sh 10m
```

Soak tests sample RSS and `/proc/self/fd` counts and fail on resource growth.
They are not part of `zig build test`. The script runs all soak scenarios in
parallel by default, like pyomq, and each scenario gets the full requested
duration. Set `OMQ_ZIG_SOAK_JOBS=1` to run them serially. Set
`OMQ_ZIG_SOAK_TIMEOUT_EXTRA_SECS` to tune shutdown/build slack.

Override native paths when building outside this repository layout:

```sh
zig build test \
  -Domq-include-dir=/path/to/include \
  -Domq-lib-dir=/path/to/lib
```

## API Docs

Generate Zig API docs:

```sh
(cd bindings/zig && zig build docs)
```

Output:

```text
bindings/zig/zig-out/docs/
```

## Benchmarks

Run perf work on an otherwise quiet machine. Do not run benchmark or profiler
jobs in parallel.

The benchmark script is adapted from `bindings/pyomq/scripts/update_perf.py`.
It uses the same append-only JSONL model and SVG chart generator shape.

Implementations:

- `omq.zig`: this binding, backed by `omq-libzmq`.
- `zzmq`: `https://github.com/nine-lives-later/zzmq`.
- `zimq`: `https://github.com/uyha/zimq`, pinned to tag `zig-0.16`.

Rows append to:

```text
~/.cache/omq.zig/bindings.jsonl
```

Quick local run:

```sh
bindings/zig/scripts/update_perf.py --quick
```

Full default run:

```sh
bindings/zig/scripts/update_perf.py
```

Defaults are 3 measured rounds per implementation and size. Throughput uses
2.5 second windows. Latency uses 1.5 second windows and only measures sizes up
to 4 KiB. Each round uses a 0.5 second warmup window. The median round is kept.
`--quick` uses one 0.5 second measured round and 0.1 second warmup.

Useful flags:

- `--impl omq.zig`
- `--impl zzmq`
- `--impl zimq`
- `--sizes 16,128,1k,8k,32k`
- `--rounds N`
- `--target-runtime SECONDS`
- `--warmup-duration SECONDS`
- `--latency-duration SECONDS`
- `--latency-warmup-duration SECONDS`
- `--chart-only`
- `--no-save`
- `--no-chart`
- `--no-build`

Chart output:

```text
bindings/zig/doc/charts/bindings.svg
```

Regenerate only the SVG from cached rows:

```sh
bindings/zig/scripts/update_perf.py --chart-only
```

Hardware subtitle comes from repository-root `.chart_hw`:

```text
prefix=Ryzen 9 9950X
postfix=16 cores, turbo off, performance governor
```

`.chart_hw` is local-only and gitignored.

The machine is noisy. Treat current rows as provisional and rerun on an idle
system before publishing performance claims.
