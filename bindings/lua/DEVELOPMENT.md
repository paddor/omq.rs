# OMQ.lua Development

Run commands from the repository root unless shown otherwise.

## Prerequisites

- `/usr/bin/lua` with Lua 5.4.
- Rust toolchain with Cargo.
- `python3` for benchmark chart generation.

## Normal Test Pass

```sh
./scripts/test-lua.sh
```

`test_pyzmq_interop.lua` runs when `python3 -c 'import zmq'` works and skips
otherwise.

The wrapper builds `bindings/lua/native` and sets:

```sh
LUA_PATH="$PWD/bindings/lua/lua/?.lua;;"
LUA_CPATH="$PWD/bindings/lua/native/target/debug/lib?.so;;"
```

## Native Checks

```sh
cargo fmt --manifest-path bindings/lua/native/Cargo.toml
cargo clippy --manifest-path bindings/lua/native/Cargo.toml --all-targets -- -D warnings
```

## Soak Tests

The Lua soak wrapper builds the native module in release mode and runs the
mixed workload soak with `/usr/bin/lua`:

```sh
bindings/lua/scripts/soak.sh
```

Default durations are `600 1800 3600` seconds. Useful env:

- `OMQ_LUA_SOAK_DURATIONS="600 1800 3600"`
- `OMQ_LUA_SOAK_WORKERS=12`
- `OMQ_LUA_SOAK_TIMEOUT_EXTRA_SECS=120`
- `OMQ_LUA_SOAK_SCENARIOS=tcp,inproc,pubsub,context-churn`
- `OMQ_LUA_SOAK_SKIP_SCENARIOS=pubsub`

## Benchmark Chart

The chart script follows the Go and Python binding benchmark shape. It runs two
Lua processes over TCP, measures fixed PUSH/PULL windows per message size,
measures REQ/REP latency, appends rows, and renders:

```text
bindings/lua/doc/charts/bindings.svg
```

Rows append to:

```text
~/.cache/omq.lua/bindings.jsonl
```

It benchmarks `omq.lua` by default. It also benchmarks `lzmq` when
`require("lzmq")` works for the selected Lua binary. Missing `lzmq` is skipped
without failing the `omq.lua` run. If `luarocks` is available, the script uses
`luarocks path` so user-local rocks are visible.

Quick local run:

```sh
bindings/lua/scripts/update_perf.py --quick
```

Full default run:

```sh
bindings/lua/scripts/update_perf.py
```

Useful flags:

- `--sizes 16,128,1k,8k,32k`
- `--impls omq.lua,lzmq`
- `--latency-impls omq.lua,lzmq`
- `--rounds N`
- `--duration SECONDS`
- `--warmup-duration SECONDS`
- `--latency-iters N`
- `--latency-warmup N`
- `--chart-only`
- `--no-save`
- `--no-chart`
- `--no-build`

Useful env:

- `OMQ_BENCH_ARENA_THRESHOLD=2048` overrides OMQ.lua's frame arena threshold.

The machine is noisy; treat current rows as provisional and rerun on an idle
system before publishing performance claims.
