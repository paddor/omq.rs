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

## Benchmark Chart

The chart script runs two Lua processes over TCP PUSH/PULL, measures fixed time
windows per message size, appends rows, and renders:

```text
bindings/lua/doc/charts/bindings.svg
```

Rows append to:

```text
~/.cache/omq.lua/bindings.jsonl
```

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
- `--rounds N`
- `--duration SECONDS`
- `--warmup-duration SECONDS`
- `--chart-only`
- `--no-save`
- `--no-chart`
- `--no-build`

The machine is noisy; treat current rows as provisional and rerun on an idle
system before publishing performance claims.
