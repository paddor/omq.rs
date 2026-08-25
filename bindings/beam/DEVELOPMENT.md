# OMQ.beam Development

Run commands from repository root unless shown otherwise.

## Prerequisites

- Erlang/OTP 27 or newer.
- `rebar3`.
- Elixir 1.18 or newer for the Elixir wrapper and `erlzmq` benchmark peer.
- Gleam built at `~/src/gleam/target/release/gleam` or available on `PATH`.
- Rust toolchain with Cargo.
- Python 3 for benchmarks and chart generation.
- `pyzmq` for TCP interop tests.

## Tests

Erlang API and NIF tests:

```sh
(cd bindings/beam && rebar3 eunit)
```

Rust NIF checks:

```sh
(cd bindings/beam/native && cargo fmt --check && cargo check)
```

Elixir wrapper:

```sh
(cd bindings/beam/elixir && \
  mix format --check-formatted lib/omq.ex ../scripts/bench_peer.exs && \
  mix compile --warnings-as-errors)
```

Gleam wrapper:

```sh
(cd bindings/beam/gleam && \
  ~/src/gleam/target/release/gleam format --check src/omq_gleam.gleam && \
  ~/src/gleam/target/release/gleam check)
```

## Benchmarks

The benchmark script builds the Erlang app, builds the Rust NIF in release
mode, copies the release shared object into `priv/`, compiles Elixir, and
builds Gleam.

Quick run:

```sh
python3 bindings/beam/scripts/update_perf.py --quick
```

Full default run:

```sh
python3 bindings/beam/scripts/update_perf.py
```

Run selected implementations and sizes:

```sh
python3 bindings/beam/scripts/update_perf.py \
  --impl omq-erlang,omq-elixir,omq-gleam,erlzmq \
  --sizes 16,128,1024,4096,32768
```

Results append to:

```text
~/.cache/omq.beam/<lang>/bindings.jsonl
```

where `<lang>` is `erlang`, `elixir`, or `gleam`.

## Chart

Regenerate SVG from cached rows:

```sh
python3 bindings/beam/scripts/update_perf.py --chart-only
```

Output:

```text
bindings/beam/doc/charts/bindings.svg
```

The script reads repo-root `.chart_hw` when present:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 cores, performance governor, turbo off
```

Useful flags:

- `--quick`
- `--no-build`
- `--chart-only`
- `--impl a,b,c`
- `--sizes 16,128,1024,4096,32768`
- `--rounds N`
- `--duration SECONDS`
- `--warmup-duration SECONDS`
- `--latency-duration SECONDS`
- `--latency-warmup-duration SECONDS`
- `--timeout SECONDS`
