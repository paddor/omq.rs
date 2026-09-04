# OMQ.beam Development

Run commands from repository root unless shown otherwise.

## Prerequisites

- Erlang/OTP 27 or newer.
- `rebar3`.
- Elixir 1.18 or newer for the Elixir wrapper, `erlzmq`, and `chumak`
  benchmark peers.
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
(cd bindings/beam/native && cargo check --no-default-features)
(cd bindings/beam/native && cargo clippy --all-targets)
(cd bindings/beam/native && cargo clippy --all-targets --no-default-features)
```

Default native features are `plain`, `curve`, `lz4`, and `zstd`. The
`--no-default-features` check keeps runtime `has/1` detection honest.

Elixir wrapper:

```sh
(cd bindings/beam/elixir && \
  mix format --check-formatted lib/omq.ex ../scripts/bench_peer.exs && \
  mix compile --warnings-as-errors && \
  mix test)
```

Gleam wrapper:

```sh
(cd bindings/beam/gleam && \
  ~/src/gleam/target/release/gleam format --check src/omq_gleam.gleam && \
  ~/src/gleam/target/release/gleam check && \
  ~/src/gleam/target/release/gleam test)
```

## Soak

Build once, then run the Erlang soak harness. Argument is duration in seconds
per scenario.

```sh
(cd bindings/beam && rebar3 compile)
(cd bindings/beam && escript scripts/soak.erl 300)
```

The harness currently runs sustained PUSH/PULL, REQ/REP cycles, and TCP peer
churn. It samples RSS, VmData, file descriptors, and thread count throughout
each scenario and fails on sustained RSS/FD growth or final FD/thread growth.
Use `600` or `1800` for longer 10 minute and 30 minute passes.

Useful resource knobs:

- `OMQ_BEAM_SOAK_REPORT_INTERVAL_SECS`
- `OMQ_BEAM_SOAK_SETTLE_MS`
- `OMQ_BEAM_SOAK_MAX_FD_GROWTH`
- `OMQ_BEAM_SOAK_MAX_FINAL_FD_GROWTH`
- `OMQ_BEAM_SOAK_MAX_THREAD_GROWTH`
- `OMQ_BEAM_SOAK_MAX_FINAL_THREAD_GROWTH`
- `OMQ_BEAM_SOAK_RSS_SLOPE_LIMIT_KIB_S`
- `OMQ_BEAM_SOAK_RSS_TAIL_GROWTH_PERCENT`
- `OMQ_BEAM_SOAK_TRACE_FD=1`

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
  --impl omq-erlang,omq-elixir,omq-gleam,erlzmq,chumak \
  --sizes 16,128,1024,4096,32768
```

Results append to:

```text
~/.cache/omq.beam/<lang>/bindings.jsonl
```

where `<lang>` is `erlang`, `elixir`, or `gleam`.

`erlzmq` is installed through Hex as `erlzmq_dnif`. `chumak` is installed
through Hex as `chumak`. Chumak PUSH/PULL throughput is capped at 512 B in
this harness because its large-frame stream path can crash the BEAM VM under
the two-process timed benchmark. `exzmq` is not benchmarked: it has no Hex
package, its GitHub package is marked work-in-progress, and its API only
covers CLIENT/SERVER with stdout logging.

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
- `--no-chart`
- `--chart-only`
- `--impl a,b,c`
- `--sizes 16,128,1024,4096,32768`
- `--rounds N`
- `--duration SECONDS`
- `--warmup-duration SECONDS`
- `--latency-duration SECONDS`
- `--latency-warmup-duration SECONDS`
- `--timeout SECONDS`

## Publishing

Hex package names:

- `omq`: Erlang base package. Owns the Rust NIF and native OMQ runtime.
- `omq_elixir`: Elixir wrapper. Depends on `omq`.
- `omq_gleam`: Gleam wrapper. Depends on `omq`.

Publish in that order. The wrapper packages cannot resolve until `omq` is
published on Hex.

One-time local setup:

```sh
mix local.hex --force
mix hex.user auth
mkdir -p ~/.config/rebar3
$EDITOR ~/.config/rebar3/rebar.config
rebar3 hex user auth
~/src/gleam/target/release/gleam hex authenticate
```

No standalone `hex` command is needed. Mix, Rebar3, and Gleam each publish
through their own Hex tasks.

Add this line to `~/.config/rebar3/rebar.config` if it is not already there:

```erlang
{plugins, [rebar3_hex]}.
```

Dry-run/audit the Erlang package:

```sh
(cd bindings/beam && rebar3 compile)
(cd bindings/beam && rebar3 hex build)
```

Publish `omq`:

```sh
(cd bindings/beam && rebar3 hex publish)
```

After Hex shows `omq` 0.2.0, dry-run/audit and publish the Elixir wrapper:

```sh
(cd bindings/beam/elixir && mix deps.get)
(cd bindings/beam/elixir && mix compile --warnings-as-errors)
(cd bindings/beam/elixir && mix hex.build --unpack)
(cd bindings/beam/elixir && mix hex.publish)
```

After Hex shows `omq` 0.2.0, dry-run/audit and publish the Gleam wrapper:

```sh
(cd bindings/beam/gleam && ~/src/gleam/target/release/gleam update)
(cd bindings/beam/gleam && ~/src/gleam/target/release/gleam check)
(cd bindings/beam/gleam && ~/src/gleam/target/release/gleam test)
(cd bindings/beam/gleam && ~/src/gleam/target/release/gleam export hex-tarball)
(cd bindings/beam/gleam && ~/src/gleam/target/release/gleam publish)
```

For token-based publishing, Mix reads `HEX_API_KEY`; Gleam reads
`HEXPM_API_KEY`. Prefer the interactive commands above for the first publish
so package metadata and included files can be reviewed before confirming.
