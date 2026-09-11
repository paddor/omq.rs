# AGENTS.md: bindings/pyomq

## Purpose

PyO3 binding for `omq-tokio`. Drop-in pyzmq API for Python: sync
(`pyomq`) and async (`pyomq.asyncio`). Stable ABI (`abi3-py312`,
Python 3.12+) via maturin. Release workflow publishes Linux wheels and
an sdist. Windows pyomq support is pending.

See [`doc/architecture.md`](../../doc/architecture.md) for internals:
threading model, queue relay, send/recv paths, zero-copy conversions,
proxy, authentication, error mapping, and known limitations.

## Python native stub maintenance

The native Python API is defined in `python/pyomq/_native.pyi` and is
maintained manually. Treat this stub as the authoritative contract for
`pyomq._native`.

When a Rust-exported symbol changes in `src/lib.rs`—including any new
`#[pyfunction]`, `#[pymodule_export]`, or `#[pyclass]` member—the stub
must be kept in sync in the same change. Do not assume PyO3's generated
information is complete enough to serve as a final API contract for the
Python binding.

## Build / test / lint

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq pytest-asyncio
maturin develop --release          # rebuild after every Rust change
pytest -v                          # soak tests excluded by default
cargo clippy --all-targets         # separate workspace, not --workspace
```

Maturin enables all features (`plain`, `curve`, `lz4`).
Runtime check: `pyomq.has("curve")`.

Own `Cargo.lock` and `uv.lock` (both committed). Not part of the
workspace root lock file.

## Benchmarks

Chart subtitle reads the repo-root `.chart_hw`. Use `OMQ_HW_PREFIX` and
`OMQ_HW_POSTFIX` to override its `prefix` and `postfix` values for one run.

Bench machine: i7-8700B, performance governor, turbo off.

```sh
maturin develop --release
python scripts/update_perf.py                # full (pyomq + pyzmq)
python scripts/update_perf.py --impl pyomq   # reuse latest pyzmq baseline
python scripts/update_perf.py --proxy-only --impl pyomq
python scripts/update_perf.py --chart-only   # regenerate SVG from JSONL
```

Set `OMQ_BENCH_TASKSET=1` on Linux to pin each peer to a separate CPU set.

Results in `~/.cache/omq/bindings.jsonl` (latest `run_id` per impl wins).
Regenerates `doc/charts/bindings.svg` and the proxy table in `README.md`.
Defaults are 3 measured rounds per size and implementation. Throughput uses
2.5 second windows; latency uses 1.5 second windows and only measures sizes up
to 4 KiB. Each round uses a 0.5 second warmup window, and the median round is
kept. `--quick` uses one 0.5 second measured round and 0.1 second warmup.

The proxy PUSH/PULL benchmark uses a native omq-tokio client
(`omq_bench_proxy_client`) to saturate the proxy without Python
sender/receiver overhead. Build it before running benchmarks:

```sh
cargo build --release -p omq-tokio --bin omq_bench_proxy_client
```

If the binary is missing, the proxy PUSH/PULL bench falls back to
Python sender/receiver (slower, measures Python overhead not proxy
throughput).
