#!/usr/bin/env bash
# Run every pattern bench in sequence. Mirrors omq/bench/run_all.rb in
# the Ruby OMQ repo: results stream to stdout and append to results.jsonl.
#
# Env knobs:
#   OMQ_BENCH_SIZES       comma-separated payload sizes in bytes
#   OMQ_BENCH_TRANSPORTS  comma-separated subset of {inproc,ipc,tcp}
#   OMQ_BENCH_PEERS       comma-separated peer counts (overrides per-pattern defaults)
#   OMQ_BENCH_NO_WRITE=1  suppress the JSONL append

set -euo pipefail

export OMQ_BENCH_RUN_ID="${OMQ_BENCH_RUN_ID:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

cd "$(dirname "$0")/../.."

# `cargo bench -p omq` runs every `[[bench]]` target in one invocation,
# avoiding the per-bench cargo dispatch + freshness-check overhead.
cargo bench -p omq
