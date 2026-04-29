#!/usr/bin/env bash
# Run every test in the workspace + bindings, with every cargo feature
# combination. Used as the "test everything" entry point. Exits non-
# zero on the first failing step.
set -euo pipefail

cd "$(dirname "$0")/.."

run() { echo "::: $*"; "$@"; }

# 1. Default workspace: NULL mechanism + tcp/ipc/inproc/udp.
run cargo test --workspace

# 2. Per-backend feature gates. Mechanism/compression features are
#    additive; toggle them in pairs that hit the gated test files.
run cargo test -p omq-tokio  --features curve     --test curve
run cargo test -p omq-compio --features curve     --test curve
run cargo test -p omq-tokio  --features blake3zmq --test blake3zmq
run cargo test -p omq-compio --features blake3zmq --test blake3zmq
run cargo test -p omq-tokio  --features lz4       --test lz4_tcp
run cargo test -p omq-compio --features lz4       --test lz4_tcp
run cargo test -p omq-tokio  --features zstd      --test zstd_tcp
run cargo test -p omq-compio --features zstd      --test zstd_tcp
run cargo test -p omq-tokio  --features priority  --test priority
run cargo test -p omq-compio --features priority  --test priority

# 3. Facade crate: both backend choices.
run cargo test -p omq
run cargo test -p omq --no-default-features --features tokio-backend

# 4. Hand-rolled fuzz suites (~1M iters each; slow). Skip with
#    `OMQ_SKIP_FUZZ=1` for fast loops.
if [[ "${OMQ_SKIP_FUZZ:-}" != "1" ]]; then
  run cargo test -p omq-tokio  --features fuzz
  run cargo test -p omq-compio --features fuzz
fi

# 5. pyomq sync + asyncio + cross-impl interop.
if [[ -d bindings/pyomq/.venv ]]; then
  pushd bindings/pyomq >/dev/null
  # shellcheck disable=SC1091
  source .venv/bin/activate
  run maturin develop --release
  run pytest -v
  deactivate
  popd >/dev/null
else
  echo "skip: bindings/pyomq/.venv not set up; see bindings/pyomq/README.md"
fi

echo "all tests passed"
