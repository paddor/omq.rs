#!/usr/bin/env bash
# Run every test in the workspace + bindings against every supported
# Cargo feature combination on both backends. Used as the "test
# everything" entry point. Exits non-zero on the first failing step.
#
# Knobs:
#   OMQ_FUZZ=1          opt in to the ~1 M-iter hand-rolled fuzz suites
#   OMQ_SKIP_PYOMQ=1    skip the pyomq build + pytest pass
#   OMQ_TEST_RETRIES=N  retry each step up to N times (default 1) -
#                       a few inproc priority tests are sensitive to
#                       multi-thread scheduler timing on heavily loaded
#                       runners; one retry is usually enough.
#   OMQ_TEST_JOBS=N     max parallel test steps (default 4)
set -euo pipefail

cd "$(dirname "$0")/.."

retries="${OMQ_TEST_RETRIES:-1}"
jobs="${OMQ_TEST_JOBS:-4}"

run() {
    echo "::: $*"
    local attempt=0
    while true; do
        attempt=$((attempt + 1))
        if "$@"; then
            return 0
        fi
        if [[ $attempt -ge $retries ]]; then
            echo "::: FAILED after $attempt attempt(s): $*" >&2
            return 1
        fi
        echo "::: retry $attempt/$retries: $*" >&2
    done
}

# Run a function in the background, keeping at most $jobs parallel workers.
# Usage: par <func> [args...]
_par_pids=()
par() {
    # Reap any finished workers and check for failures.
    local new_pids=()
    for pid in ${_par_pids[@]+"${_par_pids[@]}"}; do
        if kill -0 "$pid" 2>/dev/null; then
            new_pids+=("$pid")
        else
            wait "$pid"  # collect exit status; set -e propagates failure
        fi
    done
    _par_pids=(${new_pids[@]+"${new_pids[@]}"})

    # Block until a slot is free.
    while [[ ${#_par_pids[@]} -ge $jobs ]]; do
        wait "${_par_pids[0]}"
        _par_pids=("${_par_pids[@]:1}")
    done

    "$@" &
    _par_pids+=($!)
}

par_wait() {
    for pid in ${_par_pids[@]+"${_par_pids[@]}"}; do
        wait "$pid"
    done
    _par_pids=()
}

# ---------------------------------------------------------------- #
# 1) Default workspace: NULL mechanism + tcp/ipc/inproc/udp,
#    no compression, no priority. Smallest deploy shape.
#    No --workspace: uses default-members, which excludes the example
#    crates. zguide-compio and zguide-tokio depend on mutually
#    exclusive omq features and cannot be built in one invocation.
# ---------------------------------------------------------------- #
run cargo build --all-targets
run cargo clippy --all-targets --no-deps -- -D warnings
run cargo test


# ---------------------------------------------------------------- #
# 2) Each per-backend feature, full test suite for that backend.
#    Catches regressions in shared code that only surface under a
#    feature combination (e.g. priority swapping the routing
#    strategy alters the send-side data flow for every socket type,
#    not just the priority test file).
# ---------------------------------------------------------------- #
for feature in plain curve blake3zmq lz4 zstd priority; do
    par run cargo test -p omq-proto  --features "$feature"
    par run cargo test -p omq-tokio  --features "$feature"
    par run cargo test -p omq-compio --features "$feature"
done
par_wait

# ---------------------------------------------------------------- #
# 3) All non-fuzz features at once, full backend test suite. Catches
#    cross-feature interactions (e.g. CURVE + zstd + priority all
#    layered on the same connection).
# ---------------------------------------------------------------- #
all_features='plain curve blake3zmq lz4 zstd priority'
par run cargo test -p omq-proto  --features "$all_features"
par run cargo test -p omq-tokio  --features "$all_features"
par run cargo test -p omq-compio --features "$all_features"

# ---------------------------------------------------------------- #
# 4) Facade crate, both backend choices. Verifies the public API
#    re-exports compile and the `BACKEND` const reflects the
#    selected backend.
# ---------------------------------------------------------------- #
par run cargo test -p omq
par run cargo test -p omq --no-default-features --features tokio-backend
par_wait

# ---------------------------------------------------------------- #
# 5) Hand-rolled fuzz suites (~1 M iters each; slow). Opt in with
#    `OMQ_FUZZ=1`.
# ---------------------------------------------------------------- #
if [[ "${OMQ_FUZZ:-}" == "1" ]]; then
    par run cargo test -p omq-tokio  --features fuzz --release
    par run cargo test -p omq-compio --features fuzz --release
    par_wait
fi

# ---------------------------------------------------------------- #
# 6) pyomq sync + asyncio + cross-impl interop. Built out-of-tree
#    (its own Cargo.lock + maturin); skip when the dev venv isn't
#    set up. `OMQ_SKIP_PYOMQ=1` overrides.
# ---------------------------------------------------------------- #
if [[ "${OMQ_SKIP_PYOMQ:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_PYOMQ=1"
elif [[ -d bindings/pyomq/.venv ]]; then
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
