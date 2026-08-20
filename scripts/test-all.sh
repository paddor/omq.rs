#!/usr/bin/env bash
# Run every test in the workspace + bindings against every supported
# Cargo feature combination. Used as the "test
# everything" entry point. Exits non-zero on the first failing step.
#
# Knobs:
#   OMQ_FUZZ=1          opt in to the ~1 M-iter hand-rolled fuzz suites
#   OMQ_SKIP_PYOMQ=1    skip the pyomq build + pytest pass
#   OMQ_SKIP_GO=1       skip the Go binding build + test pass
#   OMQ_SKIP_LUA=1      skip the Lua binding build + test pass
#   OMQ_SKIP_RUBY=1     skip the Ruby binding build + test pass
#   OMQ_GO_RACE=1       run Go binding tests with the race detector too
#   OMQ_TEST_RETRIES=N  retry each step up to N times (default 2) -
#                       a few timing-sensitive tests may need one
#                       retry on heavily loaded runners.
#   OMQ_TEST_JOBS=N     max parallel test steps (default 2)
#   OMQ_SKIP_PERF=1     skip the local perf smoke/hardware gate
#   OMQ_PERF_WAIT_SECS=N
#                       wait this long for prior test/build procs to exit
#                       before perf gate (default 30)
#   OMQ_PERF_QUIESCE_SECS=N
#                       quiet sleep before perf gate after no test/build
#                       procs remain (default 10)
#   OMQ_STRESS=1        run ignored local stress tests
#   OMQ_STRESS_ROUNDS=N connect-before-bind stress rounds (default 40)
#   OMQ_LOOM=1          run loom tests
set -euo pipefail

_repo_root="$(cd "$(dirname "$0")/.." && pwd)"
_tool_dir="$_repo_root/target/omq-test-tools"

if [[ "${OMQ_TEST_ALL_REEXEC:-}" != "1" ]]; then
    mkdir -p "$_tool_dir"
    ln -sfn "$(command -v bash)" "$_tool_dir/omq_test_all"
    export OMQ_TEST_ALL_REEXEC=1
    exec "$_tool_dir/omq_test_all" "$0" "$@"
fi

cd "$_repo_root"


_resolve_rustup_tool() {
    local tool=$1
    rustup which "$tool" 2>/dev/null || command -v "$tool"
}

_setup_tool_wrappers() {
    mkdir -p "$_tool_dir"
    ln -sfn "$(_resolve_rustup_tool cargo)" "$_tool_dir/omq_cargo"
    ln -sfn "$(_resolve_rustup_tool rustc)" "$_tool_dir/omq_rustc"
    ln -sfn "$(_resolve_rustup_tool rustdoc)" "$_tool_dir/omq_rustdoc"
    if command -v python3 >/dev/null 2>&1; then
        ln -sfn "$(command -v python3)" "$_tool_dir/omq_python3"
        export OMQ_PYTHON3="$_tool_dir/omq_python3"
    fi
    export PATH="$_tool_dir:$PATH"
    export CARGO="$_tool_dir/omq_cargo"
    export OMQ_RUSTC="$_tool_dir/omq_rustc"
    export OMQ_RUSTDOC="$_tool_dir/omq_rustdoc"
}

_setup_python_tools() {
    if command -v maturin >/dev/null 2>&1; then
        ln -sfn "$(command -v maturin)" "$_tool_dir/omq_maturin"
    fi
    hash -r
}

_setup_tool_wrappers


retries="${OMQ_TEST_RETRIES:-2}"
jobs="${OMQ_TEST_JOBS:-2}"

run() {
    echo "::: $*"
    local attempt=0
    while true; do
        attempt=$((attempt + 1))
        "$@" &
        local child=$!
        _foreground_pid=$child
        local rc=0
        wait "$child" || rc=$?
        _foreground_pid=0
        if [[ $rc -eq 0 ]]; then
            return 0
        fi
        if [[ $rc -eq 130 || $rc -eq 143 ]]; then
            return "$rc"
        fi
        if [[ $attempt -ge $retries ]]; then
            echo "::: FAILED after $attempt attempt(s): $*" >&2
            return 1
        fi
        echo "::: retry $attempt/$retries: $*" >&2
    done
}

omq_cargo_with_rust_tools() {
    RUSTC="$OMQ_RUSTC" RUSTDOC="$OMQ_RUSTDOC" exec omq_cargo "$@"
}

omq_cargo_with_loom() {
    export RUSTFLAGS="--cfg loom ${RUSTFLAGS:-}"
    omq_cargo_with_rust_tools "$@"
}

# Run a function in the background, keeping at most $jobs parallel workers.
# Usage: par <func> [args...]
_par_pids=()
_par_count=0
_par_rc=0
_foreground_pid=0

_par_has_pids() {
    [[ $_par_count -gt 0 ]]
}

_kill_tree() {
    local pid=$1
    local child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        _kill_tree "$child"
    done
    kill -TERM "$pid" 2>/dev/null || true
}

_kill_workers() {
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            _kill_tree "$pid"
        done
        for pid in "${_par_pids[@]}"; do
            wait "$pid" 2>/dev/null || true
        done
    fi
    _par_pids=()
    _par_count=0
}

_handle_interrupt() {
    trap - INT TERM
    if [[ $_foreground_pid -ne 0 ]]; then
        _kill_tree "$_foreground_pid"
    fi
    _kill_workers
    exit 130
}

trap _handle_interrupt INT TERM

par() {
    # Reap any finished workers.
    local new_pids=()
    local new_count=0
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
                new_count=$((new_count + 1))
            else
                wait "$pid" || _par_rc=$?
            fi
        done
    fi
    if [[ $new_count -gt 0 ]]; then
        _par_pids=("${new_pids[@]}")
    else
        _par_pids=()
    fi
    _par_count=$new_count

    if [[ $_par_rc -ne 0 ]]; then
        _kill_workers
        exit "$_par_rc"
    fi

    # Block until a slot is free.
    while _par_has_pids && [[ ${#_par_pids[@]} -ge $jobs ]]; do
        wait "${_par_pids[0]}" || _par_rc=$?
        if [[ ${#_par_pids[@]} -gt 1 ]]; then
            _par_pids=("${_par_pids[@]:1}")
            _par_count=$((_par_count - 1))
        else
            _par_pids=()
            _par_count=0
        fi
        if [[ $_par_rc -ne 0 ]]; then
            _kill_workers
            exit "$_par_rc"
        fi
    done

    "$@" &
    _par_pids+=($!)
    _par_count=$((_par_count + 1))
}

par_wait() {
    if _par_has_pids; then
        for pid in "${_par_pids[@]}"; do
            wait "$pid" || {
                _par_rc=$?
                _kill_workers
                exit "$_par_rc"
            }
        done
    fi
    _par_pids=()
    _par_count=0
}

_repo_perf_busy_processes() {
    local proc pid cmd cwd exe
    for proc in /proc/[0-9]*; do
        pid="${proc##*/}"
        [[ "$pid" == "$$" ]] && continue
        [[ "$pid" == "${BASHPID:-$$}" ]] && continue
        cmd="$(tr '\0' ' ' <"$proc/cmdline" 2>/dev/null || true)"
        [[ -n "$cmd" ]] || continue
        case "$cmd" in
            *omq_cargo*|*cargo*|*rustc*|*rustdoc*|*maturin*|*pytest*|*target/debug/deps/*|*target/release/deps/*)
                cwd="$(readlink "$proc/cwd" 2>/dev/null || true)"
                exe="$(readlink "$proc/exe" 2>/dev/null || true)"
                if [[ "$cwd" == "$_repo_root"* || "$exe" == "$_repo_root"* || "$cmd" == *"$_repo_root"* ]]; then
                    printf '%s %s\n' "$pid" "$cmd"
                fi
                ;;
        esac
    done
}

wait_for_perf_quiet() {
    par_wait

    local wait_secs="${OMQ_PERF_WAIT_SECS:-30}"
    local quiet_secs="${OMQ_PERF_QUIESCE_SECS:-10}"
    local deadline=$((SECONDS + wait_secs))
    local busy

    while true; do
        busy="$(_repo_perf_busy_processes)"
        if [[ -z "$busy" ]]; then
            if [[ "$quiet_secs" != "0" ]]; then
                echo "::: perf gate quiet for ${quiet_secs}s"
                sleep "$quiet_secs"
            fi
            busy="$(_repo_perf_busy_processes)"
            if [[ -z "$busy" ]]; then
                return 0
            fi
        fi

        if [[ $SECONDS -ge $deadline ]]; then
            echo "::: perf gate blocked by active test/build processes:" >&2
            echo "$busy" >&2
            return 1
        fi

        echo "::: perf gate waiting for prior test/build processes to exit"
        sleep 1
    done
}

# ---------------------------------------------------------------- #
# 1) Default workspace: NULL mechanism + tcp/ipc/inproc/udp,
#    no compression. Smallest deploy shape.
#    No --workspace: uses default-members, which excludes the example
#    crates.
# ---------------------------------------------------------------- #
# Clippy compiles all targets; a separate all-target build only duplicates
# that work before the test suite.
run omq_cargo clippy --all-targets --no-deps -- -D warnings
run omq_cargo clippy -p omq-libzmq --all-targets --no-deps -- -D warnings
run omq_cargo_with_rust_tools test
run omq_cargo_with_rust_tools test -p omq-libzmq
if command -v "${CXX:-c++}" >/dev/null 2>&1 \
    && command -v pkg-config >/dev/null 2>&1 \
    && pkg-config --exists cppzmq; then
    run "$_repo_root/scripts/test-cppzmq.sh"
else
    echo "skip: cppzmq tests require C++ compiler and cppzmq"
fi
if [[ "${OMQ_SKIP_GO:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_GO=1"
elif command -v go >/dev/null 2>&1; then
    run "$_repo_root/scripts/test-go.sh"
else
    echo "skip: Go binding tests require go"
fi
if [[ "${OMQ_SKIP_LUA:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_LUA=1"
elif [[ -x /usr/bin/lua || -n "${OMQ_LUA:-}" ]]; then
    run "$_repo_root/scripts/test-lua.sh"
else
    echo "skip: Lua binding tests require /usr/bin/lua"
fi
if [[ "${OMQ_SKIP_RUBY:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_RUBY=1"
elif command -v "${OMQ_RUBY:-ruby}" >/dev/null 2>&1; then
    run "$_repo_root/scripts/test-ruby.sh"
else
    echo "skip: Ruby binding tests require ruby"
fi

if [[ -n "${CI:-}" || -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "skip: perf gate disabled on CI"
elif [[ "${OMQ_SKIP_PERF:-}" == "1" ]]; then
    echo "skip: OMQ_SKIP_PERF=1"
else
    wait_for_perf_quiet
    run omq_cargo_with_rust_tools run --release -q -p omq-tokio --bin omq_perf_verify
fi

# ---------------------------------------------------------------- #
# 2) Feature-gated tests only. Step 1 already ran the full suite
#    with default features; mechanisms and compression transforms
#    only add handshake/transform code paths, so only the gated
#    test files need re-running. Step 3 (all features) catches
#    cross-feature interactions.
# ---------------------------------------------------------------- #
for feature in plain curve; do
    par run omq_cargo_with_rust_tools test -p omq-tokio  --features "$feature" --test "omq_$feature"
done
par run omq_cargo_with_rust_tools test -p omq-tokio  --features lz4 --test omq_lz4_tcp --test omq_lz4_pub_sub
par run omq_cargo_with_rust_tools test -p omq-tokio  --features zstd --test omq_zstd_tcp
par run omq_cargo_with_rust_tools test -p omq-proto  --features "lz4 ws" --test omq_proto_endpoint
par run omq_cargo_with_rust_tools test -p omq-tokio  --features "lz4 ws" --test omq_lz4_ws
par run omq_cargo_with_rust_tools test -p omq-tokio  --features plain --test omq_interop_pyzmq_plain
par run omq_cargo_with_rust_tools test -p omq-tokio  --features curve --test omq_interop_pyzmq_curve
par_wait

# ---------------------------------------------------------------- #
# 3) All non-fuzz features at once, full backend test suite. Catches
#    cross-feature interactions and internal #[cfg(feature)] items
#    inside otherwise-ungated test files (connect_before_bind lz4).
# ---------------------------------------------------------------- #
all_features='plain curve lz4 zstd ws'
par run omq_cargo_with_rust_tools test -p omq-proto  --features "$all_features"
par run omq_cargo_with_rust_tools test -p omq-tokio  --features "$all_features"
par_wait

# ---------------------------------------------------------------- #
# 4) Hand-rolled fuzz suites (~1 M iters each; slow). Opt in with
#    `OMQ_FUZZ=1`.
# ---------------------------------------------------------------- #
if [[ "${OMQ_FUZZ:-}" == "1" ]]; then
    par run omq_cargo_with_rust_tools test -p omq-tokio  --features "fuzz plain curve lz4 zstd ws" --release
    par_wait
fi

# ---------------------------------------------------------------- #
# 5) Local stress / exhaustive scheduling checks. Opt in because these
#    are slow or expensive on developer machines.
# ---------------------------------------------------------------- #
if [[ "${OMQ_STRESS:-}" == "1" ]]; then
    run omq_cargo_with_rust_tools test -p omq-tokio --test omq_stress_connect_before_bind -- --ignored
    run omq_cargo_with_rust_tools test -p omq-tokio --test omq_push_pull -- --ignored
    run omq_cargo_with_rust_tools test -p omq-tokio --test omq_transmit_slot_stress -- --ignored
else
    echo "skip: OMQ_STRESS=1"
fi

if [[ "${OMQ_LOOM:-}" == "1" ]]; then
    run omq_cargo_with_loom test -p yring --features async --test loom
    run omq_cargo_with_rust_tools test -p omq-tokio --test omq_loom_signal
else
    echo "skip: OMQ_LOOM=1"
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
    pyomq_venv="$(realpath .venv)"
    _setup_python_tools
    run omq_maturin develop --release
    # The checked-in venv may have been copied from another worktree; invoke
    # pytest through the active interpreter so its path cannot escape here.
    run "$pyomq_venv/bin/python" -m pytest -v
    deactivate
    popd >/dev/null
else
    echo "skip: bindings/pyomq/.venv not set up; see bindings/pyomq/README.md"
fi

echo "all tests passed"
