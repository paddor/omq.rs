#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
durations="${OMQ_LUA_SOAK_DURATIONS:-600 1800 3600}"
workers="${OMQ_LUA_SOAK_WORKERS:-$(nproc)}"
timeout_extra="${OMQ_LUA_SOAK_TIMEOUT_EXTRA_SECS:-120}"
cargo_cmd="${CARGO:-cargo}"
lua_bin="${OMQ_LUA:-/usr/bin/lua}"

if [[ "${SOAK_SKIP_BUILD:-0}" != "1" ]]; then
    "${cargo_cmd}" build --release --manifest-path "${repo_root}/bindings/lua/native/Cargo.toml"
fi

case "$(uname -s)" in
    Darwin)
        lib_pattern="?.dylib"
        ;;
    *)
        lib_pattern="lib?.so"
        ;;
esac

export LUA_PATH="${repo_root}/bindings/lua/lua/?.lua;;"
export LUA_CPATH="${repo_root}/bindings/lua/native/target/release/${lib_pattern};;"

for duration in ${durations}; do
    echo "== OMQ.lua soak: ${duration}s, workers=${workers} =="
    cmd=(
        "${lua_bin}"
        "${repo_root}/bindings/lua/tests/test_soak.lua"
    )
    if command -v timeout >/dev/null 2>&1; then
        cmd=(timeout "$((duration + timeout_extra))s" "${cmd[@]}")
    fi
    OMQ_LUA_SOAK=1 \
    OMQ_LUA_SOAK_DURATION_SECS="${duration}" \
    OMQ_LUA_SOAK_WORKERS="${workers}" \
        "${cmd[@]}"
done
