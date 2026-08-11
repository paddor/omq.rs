#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

cargo_cmd="${CARGO:-cargo}"
lua_bin="${OMQ_LUA:-/usr/bin/lua}"

"$cargo_cmd" build --manifest-path bindings/lua/native/Cargo.toml

case "$(uname -s)" in
    Darwin)
        lib_pattern="?.dylib"
        ;;
    *)
        lib_pattern="lib?.so"
        ;;
esac

export LUA_PATH="$repo_root/bindings/lua/lua/?.lua;;"
export LUA_CPATH="$repo_root/bindings/lua/native/target/debug/$lib_pattern;;"

for test_file in bindings/lua/tests/test_*.lua; do
    echo "::: $test_file"
    "$lua_bin" "$test_file"
done
