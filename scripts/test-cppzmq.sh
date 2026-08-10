#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

cxx="${CXX:-c++}"
cargo_cmd="${CARGO:-cargo}"

if ! command -v "$cxx" >/dev/null 2>&1; then
    echo "error: C++ compiler not found: $cxx" >&2
    exit 1
fi

if [[ -n "${CPPZMQ_CFLAGS:-}" ]]; then
    read -r -a cppzmq_cflags <<<"$CPPZMQ_CFLAGS"
else
    if ! command -v pkg-config >/dev/null 2>&1; then
        echo "error: pkg-config not found" >&2
        exit 1
    fi

    if ! pkg-config --exists cppzmq; then
        echo "error: cppzmq not found. Install cppzmq-dev." >&2
        exit 1
    fi

    read -r -a cppzmq_cflags <<<"$(pkg-config --cflags cppzmq)"
fi

"$cargo_cmd" build -p omq-libzmq

out_dir="$repo_root/target/omq-test-tools"
lib_dir="$repo_root/target/debug"
cppzmq_dir="$repo_root/omq-libzmq/tests/cppzmq"
mkdir -p "$out_dir"

case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="$lib_dir:${DYLD_LIBRARY_PATH:-}"
        ;;
    *)
        export LD_LIBRARY_PATH="$lib_dir:${LD_LIBRARY_PATH:-}"
        ;;
esac

for src in "$cppzmq_dir"/*.cpp; do
    name="$(basename "$src" .cpp)"
    out="$out_dir/cppzmq-$name"
    "$cxx" \
        -std=c++17 \
        -Wall \
        -Wextra \
        -Werror \
        -I "$repo_root/omq-libzmq/include" \
        -I "$cppzmq_dir" \
        "${cppzmq_cflags[@]}" \
        "$src" \
        -L "$lib_dir" \
        -lomq_zmq \
        -pthread \
        -Wl,-rpath,"$lib_dir" \
        -o "$out"
    "$out"
done
