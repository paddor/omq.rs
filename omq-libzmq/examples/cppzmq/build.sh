#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$ROOT/../../.." && pwd)
TARGET="${CARGO_TARGET_DIR:-$REPO/target}/debug"
CXX_BIN=${CXX:-c++}

if ! command -v "$CXX_BIN" >/dev/null 2>&1; then
  echo "error: C++ compiler not found: $CXX_BIN" >&2
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
    echo "error: cppzmq not found. Install cppzmq-dev or set CPPZMQ_CFLAGS." >&2
    exit 1
  fi
  read -r -a cppzmq_cflags <<<"$(pkg-config --cflags cppzmq)"
fi

cargo build -p omq-libzmq
mkdir -p "$ROOT/bin"

for src in "$ROOT"/*.cpp; do
  name=$(basename "$src" .cpp)
  "$CXX_BIN" \
    -std=c++17 \
    -Wall \
    -Wextra \
    -Werror \
    -I "$REPO/omq-libzmq/include" \
    "${cppzmq_cflags[@]}" \
    "$src" \
    -L "$TARGET" \
    -lomq_zmq \
    -pthread \
    -Wl,-rpath,"$TARGET" \
    -o "$ROOT/bin/$name"
done

echo "built cppzmq examples in $ROOT/bin"
