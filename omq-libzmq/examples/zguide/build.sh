#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$ROOT/../../.." && pwd)
TARGET="${CARGO_TARGET_DIR:-$REPO/target}/debug"
CC_BIN=${CC:-cc}

cargo build -p omq-libzmq

mkdir -p "$ROOT/bin"

case "$(uname -s)" in
  Darwin)
    RPATH="-Wl,-rpath,$TARGET"
    ;;
  *)
    RPATH="-Wl,-rpath,$TARGET"
    ;;
esac

for src in "$ROOT"/[0-9][0-9]_*/*.c; do
  dir=$(basename "$(dirname "$src")")
  num=${dir%%_*}
  base=$(basename "$src" .c)
  out="$ROOT/bin/zg${num}_${base}"
  "$CC_BIN" -std=c11 -Wall -Wextra -Wpedantic -O2 -pthread \
    -I"$ROOT/common" -I"$REPO/omq-libzmq/include" \
    "$src" -L"$TARGET" $RPATH -lomq_zmq -o "$out"
done

echo "built C zguide examples in $ROOT/bin"
