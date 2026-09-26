#!/usr/bin/env bash
# Profile a bench binary with samply (no root required).
#
# Usage:
#   ./scripts/flamegraph.sh [options]
#
# Options:
#   -b BENCH    bench name (default: omq_push_pull)
#   -p CRATE    crate to bench (default: omq-tokio)
#   -t LIST     OMQ_BENCH_TRANSPORTS override (default: tcp)
#   -s LIST     OMQ_BENCH_SIZES override (default: 512,2048)
#   -e N        OMQ_BENCH_PEERS override (default: unset)
#   -h          show this help
#
# Examples:
#   ./scripts/flamegraph.sh
#   ./scripts/flamegraph.sh -p omq-tokio -b push_pull -t tcp -s 128,512
#   ./scripts/flamegraph.sh -b req_rep -t inproc,ipc,tcp
#   ./scripts/flamegraph.sh -b latency -t tcp -s 256,1024
set -euo pipefail

BENCH=omq_push_pull
CRATE=omq-tokio
TRANSPORTS=tcp
SIZES=512,2048
PEERS=""

while getopts "b:p:t:s:e:h" opt; do
    case "$opt" in
        b) BENCH="$OPTARG" ;;
        p) CRATE="$OPTARG" ;;
        t) TRANSPORTS="$OPTARG" ;;
        s) SIZES="$OPTARG" ;;
        e) PEERS="$OPTARG" ;;
        h)
            sed -n '3,/^set /p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) exit 1 ;;
    esac
done

echo "Building $CRATE --bench $BENCH (release)..."
cargo build --release -p "$CRATE" --bench "$BENCH" 2>&1

# Locate the compiled bench binary.
BIN=$(cargo build --release -p "$CRATE" --bench "$BENCH" --message-format=json 2>/dev/null \
    | grep -o '"executable":"[^"]*"' | tail -1 | cut -d'"' -f4)

if [[ -z "$BIN" ]]; then
    # Fallback: find by name in target/release/deps
    BIN=$(find target/release/deps -maxdepth 1 -name "${BENCH}-*" -executable \
          ! -name "*.d" | sort -t- -k2 | tail -1)
fi

if [[ -z "$BIN" || ! -x "$BIN" ]]; then
    echo "ERROR: could not locate compiled bench binary for $BENCH" >&2
    exit 1
fi

echo "Profiling: $BIN"
echo "  transports=$TRANSPORTS  sizes=$SIZES${PEERS:+  peers=$PEERS}"
echo ""

export OMQ_BENCH_TRANSPORTS="$TRANSPORTS"
export OMQ_BENCH_SIZES="$SIZES"
export OMQ_BENCH_NO_WRITE=1
[[ -n "$PEERS" ]] && export OMQ_BENCH_PEERS="$PEERS"

PROFILE_OUT="${CRATE}-${BENCH}.json.gz"
echo "Profile -> $PROFILE_OUT"
echo ""

# --save-only: don't start a local server; --no-open: don't launch a browser.
samply record --save-only --no-open -o "$PROFILE_OUT" "$BIN"

echo ""
echo "Profile saved: $PROFILE_OUT"
echo "Inspect: samply load $PROFILE_OUT  (needs a browser)"
