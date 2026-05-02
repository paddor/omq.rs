#!/usr/bin/env bash
# Compare omq-compio vs libzmq: single PUSH process → single PULL process,
# TCP loopback, small messages. Each cell: 3 s timed window after 500 ms warmup.
#
# Usage: ./scripts/bench_compare.sh [port]   (default port: 15555)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$SCRIPT_DIR/.."
DURATION=3
BASE_PORT="${1:-15555}"

# ---------- build ----------

echo "==> building omq-compio bench_peer..."
cargo build --release -p omq-compio --bin bench_peer 2>/dev/null
OMQ_PEER="$REPO/target/release/bench_peer"

echo "==> building libzmq bench_peer..."
gcc -O2 -o "$SCRIPT_DIR/libzmq_bench_peer" \
    "$SCRIPT_DIR/libzmq_bench_peer.c" -lzmq
LIBZMQ_PEER="$SCRIPT_DIR/libzmq_bench_peer"

# ---------- helpers ----------

# run_cell <peer_binary> <port> <size>
# Starts push in background, runs pull, kills push, returns "count elapsed size".
run_cell() {
    local peer="$1" port="$2" size="$3"

    "$peer" push "$port" "$size" &
    local push_pid=$!

    # Give the push side a moment to bind before pull connects.
    sleep 0.15

    local result
    result=$("$peer" pull "$port" "$size" "$DURATION")

    kill "$push_pid" 2>/dev/null || true
    wait "$push_pid" 2>/dev/null || true

    echo "$result"
}

# format_cell <count> <elapsed> <size>
format_cell() {
    local count="$1" elapsed="$2" size="$3"
    awk -v c="$count" -v e="$elapsed" -v s="$size" 'BEGIN {
        msgs = c / e
        mb   = (c * s) / e / 1000000
        printf "  %6dB  %8.0f msg/s  %7.1f MB/s\n", s, msgs, mb
    }'
}

# ---------- run ----------

SIZES=(128 512 2048)
PORT=$BASE_PORT

echo ""
echo "omq-compio $(cargo metadata --no-deps --format-version 1 2>/dev/null \
    | python3 -c 'import sys,json; pkgs=json.load(sys.stdin)["packages"]; \
      print(next(p["version"] for p in pkgs if p["name"]=="omq-compio"))' \
    2>/dev/null || echo '?') vs libzmq $(pkg-config --modversion libzmq 2>/dev/null || echo '?')"
echo "TCP loopback, 2 processes, ${DURATION}s window + 500ms warmup"
echo ""
printf "%-10s  %20s  %20s\n" "" "omq-compio" "libzmq"
printf "%-10s  %20s  %20s\n" "msg size" "(msg/s  |  MB/s)" "(msg/s  |  MB/s)"
echo "--------------------------------------------------------------------"

for size in "${SIZES[@]}"; do
    PORT=$((BASE_PORT + size))

    omq_raw=$(run_cell "$OMQ_PEER"    "$PORT"             "$size")
    lzq_raw=$(run_cell "$LIBZMQ_PEER" "$((PORT + 10000))" "$size")

    omq_msgs=$(echo "$omq_raw" | awk '{printf "%.0f", $1/$2}')
    omq_mb=$(echo   "$omq_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')
    lzq_msgs=$(echo "$lzq_raw" | awk '{printf "%.0f", $1/$2}')
    lzq_mb=$(echo   "$lzq_raw" | awk -v s="$size" '{printf "%.1f", ($1*s)/$2/1e6}')

    printf "  %6dB    %9s msg/s  %6s MB/s    %9s msg/s  %6s MB/s\n" \
        "$size" "$omq_msgs" "$omq_mb" "$lzq_msgs" "$lzq_mb"
done

echo ""
