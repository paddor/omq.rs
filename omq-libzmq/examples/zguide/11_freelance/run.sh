#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg11_freelance"

echo "=== Model 1: Sequential Failover ==="
"$BIN" server ipc://@omq-zguide-11-server2-c server2 0 &
S2=$!
sleep 0.2
"$BIN" client_sequential ipc://@omq-zguide-11-server1-c ipc://@omq-zguide-11-server2-c ipc://@omq-zguide-11-server3-c
kill "$S2" 2>/dev/null || true; wait "$S2" 2>/dev/null || true

echo
echo "=== Model 2: Shotgun ==="
"$BIN" server ipc://@omq-zguide-11-server1-c fast 0 &
S1=$!
"$BIN" server ipc://@omq-zguide-11-server2-c slow 0.3 &
S2=$!
sleep 0.2
"$BIN" client_shotgun ipc://@omq-zguide-11-server1-c ipc://@omq-zguide-11-server2-c
kill "$S1" "$S2" 2>/dev/null || true; wait "$S1" "$S2" 2>/dev/null || true

echo
echo "=== Model 3: Tracked ==="
"$BIN" server ipc://@omq-zguide-11-server1-c server1 0 &
S1=$!
"$BIN" server ipc://@omq-zguide-11-server2-c server2 0 &
S2=$!
sleep 0.2
"$BIN" client_tracked ipc://@omq-zguide-11-server1-c ipc://@omq-zguide-11-server2-c &
CLIENT=$!
sleep 0.8
kill "$S1" 2>/dev/null || true
echo "--- server1 killed ---"
wait "$CLIENT" 2>/dev/null || true
kill "$S2" 2>/dev/null || true
