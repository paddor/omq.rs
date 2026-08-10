#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg10_binary_star"
"$BIN" primary ipc://@omq-zguide-10-primary-c ipc://@omq-zguide-10-heartbeat-c &
PRIMARY=$!
sleep 0.3
"$BIN" backup ipc://@omq-zguide-10-heartbeat-c ipc://@omq-zguide-10-backup-c &
sleep 0.5

"$BIN" client ipc://@omq-zguide-10-primary-c ipc://@omq-zguide-10-backup-c 2
sleep 0.5

kill "$PRIMARY" 2>/dev/null || true
echo "--- primary killed ---"
sleep 0.7

"$BIN" client ipc://@omq-zguide-10-primary-c ipc://@omq-zguide-10-backup-c 2
