#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg05_heartbeat"
"$BIN" publisher ipc://@omq-zguide-05-heartbeat-c &
sleep 0.1
"$BIN" monitor ipc://@omq-zguide-05-heartbeat-c
