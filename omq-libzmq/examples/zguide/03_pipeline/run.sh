#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg03_pipeline"
"$BIN" sink ipc://@omq-zguide-03-sink-c 1000 &
SINK_PID=$!
"$BIN" ventilator ipc://@omq-zguide-03-ventilator-c 1000 &
sleep 0.3
"$BIN" worker ipc://@omq-zguide-03-ventilator-c ipc://@omq-zguide-03-sink-c 0 &
"$BIN" worker ipc://@omq-zguide-03-ventilator-c ipc://@omq-zguide-03-sink-c 1 &
"$BIN" worker ipc://@omq-zguide-03-ventilator-c ipc://@omq-zguide-03-sink-c 2 &
wait "$SINK_PID"
