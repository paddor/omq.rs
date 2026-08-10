#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg04_lazy_pirate"
"$BIN" server ipc://@omq-zguide-04-server-c &
sleep 0.3
"$BIN" client ipc://@omq-zguide-04-server-c
