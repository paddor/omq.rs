#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg07_clone"
"$BIN" server ipc://@omq-zguide-07-updates-c ipc://@omq-zguide-07-snapshot-c &
sleep 0.3
"$BIN" client ipc://@omq-zguide-07-updates-c ipc://@omq-zguide-07-snapshot-c
