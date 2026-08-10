#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg06_last_value_cache"
"$BIN" cache ipc://@omq-zguide-06-publisher-c ipc://@omq-zguide-06-subscriber-c ipc://@omq-zguide-06-snapshot-c &
sleep 0.3
"$BIN" publisher ipc://@omq-zguide-06-publisher-c 5
sleep 0.5
"$BIN" subscriber ipc://@omq-zguide-06-snapshot-c ipc://@omq-zguide-06-subscriber-c
