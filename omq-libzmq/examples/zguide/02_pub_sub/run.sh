#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg02_pub_sub"
"$BIN" publisher ipc://@omq-zguide-02-pubsub-c 20 &
sleep 0.3
"$BIN" subscriber ipc://@omq-zguide-02-pubsub-c weather.nyc 10 &
"$BIN" subscriber ipc://@omq-zguide-02-pubsub-c weather.sfo 10 &
wait
