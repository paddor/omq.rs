#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg08_majordomo"
"$BIN" broker ipc://@omq-zguide-08-frontend-c ipc://@omq-zguide-08-backend-c 3 &
sleep 0.3
"$BIN" worker ipc://@omq-zguide-08-backend-c echo 0 &
"$BIN" worker ipc://@omq-zguide-08-backend-c echo 1 &
"$BIN" worker ipc://@omq-zguide-08-backend-c upper 0 &
sleep 0.3
"$BIN" client ipc://@omq-zguide-08-frontend-c
