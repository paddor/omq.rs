#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

BIN="$ROOT/bin/zg01_req_rep"
"$BIN" broker ipc://@omq-zguide-01-frontend-c ipc://@omq-zguide-01-backend-c &
sleep 0.3
"$BIN" worker ipc://@omq-zguide-01-backend-c 0 &
"$BIN" worker ipc://@omq-zguide-01-backend-c 1 &
"$BIN" worker ipc://@omq-zguide-01-backend-c 2 &
sleep 0.3
"$BIN" client ipc://@omq-zguide-01-frontend-c 9
"$BIN" echo
