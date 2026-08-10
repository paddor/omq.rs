#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)
if [ "${OMQ_ZGUIDE_SKIP_BUILD:-0}" != 1 ]; then "$ROOT/build.sh"; fi
STORE=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$STORE"' EXIT

BIN="$ROOT/bin/zg09_titanic"
"$BIN" frontend ipc://@omq-zguide-09-frontend-c ipc://@omq-zguide-09-dispatch-c "$STORE" &
sleep 0.3
"$BIN" dispatcher ipc://@omq-zguide-09-dispatch-c "$STORE" &
sleep 0.3
"$BIN" client ipc://@omq-zguide-09-frontend-c
