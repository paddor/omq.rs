#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
PYTHON=${PYTHON:-python3}
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}../../../bindings/pyomq/python"

"$PYTHON" broker.py &
sleep 0.3
"$PYTHON" worker.py ipc://@omq-zguide-01-backend 0 &
"$PYTHON" worker.py ipc://@omq-zguide-01-backend 1 &
"$PYTHON" worker.py ipc://@omq-zguide-01-backend 2 &
sleep 0.3
"$PYTHON" client.py
