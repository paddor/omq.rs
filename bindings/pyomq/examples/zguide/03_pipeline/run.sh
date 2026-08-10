#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
PYTHON=${PYTHON:-python3}
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}../../../bindings/pyomq/python"

"$PYTHON" sink.py &
sleep 0.3
"$PYTHON" worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
"$PYTHON" worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
"$PYTHON" worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
sleep 0.3
"$PYTHON" ventilator.py
wait
