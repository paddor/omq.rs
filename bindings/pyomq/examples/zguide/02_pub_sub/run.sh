#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
PYTHON=${PYTHON:-python3}
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}../../../bindings/pyomq/python"

"$PYTHON" publisher.py ipc://@omq-zguide-02-pubsub 20 &
sleep 0.3
"$PYTHON" subscriber.py ipc://@omq-zguide-02-pubsub weather.nyc 10 &
"$PYTHON" subscriber.py ipc://@omq-zguide-02-pubsub weather.sfo 10 &
wait
