#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
"$ROOT/build.sh"

for example in req_rep poller monitor; do
  echo
  echo "=== cppzmq/$example ==="
  "$ROOT/bin/$example"
done
