#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
"$ROOT/build.sh"

export OMQ_ZGUIDE_SKIP_BUILD=1

for run in "$ROOT"/[0-9][0-9]_*/run.sh; do
  echo
  echo "=== $(basename "$(dirname "$run")") ==="
  bash "$run"
done
