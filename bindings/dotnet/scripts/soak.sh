#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
duration="${1:-${OMQ_DOTNET_SOAK_DURATION:-1h}}"

if [[ ! "$duration" =~ ^([1-9][0-9]*)([smhd]?)$ ]]; then
  printf 'error: duration must be a positive integer followed by s, m, h, or d\n' >&2
  exit 2
fi

value="${BASH_REMATCH[1]}"
case "${BASH_REMATCH[2]:-s}" in
  s) multiplier=1 ;;
  m) multiplier=60 ;;
  h) multiplier=3600 ;;
  d) multiplier=86400 ;;
esac
seconds=$((10#$value * multiplier))

cargo build --release -p omq-libzmq
export LD_LIBRARY_PATH="${repo_root}/target/release:${LD_LIBRARY_PATH:-}"

OMQ_DOTNET_SOAK_DURATION_SECS="$seconds" \
  timeout "$((seconds + 120))s" \
  dotnet run --project "${repo_root}/bindings/dotnet/tests/Omq.Net.Soak.csproj" \
    --configuration Release
