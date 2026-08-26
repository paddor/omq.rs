#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
duration="${1:-${OMQ_ZIG_SOAK_DURATION:-1h}}"
jobs="${OMQ_ZIG_SOAK_JOBS:-}"
timeout_extra="${OMQ_ZIG_SOAK_TIMEOUT_EXTRA_SECS:-300}"

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
if [[ ! "$timeout_extra" =~ ^[0-9]+$ ]]; then
  printf 'error: OMQ_ZIG_SOAK_TIMEOUT_EXTRA_SECS must be a non-negative integer\n' >&2
  exit 2
fi

tests=(
  "context churn tracks resources"
  "req rep cycles track resources"
  "push pull sustained tracks resources"
  "pair bidirectional tracks resources"
  "multipart push pull tracks resources"
  "pub sub sustained tracks resources"
  "large messages track resources"
  "reconnect storm tracks resources"
  "peer churn tracks resources"
)

if [[ "${SOAK_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --release -p omq-libzmq
fi

export LD_LIBRARY_PATH="${repo_root}/target/release:${LD_LIBRARY_PATH:-}"

if [[ -z "$jobs" ]]; then
  jobs="${#tests[@]}"
fi
if [[ ! "$jobs" =~ ^[1-9][0-9]*$ ]]; then
  printf 'error: OMQ_ZIG_SOAK_JOBS must be a positive integer\n' >&2
  exit 2
fi
if (( jobs > ${#tests[@]} )); then
  jobs="${#tests[@]}"
fi

cd "${repo_root}"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 0.2
  for pid in "${pids[@]:-}"; do
    pkill -KILL -P "$pid" 2>/dev/null || true
    kill -KILL "$pid" 2>/dev/null || true
  done
}
trap 'cleanup; exit 130' INT TERM

printf '== OMQ.zig soak: %ss, jobs=%s ==\n' "$seconds" "$jobs"

active=0
failed=0
for test_name in "${tests[@]}"; do
  label="${test_name// /-}"
  bash -c '
    set -o pipefail
    label="$1"
    seconds="$2"
    repo_root="$3"
    test_name="$4"
    timeout_extra="$5"
    OMQ_ZIG_SOAK_DURATION_SECS="$seconds" \
      timeout "$((seconds + timeout_extra))s" \
      zig build --build-file "${repo_root}/bindings/zig/build.zig" \
        soak -Dtest-filter="$test_name" \
      2>&1 | sed -u "s/^/[${label}] /"
  ' bash "$label" "$seconds" "$repo_root" "$test_name" "$timeout_extra" &
  pids+=("$!")
  active=$((active + 1))
  if (( active >= jobs )); then
    if ! wait -n; then
      failed=1
    fi
    active=$((active - 1))
  fi
done

while (( active > 0 )); do
  if ! wait -n; then
    failed=1
  fi
  active=$((active - 1))
done

exit "$failed"
