#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
duration="${1:-${OMQ_PYOMQ_SOAK_DURATION:-1h}}"

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
python="${OMQ_PYTHON:-${repo_root}/bindings/pyomq/.venv/bin/python}"
maturin="${OMQ_MATURIN:-${repo_root}/bindings/pyomq/.venv/bin/maturin}"
jobs="${OMQ_PYOMQ_SOAK_JOBS:-4}"
if [[ ! "$jobs" =~ ^[1-9][0-9]*$ ]]; then
  printf 'error: OMQ_PYOMQ_SOAK_JOBS must be a positive integer\n' >&2
  exit 2
fi

cd "${repo_root}/bindings/pyomq"
"$maturin" develop --release

pids=()
names=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap 'cleanup; exit 130' INT TERM

mapfile -t test_ids < <(
  "$python" -m pytest --collect-only -q tests/soak |
    sed -n '/^tests\/soak\/.*::/p'
)
if [[ ${#test_ids[@]} -eq 0 ]]; then
  printf 'error: no pyomq soak tests collected\n' >&2
  exit 1
fi
if (( jobs > ${#test_ids[@]} )); then
  jobs="${#test_ids[@]}"
fi
printf '== pyomq soak: %ss, jobs=%s ==\n' "$seconds" "$jobs"

active=0
failed=0
for test_id in "${test_ids[@]}"; do
  name="${test_id#tests/soak/}"
  name="${name//.py::/-}"
  (
    OMQ_SOAK_DURATION_SECS="$seconds" \
      timeout "$((seconds + 120))s" "$python" -m pytest -v --tb=short \
        -o "timeout=$((seconds + 90))" "$test_id"
  ) 2>&1 | sed -u "s/^/[${name}] /" &
  pids+=("$!")
  names+=("$name")
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
