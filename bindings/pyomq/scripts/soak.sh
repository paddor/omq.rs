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
done

failed=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    printf 'error: pyomq soak failed: %s\n' "${names[$i]}" >&2
    failed=1
  fi
done
exit "$failed"
