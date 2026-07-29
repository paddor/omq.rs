#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <target-triple> <cargo-test-args...>" >&2
  exit 2
fi

target="$1"
shift
timeout_seconds="${OMQ_TEST_TIMEOUT_SECONDS:-180}"

list_output="$(mktemp)"
trap 'rm -f "$list_output"' EXIT

timeout --kill-after=10s "${timeout_seconds}s" \
  cross test --target "$target" "$@" -- --list --format terse > "$list_output"
mapfile -t tests < <(sed -n 's/: test$//p' "$list_output")

if [[ "${#tests[@]}" -eq 0 ]]; then
  echo "no tests found: cross test --target $target $*" >&2
  exit 1
fi

for test_name in "${tests[@]}"; do
  echo "::group::$test_name"
  timeout --kill-after=10s "${timeout_seconds}s" \
    cross test --target "$target" "$@" "$test_name" -- --exact
  echo "::endgroup::"
done
