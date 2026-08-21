#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
duration="${1:-${OMQ_RUBY_SOAK_DURATION:-1h}}"

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
ruby="${OMQ_RUBY:-ruby}"
if ! ruby="$(command -v "$ruby")"; then
  printf 'error: Ruby executable not found: %s\n' "${OMQ_RUBY:-ruby}" >&2
  exit 1
fi
export PATH="$(dirname "$ruby"):$PATH"

cd "${repo_root}/bindings/ruby"
"$ruby" -S bundle check
"$ruby" -S bundle exec rake compile
OMQ_RUBY_SOAK=1 OMQ_RUBY_SOAK_DURATION_SECS="$seconds" \
  timeout "$((seconds + 120))s" \
  "$ruby" -Ilib:test test/test_soak.rb
