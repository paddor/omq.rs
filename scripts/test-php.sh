#!/usr/bin/env bash
# Run the PECL zmq extension tests against libomq_zmq.
#
# Env:
#   OMQ_PHP      PHP command with the zmq extension (default: php)
#   OMQ_PROFILE  Cargo profile to stage: debug or release (default: debug)
#   OMQ_PHP_TEST_TIMEOUT  seconds per test file (default: 120)
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

read -r -a php <<<"${OMQ_PHP:-php}"
profile="${OMQ_PROFILE:-debug}"
profile_args=()
[[ "$profile" == release ]] && profile_args=(--release)

if ! command -v "${php[0]}" >/dev/null 2>&1; then
    echo "error: PHP not found: ${php[0]}" >&2
    exit 1
fi
if ! "${php[@]}" -r 'exit(extension_loaded("zmq") ? 0 : 1);'; then
    echo "error: PHP zmq extension not loaded. Install php-zmq." >&2
    exit 1
fi

target_dir="$(cargo metadata --format-version 1 --no-deps \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
prefix="$target_dir/omq-libzmq-compat/$profile"
CARGO_TARGET_DIR="$target_dir" \
    omq-libzmq/scripts/stage-compat.sh "${profile_args[@]}" "$prefix" >/dev/null

case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="$prefix/lib:${DYLD_LIBRARY_PATH:-}" ;;
    *) export LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" ;;
esac

runner=()
command -v timeout >/dev/null 2>&1 && runner=(timeout "${OMQ_PHP_TEST_TIMEOUT:-120}")

for test_file in bindings/php/tests/*_test.php; do
    echo "::: $test_file"
    "${runner[@]}" "${php[@]}" "$test_file"
done
