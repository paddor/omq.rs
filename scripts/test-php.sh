#!/usr/bin/env bash
# Run the PHP tests in both modes:
#   drop-in  the PECL zmq extension with libzmq.so.5 -> libomq_zmq
#   native   bindings/php/native, the zmq extension built on omq-libzmq,
#            tested by bindings/php/scripts/test-extension.sh: the test
#            scripts, ext-zmq's .phpt suite, and a PHP-FPM check
# Each mode runs when its prerequisites are present and skips otherwise.
#
# Env:
#   OMQ_PHP      PHP command for the drop-in mode (default: php)
#   OMQ_PROFILE  Cargo profile: release or debug (default: release, which
#                ships; optimizations expose bugs debug builds hide)
#   OMQ_PHP_TEST_TIMEOUT  seconds per test file (default: 120)
#   OMQ_PHP_FPM  php-fpm binary for the native FPM check (default: search)
#   OMQ_PHP_REQUIRE=dropin,native,fpm  fail instead of skipping these
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

read -r -a php <<<"${OMQ_PHP:-php}"
profile="${OMQ_PROFILE:-release}"
profile_args=()
[[ "$profile" == release ]] && profile_args=(--release)
runner=()
command -v timeout >/dev/null 2>&1 && runner=(timeout "${OMQ_PHP_TEST_TIMEOUT:-120}")

if ! command -v "${php[0]}" >/dev/null 2>&1; then
    echo "error: PHP not found: ${php[0]}" >&2
    exit 1
fi

skip() {
    if [[ ",${OMQ_PHP_REQUIRE:-}," == *",$1,"* ]]; then
        echo "error: $1 mode required: $2" >&2
        exit 1
    fi
    echo "skip: $1 mode: $2"
}

target_metadata() {
    cargo metadata --format-version 1 --no-deps "$@" \
        | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
}

run_dropin() {
    if ! "${php[@]}" -r 'exit(extension_loaded("zmq") ? 0 : 1);'; then
        skip dropin "PHP zmq extension not loaded (install php-zmq)"
        return
    fi

    local target_dir prefix
    target_dir="$(target_metadata)"
    prefix="$target_dir/omq-libzmq-compat/$profile"
    CARGO_TARGET_DIR="$target_dir" \
        omq-libzmq/scripts/stage-compat.sh "${profile_args[@]}" "$prefix" >/dev/null

    local lib_var=LD_LIBRARY_PATH
    [[ "$(uname -s)" == Darwin ]] && lib_var=DYLD_LIBRARY_PATH
    for test_file in bindings/php/tests/*_test.php; do
        echo "::: drop-in $test_file"
        env "$lib_var=$prefix/lib:${!lib_var:-}" "${runner[@]}" "${php[@]}" "$test_file"
    done
}

run_native() {
    if ! command -v php-config >/dev/null 2>&1; then
        skip native "php-config not found (install php-dev)"
        return
    fi

    local manifest=bindings/php/native/Cargo.toml target_dir setting
    while IFS= read -r setting; do
        export "${setting?}"
    done < <(bindings/php/scripts/libclang-env.sh)
    cargo build --manifest-path "$manifest" "${profile_args[@]}"
    target_dir="$(target_metadata --manifest-path "$manifest")"
    local extension="$target_dir/$profile/libomq_php.so"
    [[ "$(uname -s)" == Darwin ]] && extension="$target_dir/$profile/libomq_php.dylib"

    echo "::: native $extension"
    bindings/php/scripts/test-extension.sh "$extension"
}

run_dropin
run_native
