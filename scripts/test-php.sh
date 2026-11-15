#!/usr/bin/env bash
# Run the PHP tests in both modes:
#   drop-in  the PECL zmq extension with libzmq.so.5 -> libomq_zmq
#   native   bindings/php/native, the zmq extension built on omq-libzmq,
#            plus ext-zmq's own .phpt suite
# Each mode runs when its prerequisites are present and skips otherwise.
#
# Env:
#   OMQ_PHP      PHP command for the drop-in mode (default: php)
#   OMQ_PROFILE  Cargo profile: debug or release (default: debug)
#   OMQ_PHP_TEST_TIMEOUT  seconds per test file (default: 120)
#   OMQ_PHP_REQUIRE=dropin,native  fail instead of skipping these modes
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

read -r -a php <<<"${OMQ_PHP:-php}"
profile="${OMQ_PROFILE:-debug}"
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

run_phpt() {
    local extension=$1 work=$2
    local run_tests
    run_tests="$(php-config --extension-dir)/build/run-tests.php"
    if [[ ! -f "$run_tests" ]]; then
        echo "skip: phpt suite: run-tests.php not found at $run_tests"
        return
    fi

    # run-tests.php writes next to itself and into the test directory.
    rm -rf "$work" && mkdir -p "$work"
    cp "$run_tests" "$work/run-tests.php"
    cp -r bindings/php/tests/phpt "$work/phpt"
    echo "::: native ext-zmq .phpt suite"
    TEST_PHP_EXECUTABLE="$(command -v php)" NO_INTERACTION=1 \
        php -n "$work/run-tests.php" -n -d "extension=$extension" -q "$work/phpt" \
        >"$work/run.log" 2>&1 || true
    grep -E '^(Tests (passed|failed|skipped)|Expected fail)' "$work/run.log" || true
    # run-tests.php exits 0 even when tests fail.
    if grep -qE '^(FAILED|BORKED) TEST SUMMARY' "$work/run.log"; then
        sed -n '/^FAILED TEST SUMMARY/,/^=====/p;/^BORKED TEST SUMMARY/,/^=====/p' "$work/run.log" >&2
        exit 1
    fi
}

run_native() {
    if ! command -v php-config >/dev/null 2>&1; then
        skip native "php-config not found (install php-dev)"
        return
    fi

    local manifest=bindings/php/native/Cargo.toml target_dir
    cargo build --manifest-path "$manifest" "${profile_args[@]}"
    target_dir="$(target_metadata --manifest-path "$manifest")"
    local extension="$target_dir/$profile/libomq_php.so"
    [[ "$(uname -s)" == Darwin ]] && extension="$target_dir/$profile/libomq_php.dylib"

    for test_file in bindings/php/tests/*_test.php bindings/php/tests/native_*.php; do
        echo "::: native $test_file"
        "${runner[@]}" php -n -d "extension=$extension" "$test_file"
    done

    run_phpt "$extension" "$target_dir/omq-php-phpt"
}

run_dropin
run_native
