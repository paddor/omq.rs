#!/bin/sh
# Run the OMQ.php tests against a built zmq extension: tests/*_test.php,
# tests/native_*.php, ext-zmq's .phpt suite, and the PHP-FPM check when
# php-fpm is found. scripts/test-php.sh, scripts/test-php-matrix.rb, and the
# PIE release workflow (which ships this script with tests/) use it.
#
# usage: test-extension.sh PATH/TO/zmq.so
#
# Env:
#   PHP                   PHP binary (default: php)
#   OMQ_PHP_FPM           php-fpm binary (default: search)
#   OMQ_PHP_RUN_TESTS     php-src run-tests.php (default: search, then
#                         download the one of this PHP version)
#   OMQ_PHP_TEST_TIMEOUT  seconds per test file (default: 120)
#   OMQ_PHP_REQUIRE=fpm   fail instead of skipping the FPM check
set -eu

ext=${1:?usage: test-extension.sh PATH/TO/zmq.so}
case $ext in /*) ;; *) ext="$(pwd)/$ext" ;; esac
tests="$(cd "$(dirname "$0")/../tests" && pwd)"
php=${PHP:-php}
php_binary="$("$php" -n -r 'echo PHP_BINARY;')"
runner=
command -v timeout >/dev/null 2>&1 && runner="timeout ${OMQ_PHP_TEST_TIMEOUT:-120}"

fail() {
    echo "error: $*" >&2
    exit 1
}

# Skipped tests would hide an extension that does not load.
"$php_binary" -n -d "extension=$ext" -r 'exit(extension_loaded("zmq") ? 0 : 1);' ||
    fail "$ext does not load into $php_binary"
"$php_binary" -n -d "extension=$ext" -r 'printf("::: PHP %s %s, %s\n", PHP_VERSION, PHP_ZTS ? "ZTS" : "NTS", PHP_OS);'

# native_lifecycle.php forks. Load pcntl when it is a shared extension.
pcntl=
if ! "$php_binary" -n -r 'exit(function_exists("pcntl_fork") ? 0 : 1);'; then
    "$php_binary" -n -d extension=pcntl -r 'exit(function_exists("pcntl_fork") ? 0 : 1);' 2>/dev/null ||
        fail "pcntl is missing from $php_binary"
    pcntl="-d extension=pcntl"
fi

for test_file in "$tests"/*_test.php "$tests"/native_*.php; do
    echo "::: $test_file"
    # shellcheck disable=SC2086 # $runner and $pcntl split into words.
    $runner "$php_binary" -n $pcntl -d "extension=$ext" "$test_file"
done

find_run_tests() {
    if [ -n "${OMQ_PHP_RUN_TESTS:-}" ]; then
        echo "$OMQ_PHP_RUN_TESTS"
        return
    fi
    ext_dir="$("$php_binary" -n -r 'echo PHP_EXTENSION_DIR;')"
    prefix="$(dirname "$(dirname "$php_binary")")"
    for candidate in "$ext_dir/build/run-tests.php" "$prefix/lib/php/build/run-tests.php"; do
        if [ -f "$candidate" ]; then
            echo "$candidate"
            return
        fi
    done
    version="$("$php_binary" -n -r 'echo PHP_VERSION;')"
    curl -fsSL -o "$work/downloaded-run-tests.php" \
        "https://raw.githubusercontent.com/php/php-src/php-$version/run-tests.php"
    echo "$work/downloaded-run-tests.php"
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# run-tests.php writes next to itself and into the test directory.
cp "$(find_run_tests)" "$work/run-tests.php"
cp -R "$tests/phpt" "$work/phpt"
echo "::: ext-zmq .phpt suite"
# run-tests.php times out quiet tests itself, but not every hang.
suite_runner=
command -v timeout >/dev/null 2>&1 && suite_runner="timeout ${OMQ_PHPT_SUITE_TIMEOUT:-900}"
status=0
# shellcheck disable=SC2086
TEST_PHP_EXECUTABLE="$php_binary" NO_INTERACTION=1 $suite_runner \
    "$php_binary" -n "$work/run-tests.php" -n -d "extension=$ext" -q "$work/phpt" >"$work/run.log" 2>&1 ||
    status=$?
# GNU timeout exits 124; BusyBox timeout dies of the TERM it sends.
case $status in
124 | 137 | 143)
    tail -n 5 "$work/run.log" >&2
    fail "ext-zmq .phpt suite hung; the last lines above name the tests before it"
    ;;
esac
grep -E '^(Tests (passed|failed|skipped)|Expected fail)' "$work/run.log" || true
grep -E '^SKIP ' "$work/run.log" | sed 's/^/  /' || true
# run-tests.php exits 0 even when tests fail.
if grep -qE '^(FAILED|BORKED) TEST SUMMARY' "$work/run.log"; then
    sed -n '/^FAILED TEST SUMMARY/,/^=====/p;/^BORKED TEST SUMMARY/,/^=====/p' "$work/run.log" >&2
    for diff in "$work"/phpt/*.diff; do
        [ -f "$diff" ] || continue
        echo "--- $diff" >&2
        head -n 40 "$diff" >&2
    done
    fail "ext-zmq .phpt suite failed"
fi
grep -qE '^Tests passed +: +[1-9]' "$work/run.log" || fail "no .phpt test passed"

fpm=${OMQ_PHP_FPM:-}
if [ -z "$fpm" ]; then
    fpm="$(command -v php-fpm 2>/dev/null || true)"
fi
if [ -z "$fpm" ]; then
    minor="$("$php_binary" -n -r 'echo PHP_MAJOR_VERSION, ".", PHP_MINOR_VERSION;')"
    for candidate in "$(dirname "$(dirname "$php_binary")")/sbin/php-fpm" /usr/local/sbin/php-fpm "/usr/sbin/php-fpm$minor"; do
        if [ -x "$candidate" ]; then
            fpm=$candidate
            break
        fi
    done
fi
if [ -z "$fpm" ]; then
    case ",${OMQ_PHP_REQUIRE:-}," in
    *,fpm,*) fail "php-fpm not found" ;;
    esac
    echo "skip: PHP-FPM check: php-fpm not found"
else
    echo "::: PHP-FPM check ($fpm)"
    # shellcheck disable=SC2086
    $runner "$php_binary" -n -d "extension=$ext" "$tests/fpm_check.php" "$fpm" "$ext"
fi
echo "::: all OMQ.php tests passed"
