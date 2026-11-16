#!/bin/sh
# Inside an official PHP image (Debian or Alpine): build bindings/php/native
# from the repository at /src, or with OMQ_PIE_VERSION install that OMQ.php
# release from Packagist with PIE, then run test-extension.sh on it.
# scripts/test-php-matrix.rb runs it with CARGO_HOME, RUSTUP_HOME, and
# CARGO_TARGET_DIR on volumes.
set -eu

alpine=
command -v apk >/dev/null && alpine=1
build_tools=
[ -z "${OMQ_PIE_VERSION:-}" ] && build_tools=1

if [ -n "$alpine" ]; then
    # shellcheck disable=SC2086 # $PHPIZE_DEPS lists packages.
    apk add --no-cache $PHPIZE_DEPS linux-headers curl git unzip ${build_tools:+clang-dev} >/dev/null
else
    apt-get update -qq >/dev/null
    apt-get install -y -qq --no-install-recommends curl git unzip ${build_tools:+clang libclang-dev} >/dev/null
fi
# native_lifecycle.php forks; the official images build pcntl on request.
docker-php-ext-install pcntl >/dev/null 2>&1

if [ -n "${OMQ_PIE_VERSION:-}" ]; then
    curl -fsSL -o /usr/local/bin/pie https://github.com/php/pie/releases/download/1.5.5/pie.phar
    chmod +x /usr/local/bin/pie
    pie install "paddor/omq-binding.php:$OMQ_PIE_VERSION"
    ext="$(php -r 'echo ini_get("extension_dir");')/zmq.so"
else
    if [ ! -x "$CARGO_HOME/bin/cargo" ]; then
        curl -fsSL https://sh.rustup.rs |
            sh -s -- -y -q --profile minimal --default-toolchain stable --no-modify-path
    fi
    export PATH="$CARGO_HOME/bin:$PATH"
    # A cdylib needs the dynamic C runtime on musl.
    [ -n "$alpine" ] && export RUSTFLAGS="-C target-feature=-crt-static"
    # The repository is read-only, so Cargo.lock must be current.
    (cd /src/bindings/php/native && cargo build --release --locked --quiet)
    ext="$CARGO_TARGET_DIR/release/libomq_php.so"
fi

sh /src/bindings/php/scripts/test-extension.sh "$ext"
