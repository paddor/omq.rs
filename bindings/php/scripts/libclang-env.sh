#!/usr/bin/env bash
# Print LIBCLANG_PATH and CLANG_PATH for ext-php-rs's bindgen step.
# bindgen loads the newest libclang but takes builtin headers from the first
# clang it finds; with several LLVM versions installed they can disagree.
# Pick the newest /usr/lib/llvm-N that has libclang, clang, and its headers.
# Prints nothing when LIBCLANG_PATH is already set or no such LLVM exists.
# Output is KEY=VALUE lines, usable with `export` or GitHub's $GITHUB_ENV.
set -euo pipefail

[[ -n "${LIBCLANG_PATH:-}" ]] && exit 0

shopt -s nullglob
for dir in $(printf '%s\n' /usr/lib/llvm-* | sort -V -r); do
    libs=("$dir"/lib/libclang*.so*)
    headers=("$dir"/lib/clang/*/include)
    if [[ -x "$dir/bin/clang" && ${#libs[@]} -gt 0 && ${#headers[@]} -gt 0 ]]; then
        echo "LIBCLANG_PATH=$dir/lib"
        echo "CLANG_PATH=${CLANG_PATH:-$dir/bin/clang}"
        exit 0
    fi
done
