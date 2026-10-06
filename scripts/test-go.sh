#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

cargo_cmd="${CARGO:-cargo}"

"$cargo_cmd" build --release --manifest-path bindings/go/native/Cargo.toml

# Cargo config and CARGO_TARGET_DIR also apply to this separate workspace.
target_dir="$("$cargo_cmd" metadata --format-version 1 --no-deps \
    --manifest-path bindings/go/native/Cargo.toml \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
lib_dir="$target_dir/release"
export CGO_LDFLAGS="\"-L$lib_dir\" ${CGO_LDFLAGS:-}"

case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="$lib_dir:${DYLD_LIBRARY_PATH:-}"
        ;;
    *)
        export LD_LIBRARY_PATH="$lib_dir:${LD_LIBRARY_PATH:-}"
        ;;
esac

(cd bindings/go && go test -count=1 ./...)
if [[ "${OMQ_GO_RACE:-}" == "1" ]]; then
    (cd bindings/go && go test -race -count=1 ./...)
fi
