#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

cargo_cmd="${CARGO:-cargo}"

"$cargo_cmd" build --release --manifest-path bindings/go/native/Cargo.toml

case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="$repo_root/bindings/go/native/target/release:$repo_root/bindings/go/native/target/debug:${DYLD_LIBRARY_PATH:-}"
        ;;
    *)
        export LD_LIBRARY_PATH="$repo_root/bindings/go/native/target/release:$repo_root/bindings/go/native/target/debug:${LD_LIBRARY_PATH:-}"
        ;;
esac

(cd bindings/go && go test -count=1 ./...)
if [[ "${OMQ_GO_RACE:-}" == "1" ]]; then
    (cd bindings/go && go test -race -count=1 ./...)
fi
