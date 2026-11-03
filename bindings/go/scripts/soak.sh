#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
durations="${OMQ_GO_SOAK_DURATIONS:-300 600 1800 3600}"
workers="${OMQ_GO_SOAK_WORKERS:-$(nproc)}"
cargo_cmd="${CARGO:-cargo}"

if [[ "${SOAK_SKIP_BUILD:-0}" != "1" ]]; then
    "${cargo_cmd}" build --release --manifest-path "${repo_root}/bindings/go/native/Cargo.toml"
fi

# Cargo config and CARGO_TARGET_DIR also apply to this separate workspace.
target_dir="$("${cargo_cmd}" metadata --format-version 1 --no-deps \
    --manifest-path "${repo_root}/bindings/go/native/Cargo.toml" \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
lib_dir="${target_dir}/release"
export CGO_LDFLAGS="\"-L${lib_dir}\" ${CGO_LDFLAGS:-}"

case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="${lib_dir}:${DYLD_LIBRARY_PATH:-}"
        ;;
    *)
        export LD_LIBRARY_PATH="${lib_dir}:${LD_LIBRARY_PATH:-}"
        ;;
esac

for duration in ${durations}; do
    echo "== OMQ.go soak: ${duration}s, workers=${workers} =="
    (
        cd "${repo_root}/bindings/go"
        OMQ_GO_SOAK=1 \
        OMQ_GO_SOAK_DURATION_SECS="${duration}" \
        OMQ_GO_SOAK_WORKERS="${workers}" \
        GOMAXPROCS="${workers}" \
            go test -v -count=1 -run '^TestSoak' -parallel="${workers}" -timeout "$((duration + 120))s"
    )
done
