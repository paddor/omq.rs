#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
durations="${OMQ_GO_SOAK_DURATIONS:-300 600 1800 3600}"
workers="${OMQ_GO_SOAK_WORKERS:-$(nproc)}"
cargo_cmd="${CARGO:-cargo}"

"${cargo_cmd}" build --release --manifest-path "${repo_root}/bindings/go/native/Cargo.toml"

case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="${repo_root}/bindings/go/native/target/release:${repo_root}/bindings/go/native/target/debug:${DYLD_LIBRARY_PATH:-}"
        ;;
    *)
        export LD_LIBRARY_PATH="${repo_root}/bindings/go/native/target/release:${repo_root}/bindings/go/native/target/debug:${LD_LIBRARY_PATH:-}"
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
