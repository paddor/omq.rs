#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cpu_count() {
  if command -v nproc >/dev/null 2>&1; then
    nproc
  else
    getconf _NPROCESSORS_ONLN
  fi
}

durations="${OMQ_JAVA_SOAK_DURATIONS:-300 600 1800 3600}"
workers="${OMQ_JAVA_SOAK_WORKERS:-$(cpu_count)}"
cargo_cmd="${CARGO:-cargo}"

case "$(uname -s)" in
  Darwin)
    os="macos"
    library="libomq_java.dylib"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    os="windows"
    library="omq_java.dll"
    ;;
  *)
    os="linux"
    library="libomq_java.so"
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64)
    arch="x86_64"
    ;;
  arm64|aarch64)
    arch="aarch64"
    ;;
  *)
    arch="$(uname -m)"
    ;;
esac

platform="${os}-${arch}"
native_dir="${repo_root}/bindings/java/native/target/release"
resource_dir="${repo_root}/bindings/java/target/test-classes/io/omq/native/${platform}"

"${cargo_cmd}" build --release --manifest-path "${repo_root}/bindings/java/native/Cargo.toml" \
  --features plain,curve,lz4,zstd

mvn -f "${repo_root}/bindings/java/pom.xml" -DskipNative=true -DskipTests test-compile
mkdir -p "${resource_dir}"
cp "${native_dir}/${library}" "${resource_dir}/"

for duration in ${durations}; do
  echo "== OMQ.java soak: ${duration}s, workers=${workers} =="
  OMQ_JAVA_SOAK=1 \
  OMQ_JAVA_SOAK_DURATION_SECS="${duration}" \
  OMQ_JAVA_SOAK_WORKERS="${workers}" \
    mvn -f "${repo_root}/bindings/java/pom.xml" -DskipNative=true -Dtest=JavaSoakTest test
done
