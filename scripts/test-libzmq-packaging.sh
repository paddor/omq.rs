#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
prefix="$repo_root/target/omq-libzmq-packaging-smoke"
tmp="$repo_root/target/omq-libzmq-packaging-smoke-src"

command -v cc >/dev/null 2>&1 || { echo "error: cc not found" >&2; exit 1; }
command -v c++ >/dev/null 2>&1 || { echo "error: c++ not found" >&2; exit 1; }
command -v pkg-config >/dev/null 2>&1 || { echo "error: pkg-config not found" >&2; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo "error: cmake not found" >&2; exit 1; }

rm -rf "$prefix" "$tmp"
mkdir -p "$tmp"

"$repo_root/omq-libzmq/scripts/stage-compat.sh" "$prefix"

cat > "$tmp/pkg_smoke.c" <<'C'
#include <string.h>
#include <zmq.h>

int main(void) {
    void *ctx = zmq_ctx_new();
    void *push = zmq_socket(ctx, ZMQ_PUSH);
    void *pull = zmq_socket(ctx, ZMQ_PULL);
    int timeout = 1000;
    int linger = 0;
    zmq_setsockopt(push, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(push, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(push, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(pull, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(pull, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
    zmq_setsockopt(pull, ZMQ_LINGER, &linger, sizeof(linger));
    if (zmq_bind(pull, "inproc://pkg-smoke") != 0) return 2;
    if (zmq_connect(push, "inproc://pkg-smoke") != 0) return 3;
    if (zmq_send(push, "ok", 2, 0) != 2) return 4;
    char buf[8] = {0};
    if (zmq_recv(pull, buf, sizeof(buf), 0) != 2) return 5;
    if (memcmp(buf, "ok", 2) != 0) return 6;
    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
    return 0;
}
C

export PKG_CONFIG_PATH="$prefix/lib/pkgconfig"
export LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$prefix/lib:${DYLD_LIBRARY_PATH:-}"

cc "$tmp/pkg_smoke.c" $(pkg-config --cflags --libs libzmq) -Wl,-rpath,"$prefix/lib" -o "$tmp/pkg-smoke-libzmq"
"$tmp/pkg-smoke-libzmq"

cc "$tmp/pkg_smoke.c" $(pkg-config --cflags --libs omq-libzmq) -Wl,-rpath,"$prefix/lib" -o "$tmp/pkg-smoke-omq"
"$tmp/pkg-smoke-omq"

mkdir -p "$tmp/cmake"
cat > "$tmp/cmake/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.16)
project(omq_libzmq_packaging_smoke CXX)
find_package(ZeroMQ REQUIRED CONFIG)
add_executable(cmake-smoke main.cpp)
target_link_libraries(cmake-smoke PRIVATE ZeroMQ::ZeroMQ)
CMAKE
cat > "$tmp/cmake/main.cpp" <<'CPP'
#include <cstring>
#include <stdexcept>
#include <zmq.h>

int main() {
    int major = 0;
    int minor = 0;
    int patch = 0;
    zmq_version(&major, &minor, &patch);
    if (major != 4 || minor != 3 || patch != 6) {
        throw std::runtime_error("bad zmq version");
    }
    return 0;
}
CPP

cmake -S "$tmp/cmake" -B "$tmp/cmake-build" -DCMAKE_PREFIX_PATH="$prefix" >/dev/null
cmake --build "$tmp/cmake-build" >/dev/null
"$tmp/cmake-build/cmake-smoke"

echo "libzmq packaging smoke passed"
