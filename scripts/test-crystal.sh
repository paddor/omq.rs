#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

cargo_cmd="${CARGO:-cargo}"
crystal_bin="${OMQ_CRYSTAL:-crystal}"

"$cargo_cmd" build -p omq-libzmq

case "$(uname -s)" in
    Darwin)
        dylib_var="DYLD_LIBRARY_PATH"
        ;;
    *)
        dylib_var="LD_LIBRARY_PATH"
        ;;
esac

lib_dir="$repo_root/target/debug"
export LIBRARY_PATH="$lib_dir${LIBRARY_PATH:+:$LIBRARY_PATH}"
export CRYSTAL_LIBRARY_PATH="$lib_dir${CRYSTAL_LIBRARY_PATH:+:$CRYSTAL_LIBRARY_PATH}"
export "$dylib_var=$lib_dir${!dylib_var:+:${!dylib_var}}"

"$crystal_bin" tool format --check bindings/crystal/src bindings/crystal/spec bindings/crystal/scripts
"$crystal_bin" spec bindings/crystal/spec --link-flags "-L$lib_dir -Wl,-rpath,$lib_dir"
