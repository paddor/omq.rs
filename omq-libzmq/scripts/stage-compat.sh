#!/usr/bin/env bash
set -euo pipefail

profile=debug
cargo_profile_arg=()

usage() {
  echo "usage: $0 [--release] [PREFIX]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      profile=release
      cargo_profile_arg=(--release)
      shift
      ;;
    -h|--help)
      usage
      ;;
    --*)
      usage
      ;;
    *)
      break
      ;;
  esac
done

script_dir=$(cd "$(dirname "$0")" && pwd)
crate_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$crate_dir/.." && pwd)
prefix=${1:-"$repo_root/target/omq-libzmq-compat/$profile"}

cargo build -p omq-libzmq "${cargo_profile_arg[@]}"

mkdir -p "$prefix"
prefix=$(cd "$prefix" && pwd)

target_root=${CARGO_TARGET_DIR:-"$repo_root/target"}
if [[ "$target_root" != /* ]]; then
  target_root="$repo_root/$target_root"
fi
target_dir="$target_root/$profile"

case "$(uname -s)" in
  Darwin)
    omq_dynamic=libomq_zmq.dylib
    zmq_dynamic=libzmq.dylib
    ;;
  *)
    omq_dynamic=libomq_zmq.so
    zmq_dynamic=libzmq.so
    ;;
esac

omq_static=libomq_zmq.a
zmq_static=libzmq.a

if [[ ! -f "$target_dir/$omq_dynamic" ]]; then
  echo "missing dynamic library: $target_dir/$omq_dynamic" >&2
  exit 1
fi
if [[ ! -f "$target_dir/$omq_static" ]]; then
  echo "missing static library: $target_dir/$omq_static" >&2
  exit 1
fi

include_dir="$prefix/include"
lib_dir="$prefix/lib"
pc_dir="$lib_dir/pkgconfig"
cmake_zmq_dir="$lib_dir/cmake/ZeroMQ"
cmake_omq_dir="$lib_dir/cmake/omq-libzmq"

mkdir -p "$include_dir" "$lib_dir" "$pc_dir" "$cmake_zmq_dir" "$cmake_omq_dir"
cp "$crate_dir/include/zmq.h" "$include_dir/zmq.h"
cp "$target_dir/$omq_dynamic" "$lib_dir/$omq_dynamic"
cp "$target_dir/$omq_static" "$lib_dir/$omq_static"
ln -sfn "$omq_dynamic" "$lib_dir/$zmq_dynamic"
ln -sfn "$omq_static" "$lib_dir/$zmq_static"

version=$(awk -F'"' '/^version = / {print $2; exit}' "$crate_dir/Cargo.toml")
escape_sed() {
  printf '%s' "$1" | sed 's/[&|]/\\&/g'
}
prefix_esc=$(escape_sed "$prefix")

render() {
  local template=$1
  local out=$2
  sed \
    -e "s|@prefix@|$prefix_esc|g" \
    -e "s|@version@|$version|g" \
    -e "s|@omq_dynamic@|$omq_dynamic|g" \
    -e "s|@zmq_dynamic@|$zmq_dynamic|g" \
    "$template" > "$out"
}

render "$crate_dir/pkgconfig/libzmq.pc.in" "$pc_dir/libzmq.pc"
render "$crate_dir/pkgconfig/omq-libzmq.pc.in" "$pc_dir/omq-libzmq.pc"
render "$crate_dir/cmake/ZeroMQConfig.cmake.in" "$cmake_zmq_dir/ZeroMQConfig.cmake"
render "$crate_dir/cmake/omq-libzmqConfig.cmake.in" "$cmake_omq_dir/omq-libzmqConfig.cmake"

echo "staged omq-libzmq compatibility files in $prefix"
echo "  PKG_CONFIG_PATH=$pc_dir"
echo "  CMAKE_PREFIX_PATH=$prefix"
