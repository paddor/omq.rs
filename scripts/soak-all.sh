#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}/.."

if (( $# > 1 )); then
  printf 'usage: %s [DURATION]\n' "${0##*/}" >&2
  exit 2
fi

duration="${1:-1h}"
if [[ ! "$duration" =~ ^([1-9][0-9]*)([smhd]?)$ ]]; then
  printf 'error: duration must be a positive integer followed by s, m, h, or d\n' >&2
  exit 2
fi

duration_value="${BASH_REMATCH[1]}"
case "${BASH_REMATCH[2]:-s}" in
  s) duration_multiplier=1 ;;
  m) duration_multiplier=60 ;;
  h) duration_multiplier=3600 ;;
  d) duration_multiplier=86400 ;;
esac
duration_seconds=$((10#$duration_value * duration_multiplier))

export SOAK_FEATURES="${SOAK_FEATURES:-soak plain curve lz4 zstd ws}"
export OMQ_SOAK_DURATION_SECS="$duration_seconds"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
if [[ -z "${OMQ_RUBY:-}" ]] && ! command -v ruby >/dev/null 2>&1 && [[ -x /home/roadster/.rubies/ruby-4.0.6/bin/ruby ]]; then
  export OMQ_RUBY=/home/roadster/.rubies/ruby-4.0.6/bin/ruby
fi
if [[ -n "${OMQ_RUBY:-}" && -z "${RUBY:-}" ]]; then
  export RUBY="$OMQ_RUBY"
fi

timeout_seconds="${SOAK_TIMEOUT_SECS:-$((duration_seconds + 900))}"
if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf 'error: SOAK_TIMEOUT_SECS must be a positive integer\n' >&2
  exit 2
fi

mapfile -t soak_targets < <(
  awk '
    /^\[\[test\]\]/ { in_test = 1; next }
    /^\[/ { in_test = 0 }
    in_test && /^[[:space:]]*name[[:space:]]*=/ {
      split($0, parts, "\"")
      if (parts[2] ~ /^omq_soak_/) print parts[2]
    }
  ' omq-tokio/Cargo.toml
)

mapfile -t soak_files < <(
  find omq-tokio/tests -maxdepth 1 -type f -name 'omq_soak_*.rs' -printf '%f\n' |
    sed 's/\.rs$//' |
    sort
)

if [[ ${#soak_targets[@]} -eq 0 ]]; then
  printf 'error: no omq_soak_* targets found\n' >&2
  exit 1
fi

mapfile -t sorted_soak_targets < <(printf '%s\n' "${soak_targets[@]}" | sort)
missing_targets="$(comm -23 <(printf '%s\n' "${soak_files[@]}") <(printf '%s\n' "${sorted_soak_targets[@]}"))"
missing_files="$(comm -13 <(printf '%s\n' "${soak_files[@]}") <(printf '%s\n' "${sorted_soak_targets[@]}"))"

if [[ -n "$missing_targets" ]]; then
  printf 'error: soak test files missing Cargo.toml targets:\n%s\n' "$missing_targets" >&2
  exit 1
fi

if [[ -n "$missing_files" ]]; then
  printf 'error: Cargo.toml soak targets missing test files:\n%s\n' "$missing_files" >&2
  exit 1
fi

nextest_config="$(mktemp)"
pids=()
labels=()
jobs_running=0
stop_jobs() {
  for pid in "${pids[@]:-}"; do
    pkill -TERM -s "$pid" 2>/dev/null || true
    kill -- "-$pid" 2>/dev/null || true
  done
}
kill_jobs() {
  for pid in "${pids[@]:-}"; do
    pkill -KILL -s "$pid" 2>/dev/null || true
    kill -KILL -- "-$pid" 2>/dev/null || true
  done
}
cleanup() {
  local status=$?
  if (( jobs_running )); then
    stop_jobs
    sleep 0.2
    kill_jobs
  fi
  rm -f "$nextest_config"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

cat >"$nextest_config" <<EOF
[profile.default]
slow-timeout = { period = "${timeout_seconds}s", terminate-after = 1, grace-period = "10s" }

[profile.ci]
inherits = "default"
fail-fast = false
failure-output = "immediate-final"
status-level = "slow"
final-status-level = "slow"

[profile.extended]
inherits = "ci"
slow-timeout = { period = "${timeout_seconds}s", terminate-after = 1, grace-period = "10s" }

[[profile.ci.overrides]]
filter = 'package(omq-tokio) and binary(omq_zstd_tcp) and test(/pub_sub_zstd_io_lane_(send|try_send)_auto_train_dict_for_late_subscriber/)'
threads-required = "num-test-threads"
EOF

nextest_common=(
  --profile extended -p omq-tokio
  --config-file "$nextest_config"
  --features "$SOAK_FEATURES" --release
)

for target in "${soak_targets[@]}"; do
  nextest_common+=(--test "$target")
done

read_cpu_count() {
  local count
  count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || printf '1\n')"
  count="${count%%$'\n'*}"
  if [[ ! "$count" =~ ^[1-9][0-9]*$ ]]; then
    count=1
  fi
  printf '%s\n' "$count"
}

cpu_count="$(read_cpu_count)"
mapfile -t soak_tests < <(
  cargo nextest list "${nextest_common[@]}" -T oneline
)
if [[ ${#soak_tests[@]} -eq 0 ]]; then
  printf 'error: no soak tests found\n' >&2
  exit 1
fi

soak_test_count="${#soak_tests[@]}"
if [[ -z "${SOAK_TEST_THREADS:-}" ]]; then
  export SOAK_TEST_THREADS="$soak_test_count"
fi
if [[ ! "$SOAK_TEST_THREADS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'error: SOAK_TEST_THREADS must be a positive integer\n' >&2
  exit 2
fi

rust_waves=$(((soak_test_count + SOAK_TEST_THREADS - 1) / SOAK_TEST_THREADS))
rust_timeout_seconds="${SOAK_RUST_TIMEOUT_SECS:-$timeout_seconds}"
if [[ ! "$rust_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf 'error: SOAK_RUST_TIMEOUT_SECS must be a positive integer\n' >&2
  exit 2
fi

printf 'soak_duration=%s (%ss)\n' "$duration" "$duration_seconds"
printf 'soak_targets=%s\n' "${#soak_targets[@]}"
printf 'soak_target_list:\n'
printf '  %s\n' "${soak_targets[@]}"
printf 'soak_tests=%s\n' "$soak_test_count"
printf 'soak_test_threads=%s\n' "$SOAK_TEST_THREADS"
printf 'soak_test_waves=%s\n' "$rust_waves"
printf 'soak_cpu_count=%s\n' "$cpu_count"
printf 'rust_timeout=%ss\n' "$rust_timeout_seconds"

cargo_cmd="${CARGO:-cargo}"

if [[ "${SOAK_SKIP_PREBUILD:-0}" != "1" ]]; then
  printf 'prebuild=1\n'
  "$cargo_cmd" build --release --workspace --features "$SOAK_FEATURES"
  "$cargo_cmd" build --release -p omq-libzmq
  "$cargo_cmd" build --release --manifest-path bindings/pyomq/Cargo.toml \
    --features "plain curve lz4 zstd"
  (
    cd bindings/pyomq
    "${OMQ_MATURIN:-./.venv/bin/maturin}" develop --release
  )
  "$cargo_cmd" build --release --manifest-path bindings/go/native/Cargo.toml
  "$cargo_cmd" build --release --manifest-path bindings/java/native/Cargo.toml \
    --features plain,curve,lz4,zstd
  mvn -f bindings/java/pom.xml -DskipNative=true -DskipTests test-compile
  "$cargo_cmd" build --release --manifest-path bindings/lua/native/Cargo.toml
  "$cargo_cmd" build --release --manifest-path bindings/node/Cargo.toml
  (
    cd bindings/node
    npm run build
  )
  "$cargo_cmd" build --release --manifest-path bindings/ruby/ext/omq_rs_native/Cargo.toml
  (
    cd bindings/ruby
    export PATH="$(dirname "$RUBY"):$PATH"
    "$RUBY" -S bundle check
    "$RUBY" -S bundle exec rake compile
  )
  dotnet build bindings/dotnet/tests/Omq.Net.Soak.csproj --configuration Release
else
  printf 'prebuild=0\n'
fi

run_job() {
  local label="$1"
  shift
  setsid --wait bash -c '
    set -o pipefail
    label="$1"
    shift
    "$@" 2>&1 | sed -u "s/^/[${label}] /"
  ' bash "$label" "$@" &
  pids+=("$!")
  labels+=("$label")
}

binding_workers="${SOAK_BINDING_WORKERS:-1}"
printf 'binding_workers=%s\n' "$binding_workers"
jobs_running=1
run_job rust env OMQ_SOAK_DISABLE_THROUGHPUT_STABILITY=1 \
  bash scripts/ci-run-with-forensics.sh "all soak ${duration}" "$rust_timeout_seconds" -- \
  cargo nextest run "${nextest_common[@]}" --test-threads="$SOAK_TEST_THREADS"
run_job pyomq bindings/pyomq/scripts/soak.sh "$duration"
run_job go env OMQ_GO_SOAK_DURATIONS="$duration_seconds" OMQ_GO_SOAK_WORKERS="$binding_workers" \
  bindings/go/scripts/soak.sh
run_job java env OMQ_JAVA_SOAK_DURATIONS="$duration_seconds" OMQ_JAVA_SOAK_WORKERS="$binding_workers" \
  bindings/java/scripts/soak.sh
run_job lua env OMQ_LUA_SOAK_DURATIONS="$duration_seconds" OMQ_LUA_SOAK_WORKERS="$binding_workers" \
  bindings/lua/scripts/soak.sh
run_job node bindings/node/scripts/soak.sh "$duration"
run_job ruby bindings/ruby/scripts/soak.sh "$duration"
run_job dotnet bindings/dotnet/scripts/soak.sh "$duration"

omq_ts_dir="${OMQ_TS_DIR:-${script_dir}/../../omq.ts}"
if [[ -d "$omq_ts_dir" ]]; then
  run_job omq-ts env OMQ_TS_SOAK_DURATION_SECS="$duration_seconds" \
    npm --prefix "$omq_ts_dir" run soak
else
  printf 'warning: OMQ.ts checkout not found at %s; set OMQ_TS_DIR\n' "$omq_ts_dir" >&2
fi

failed=0
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    printf 'error: soak job failed: %s\n' "${labels[$i]}" >&2
    failed=1
  fi
done
jobs_running=0
exit "$failed"
