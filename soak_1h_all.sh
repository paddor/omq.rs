#!/usr/bin/env bash
set -euo pipefail

export SOAK_FEATURES="${SOAK_FEATURES:-soak plain curve lz4 zstd ws}"
export OMQ_SOAK_DURATION_SECS="${OMQ_SOAK_DURATION_SECS:-3600}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

timeout_seconds="${SOAK_TIMEOUT_SECS:-4500}"

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
cleanup() {
  rm -f "$nextest_config"
}
trap cleanup EXIT

cat >"$nextest_config" <<'EOF'
[profile.default]
slow-timeout = { period = "75m", terminate-after = 1, grace-period = "10s" }

[profile.ci]
inherits = "default"
fail-fast = false
failure-output = "immediate-final"
status-level = "slow"
final-status-level = "slow"

[profile.extended]
inherits = "ci"
slow-timeout = { period = "75m", terminate-after = 1, grace-period = "10s" }

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

if [[ -z "${SOAK_TEST_THREADS:-}" ]]; then
  mapfile -t soak_tests < <(
    cargo nextest list "${nextest_common[@]}" -T oneline
  )

  if [[ ${#soak_tests[@]} -eq 0 ]]; then
    printf 'error: no soak tests found\n' >&2
    exit 1
  fi

  export SOAK_TEST_THREADS="${#soak_tests[@]}"
fi

printf 'soak_targets=%s\n' "${#soak_targets[@]}"
printf 'soak_target_list:\n'
printf '  %s\n' "${soak_targets[@]}"
printf 'soak_tests=%s\n' "$SOAK_TEST_THREADS"

bash scripts/ci-run-with-forensics.sh "all soak 1h" "$timeout_seconds" -- \
  cargo nextest run "${nextest_common[@]}" --test-threads="$SOAK_TEST_THREADS"
