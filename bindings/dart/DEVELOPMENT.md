# OMQ.dart development

Requires the Dart SDK 3.10 or later and a Rust toolchain.

## Build and test

From the repository root:

```sh
cargo build --release -p omq-libzmq
cd bindings/dart
dart pub get
dart format --output none --set-exit-if-changed .
dart analyze --fatal-infos
LD_LIBRARY_PATH="$PWD/../../target/release" dart test
```

The package's own `pubspec.yaml` sets the `system: true` user-define, so its
tests load the local build through `LD_LIBRARY_PATH` instead of downloading
a release. They cover the socket patterns over TCP and inproc, async wakes
and timeouts, HWM backpressure, multipart edge cases, the `messages` stream,
isolate lifetime, PLAIN, CURVE, monitors, and the build hook. CI runs them
on Linux with Dart 3.10.0 and stable.

## Prebuilt library pin

`lib/src/hook/libomq_zmq.dart` names the omq-libzmq release whose prebuilt
libraries the hook downloads, with their SHA-256 sums. Regenerate it from a
release's `SHA256SUMS`:

```sh
ruby bindings/dart/scripts/pin_libomq_zmq.rb 0.11.2
```

## Benchmarks

Run the two-process TCP comparison against dartzmq, then redraw the chart:

```sh
OMQ_BENCH_TASKSET=1 ruby bindings/dart/scripts/update_perf.rb
```

The script builds `omq-libzmq`, downloads dartzmq from pub.dev (checked by
SHA-256) with the libzmq it bundles, and AOT-builds `bench/peer.dart` in
`~/.cache/omq.dart/bench`. It runs on Linux. `OMQ_DART` selects the Dart
command. `OMQ_BENCH_TASKSET=1` pins the throughput peers; latency runs
unpinned. Results append to `~/.cache/omq.dart/bindings.jsonl`. Use `--quick`
for a short check without writes, and `--chart-only` to redraw the chart.
