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

The tests cover the socket patterns over TCP and inproc, async wakes and
timeouts, HWM backpressure, multipart edge cases, the `messages` stream,
isolate lifetime, PLAIN, CURVE, and monitors. CI runs them on Linux with
Dart 3.10.0 and stable.

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
