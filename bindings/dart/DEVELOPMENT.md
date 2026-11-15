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
