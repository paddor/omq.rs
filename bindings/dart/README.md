# OMQ.dart

Dart binding for OMQ, backed by the `omq-libzmq` C ABI. It is pure Dart:
`@Native` calls with no native shim. Async waits run on the native ready
callback, so a pending `receive()` holds no thread.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/dart/doc/charts/bindings.svg" alt="OMQ.dart performance" width="850">
</p>

2-process loopback TCP comparison against dartzmq on its bundled libzmq.
Throughput covers 16 B to 32 KiB; REQ/REP p50 latency covers 16 B to 4 KiB.
See [`DEVELOPMENT.md`](DEVELOPMENT.md) for benchmark and regeneration
commands.

## Install

Depend on the package from Git:

```yaml
dependencies:
  omq:
    git:
      url: https://github.com/paddor/omq.rs
      path: bindings/dart
```

Build the native library:

```sh
cargo build --release -p omq-libzmq
```

The build hook loads `libomq_zmq` through the system loader
(`LD_LIBRARY_PATH`, rpath, or system paths). To bundle a prebuilt file
instead, set the `library` user-define in the app's `pubspec.yaml`:

```yaml
hooks:
  user_defines:
    omq:
      library: /path/to/libomq_zmq.so
```

`dart run` loads that file, and `dart build cli` copies it into the bundle.
Requires Dart 3.10 or later.

## API

```dart
import 'package:omq/omq.dart';

Future<void> main() async {
  final context = Context();
  final pull = context.socket(SocketType.pull);
  final push = context.socket(SocketType.push);
  push.connect(pull.bind('tcp://127.0.0.1:*'));
  await push.send(Message.text('hello'));
  print((await pull.receive()).text);
  context.close();
}
```

- Sends and receives come in three forms: `trySend`/`tryReceive` never
  block, `sendSync`/`receiveSync` block the isolate, and `send`/`receive`
  return a `Future`. A `receive(timeout:)` that times out takes no message.
- `socket.messages` is a stream. Pausing it leaves messages queued, so the
  HWM applies backpressure.
- `Message` holds one or more frames, a `routingId` for SERVER and PEER, and
  a `group` for RADIO and DISH.
- `SocketOptions` applies options at creation; `setInt`, `setBytes`, and the
  `SocketOption` constants cover the rest.
- `configurePlainServer`, `configurePlainClient`, `configureCurveServer`, and
  `configureCurveClient` set up security; `CurveKeyPair.generate()` makes
  keys. PLAIN authenticates but does not encrypt.
- `socket.monitor()` reports connection events.
- `Context.shareKey` and `Context.fromShareKey` let isolates share one native
  context and its `inproc://` endpoints.

Use a socket from the isolate that created it, and close the context before
that isolate exits.

## Development

Architecture: [`doc/architecture.md`](doc/architecture.md).

Build and test instructions: [`DEVELOPMENT.md`](DEVELOPMENT.md).
