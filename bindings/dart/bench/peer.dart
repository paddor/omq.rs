// Benchmark peer for scripts/update_perf.rb, which builds it in a scratch
// package next to a downloaded dartzmq.
//
// usage: peer <impl> <pushpull|reqrep> <role> <endpoint> <size> <duration> <warmup>
//
// impl: omq (sync calls), omq-async (Futures), or dartzmq (on the libzmq it
// ships). The server prints ENDPOINT once bound; the measuring side prints
// RESULT {json} and exits.
import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartzmq/dartzmq.dart' as dz;
import 'package:ffi/ffi.dart';
import 'package:omq/omq.dart' as omq;

final _clock = Stopwatch()..start();
final double _tick = 1 / _clock.frequency;
double now() => _clock.elapsedTicks * _tick;

/// Throughput loops check the clock once per batch.
const batch = 64;

late final String impl;
late final String pattern;
late final String role;
late final String endpoint;
late final int size;
late final double duration;
late final double warmup;
late final Uint8List payload;

bool get isServer => role == 'pull' || role == 'rep';

void check(int length) {
  if (length == size) return;
  stderr.writeln('bad message size $length');
  exit(1);
}

Never finish(Map<String, Object?> result, String version) {
  final row = {'impl': impl, 'pattern': pattern, 'size': size, 'library_version': version};
  stdout.writeln('RESULT ${jsonEncode({...row, ...result})}');
  exit(0);
}

Never throughput(int count, double elapsed, String version) =>
    finish({'count': count, 'elapsed': elapsed, 'msgs_s': count / elapsed}, version);

Never latency(List<double> samples, String version) {
  samples.sort();
  final n = samples.length;
  finish({
    'count': n,
    'p50_us': samples[n ~/ 2],
    'p99_us': samples[(n * 99 ~/ 100).clamp(0, n - 1)],
  }, version);
}

Future<void> main(List<String> args) async {
  impl = args[0];
  pattern = args[1];
  role = args[2];
  endpoint = args[3];
  size = int.parse(args[4]);
  duration = double.parse(args[5]);
  warmup = double.parse(args[6]);
  payload = Uint8List(size)..fillRange(0, size, 0x78);
  switch (impl) {
    case 'omq':
      runOmqSync();
    case 'omq-async':
      await runOmqAsync();
    case 'dartzmq':
      await runDartzmq();
    default:
      throw ArgumentError('unknown impl $impl');
  }
}

// OMQ.dart.

omq.Socket omqSocket() {
  final type = switch (role) {
    'pull' => omq.SocketType.pull,
    'push' => omq.SocketType.push,
    'rep' => omq.SocketType.rep,
    _ => omq.SocketType.req,
  };
  final socket = omq.Context().socket(type, const omq.SocketOptions(linger: Duration.zero));
  if (isServer) {
    stdout.writeln('ENDPOINT ${socket.bind(endpoint)}');
  } else {
    socket.connect(endpoint);
  }
  return socket;
}

String omqVersion() {
  final version = omq.zmqVersion();
  return 'libomq_zmq ${version.major}.${version.minor}.${version.patch}';
}

void runOmqSync() {
  final socket = omqSocket();
  final message = omq.Message(payload);
  final version = omqVersion();
  switch (role) {
    case 'push':
      while (true) {
        socket.sendSync(message);
      }
    case 'rep':
      while (true) {
        socket.sendSync(socket.receiveSync());
      }
    case 'pull':
      pullSync(() {
        for (var i = 0; i < batch; i++) {
          check(socket.receiveSync().data.length);
        }
      }, version);
    case 'req':
      reqSync(() {
        socket.sendSync(message);
        check(socket.receiveSync().data.length);
      }, version);
  }
}

Future<void> runOmqAsync() async {
  final socket = omqSocket();
  final message = omq.Message(payload);
  final version = omqVersion();
  switch (role) {
    case 'push':
      while (true) {
        await socket.send(message);
      }
    case 'rep':
      while (true) {
        await socket.send(await socket.receive());
      }
    case 'pull':
      await pullAsync(() async {
        for (var i = 0; i < batch; i++) {
          check((await socket.receive()).data.length);
        }
      }, version);
    case 'req':
      await reqAsync(() async {
        await socket.send(message);
        check((await socket.receive()).data.length);
      }, version);
  }
}

// dartzmq: its Stream receive drains on a 1 s timer; REQ/REP uses its
// blocking sync socket.

Future<void> runDartzmq() async {
  final context = dz.ZContext();
  final version = libzmqVersion();
  final type = switch (role) {
    'pull' => dz.SocketType.pull,
    'push' => dz.SocketType.push,
    'rep' => dz.SocketType.rep,
    _ => dz.SocketType.req,
  };
  if (pattern == 'reqrep') {
    final socket = context.createdSynSocket(type);
    bindOrConnect(socket.bind, socket.connect);
    if (role == 'rep') {
      while (true) {
        socket.send(socket.recv().first.payload);
      }
    }
    reqSync(() {
      socket.send(payload);
      check(socket.recv().first.payload.length);
    }, version);
  }
  final socket = context.createSocket(type);
  bindOrConnect(socket.bind, socket.connect);
  if (role == 'push') {
    while (true) {
      socket.send(payload);
    }
  }
  final warmupEnd = now() + warmup;
  double? started;
  var count = 0;
  socket.payloads.listen((message) {
    check(message.length);
    final start = started;
    if (start == null) {
      if (now() >= warmupEnd) started = now();
      return;
    }
    if (++count % batch != 0) return;
    final elapsed = now() - start;
    if (elapsed >= duration) throughput(count, elapsed, version);
  });
  await Completer<void>().future;
}

void bindOrConnect(void Function(String) bind, void Function(String) connect) {
  if (isServer) {
    bind(endpoint);
    stdout.writeln('ENDPOINT $endpoint');
  } else {
    connect(endpoint);
  }
}

typedef _VersionC = Void Function(Pointer<Int>, Pointer<Int>, Pointer<Int>);
typedef _Version = void Function(Pointer<Int>, Pointer<Int>, Pointer<Int>);

/// dartzmq does not expose the version; ask the libzmq it loaded.
String libzmqVersion() => using((arena) {
  final library = DynamicLibrary.open('libzmq.so');
  final version = library.lookupFunction<_VersionC, _Version>('zmq_version');
  final parts = arena<Int>(3);
  version(parts, parts + 1, parts + 2);
  return 'libzmq ${parts[0]}.${parts[1]}.${parts[2]}';
});

// Measurement loops.

Never pullSync(void Function() receiveBatch, String version) {
  var deadline = now() + warmup;
  while (now() < deadline) {
    receiveBatch();
  }
  var count = 0;
  final started = now();
  deadline = started + duration;
  do {
    receiveBatch();
    count += batch;
  } while (now() < deadline);
  throughput(count, now() - started, version);
}

Future<Never> pullAsync(Future<void> Function() receiveBatch, String version) async {
  var deadline = now() + warmup;
  while (now() < deadline) {
    await receiveBatch();
  }
  var count = 0;
  final started = now();
  deadline = started + duration;
  do {
    await receiveBatch();
    count += batch;
  } while (now() < deadline);
  throughput(count, now() - started, version);
}

Never reqSync(void Function() roundTrip, String version) {
  var deadline = now() + warmup;
  while (now() < deadline) {
    roundTrip();
  }
  final samples = <double>[];
  deadline = now() + duration;
  double t1;
  do {
    final t0 = now();
    roundTrip();
    t1 = now();
    samples.add((t1 - t0) * 1e6);
  } while (t1 < deadline);
  latency(samples, version);
}

Future<Never> reqAsync(Future<void> Function() roundTrip, String version) async {
  var deadline = now() + warmup;
  while (now() < deadline) {
    await roundTrip();
  }
  final samples = <double>[];
  deadline = now() + duration;
  double t1;
  do {
    final t0 = now();
    await roundTrip();
    t1 = now();
    samples.add((t1 - t0) * 1e6);
  } while (t1 < deadline);
  latency(samples, version);
}
