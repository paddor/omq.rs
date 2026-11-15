import 'dart:async';
import 'dart:isolate';

import 'package:omq/omq.dart';
import 'package:test/test.dart';

import 'helpers.dart';

void main() {
  for (final transport in ['tcp', 'inproc']) {
    String endpoint() => transport == 'tcp' ? 'tcp://127.0.0.1:*' : inproc('async');

    group(transport, () {
      test('a pending receive wakes when a message arrives', () async {
        final (pull, push) = pair(
          testContext(),
          SocketType.pull,
          SocketType.push,
          endpoint: endpoint(),
        );
        final received = pull.receive(timeout: wait);
        await Future<void>.delayed(const Duration(milliseconds: 20));
        push.sendSync(Message.text('late'));
        expect((await received).text, 'late');
      });

      test('req/rep round trips through async calls', () async {
        final (rep, req) = pair(
          testContext(),
          SocketType.rep,
          SocketType.req,
          endpoint: endpoint(),
        );
        final server = () async {
          for (var index = 0; index < 100; index++) {
            final request = await rep.receive(timeout: wait);
            await rep.send(request);
          }
        }();
        for (var index = 0; index < 100; index++) {
          await req.send(Message.text('$index'));
          expect((await req.receive(timeout: wait)).text, '$index');
        }
        await server;
      });

      test('a send blocked on HWM resumes when the peer reads', () async {
        final (pull, push) = pair(
          testContext(),
          SocketType.pull,
          SocketType.push,
          endpoint: endpoint(),
          serverOptions: const SocketOptions(linger: Duration.zero, receiveHwm: 4),
          clientOptions: const SocketOptions(linger: Duration.zero, sendHwm: 4),
        );
        var sent = 0;
        final sender = () async {
          for (; sent < 2000; sent++) {
            await push.send(Message.text('$sent'));
          }
        }();
        for (var index = 0; index < 2000; index++) {
          expect((await pull.receive(timeout: wait)).text, '$index');
        }
        await sender.timeout(wait);
        expect(sent, 2000);
      });
    });
  }

  test('a send on a bind-side push resumes when a peer connects', () async {
    final context = testContext();
    final push = context.socket(SocketType.push, quick);
    final endpoint = push.bind('tcp://127.0.0.1:*');
    final sent = push.send(Message.text('first'));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    final pull = context.socket(SocketType.pull, quick)..connect(endpoint);
    await sent.timeout(wait);
    expect((await pull.receive(timeout: wait)).text, 'first');
  });

  test('concurrent receives each get one message', () async {
    final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
    final pending = [for (var index = 0; index < 10; index++) pull.receive(timeout: wait)];
    for (var index = 0; index < 10; index++) {
      await push.send(Message.text('$index'));
    }
    final texts = [for (final message in await Future.wait(pending)) message.text];
    expect(texts.toSet(), {for (var index = 0; index < 10; index++) '$index'});
  });

  test('a receive timeout takes no message', () async {
    final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
    await expectLater(
      pull.receive(timeout: const Duration(milliseconds: 20)),
      throwsA(isA<TimeoutException>()),
    );
    await push.send(Message.text('kept'));
    expect((await pull.receive(timeout: wait)).text, 'kept');
  });

  test('closing a socket fails its pending receive', () async {
    final socket = testContext().socket(SocketType.pull, quick)..bind('tcp://127.0.0.1:*');
    final received = socket.receive();
    await Future<void>.delayed(const Duration(milliseconds: 10));
    socket.close();
    await expectLater(received, throwsA(isA<OmqClosedException>()));
  });

  test('closing the context fails pending receives', () async {
    final context = Context();
    final socket = context.socket(SocketType.pull, quick)..bind('tcp://127.0.0.1:*');
    final received = socket.receive();
    await Future<void>.delayed(const Duration(milliseconds: 10));
    context.close();
    await expectLater(received, throwsA(isA<OmqClosedException>()));
  });

  group('messages stream', () {
    test('delivers messages in order and stops on cancel', () async {
      final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
      for (var index = 0; index < 1000; index++) {
        await push.send(Message.text('$index'));
      }
      final texts = await pull.messages.take(1000).map((message) => message.text).toList();
      expect(texts, [for (var index = 0; index < 1000; index++) '$index']);
      await push.send(Message.text('after'));
      expect((await pull.receive(timeout: wait)).text, 'after');
    });

    test('pausing leaves messages queued', () async {
      final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
      final seen = <String>[];
      late final StreamSubscription<Message> subscription;
      final first = Completer<void>();
      subscription = pull.messages.listen((message) {
        seen.add(message.text);
        subscription.pause();
        first.complete();
      });
      await push.send(Message.text('a'));
      await push.send(Message.text('b'));
      await first.future.timeout(wait);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(seen, ['a']);
      await subscription.cancel();
      expect((await pull.receive(timeout: wait)).text, 'b');
    });

    test('ends when the socket closes', () async {
      final socket = testContext().socket(SocketType.pull, quick)..bind('tcp://127.0.0.1:*');
      final done = socket.messages.toList();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      socket.close();
      expect(await done.timeout(wait), isEmpty);
    });
  });

  test('an isolate with an idle socket exits when nothing waits', () async {
    // A registered callback must not keep the isolate alive by itself.
    final exited = ReceivePort();
    await Isolate.spawn(_idleIsolate, null, onExit: exited.sendPort);
    await exited.first.timeout(wait);
  });

  test('a pending receive keeps its isolate alive until a message arrives', () async {
    final results = ReceivePort();
    final exited = ReceivePort();
    final context = testContext();
    final pull = context.socket(SocketType.pull, quick);
    final endpoint = pull.bind(inproc('alive'));
    await Isolate.spawn(_waitingIsolate, (
      context.shareKey,
      endpoint,
      results.sendPort,
    ), onExit: exited.sendPort);
    final messages = results.asBroadcastStream();
    expect(await messages.first.timeout(wait), 'waiting');
    // Time for the isolate to exit if the wait did not keep it alive.
    await Future<void>.delayed(const Duration(milliseconds: 100));
    final push = context.socket(SocketType.push, quick)..connect('$endpoint-reply');
    await push.send(Message.text('wake'));
    expect(await messages.first.timeout(wait), 'wake');
    await exited.first.timeout(wait);
    results.close();
  });
}

Future<void> _idleIsolate(void _) async {
  final context = Context();
  final socket = context.socket(SocketType.pull, quick)..bind('tcp://127.0.0.1:*');
  await expectLater(
    socket.receive(timeout: const Duration(milliseconds: 10)),
    throwsA(isA<TimeoutException>()),
  );
  context.close();
}

Future<void> _waitingIsolate((ShareKey, String, SendPort) args) async {
  final (key, endpoint, results) = args;
  final context = Context.fromShareKey(key);
  final pull = context.socket(SocketType.pull, quick)..bind('$endpoint-reply');
  results.send('waiting');
  final message = await pull.receive();
  results.send(message.text);
  context.close();
}
