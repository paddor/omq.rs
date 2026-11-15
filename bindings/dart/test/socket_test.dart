import 'dart:async';
import 'dart:typed_data';

import 'package:omq/omq.dart';
import 'package:test/test.dart';

import 'helpers.dart';

void main() {
  group('sync', () {
    test('push/pull over tcp', () {
      final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
      push.sendSync(Message.text('hello'));
      expect(pull.receiveSync().text, 'hello');
    });

    test('req/rep round trip', () {
      final (rep, req) = pair(testContext(), SocketType.rep, SocketType.req);
      req.sendSync(Message.text('ping'));
      expect(rep.receiveSync().text, 'ping');
      rep.sendSync(Message.text('pong'));
      expect(req.receiveSync().text, 'pong');
    });

    test('tryReceive returns null when nothing is queued', () {
      final (pull, _) = pair(testContext(), SocketType.pull, SocketType.push);
      expect(pull.tryReceive(), isNull);
    });

    test('receive timeout throws TimeoutException', () {
      final context = testContext();
      final pull = context.socket(
        SocketType.pull,
        const SocketOptions(linger: Duration.zero, receiveTimeout: Duration(milliseconds: 20)),
      );
      pull.bind('tcp://127.0.0.1:*');
      expect(pull.receiveSync, throwsA(isA<TimeoutException>()));
    });

    test('bind-side push without a peer does not accept sends', () {
      final context = testContext();
      final push = context.socket(SocketType.push, quick);
      push.bind('tcp://127.0.0.1:*');
      expect(push.trySend(Message.text('x')), isFalse);
    });

    for (final transport in ['tcp', 'inproc']) {
      test('empty and large frames keep their bytes over $transport', () {
        final (pull, push) = pair(
          testContext(),
          SocketType.pull,
          SocketType.push,
          endpoint: transport == 'tcp' ? 'tcp://127.0.0.1:*' : inproc('large'),
        );
        final large = Uint8List(100000);
        for (var index = 0; index < large.length; index++) {
          large[index] = index & 0xff;
        }
        push.sendSync(Message(Uint8List(0)));
        push.sendSync(Message(large));
        expect(push.trySend(Message(large)), isTrue);
        expect(pull.receiveSync().data, isEmpty);
        expect(pull.receiveSync().data, large);
        expect(pull.receiveSync().data, large);
      });
    }
  });

  group('multipart', () {
    test('router/dealer keeps frames and identity', () {
      final (router, dealer) = pair(
        testContext(),
        SocketType.router,
        SocketType.dealer,
        clientOptions: SocketOptions(linger: Duration.zero, routingId: bytes('d1')),
      );
      dealer.sendSync(Message.multipart([bytes('a'), Uint8List(0), bytes('c')]));
      final request = router.receiveSync();
      expect(request.frames, [bytes('d1'), bytes('a'), Uint8List(0), bytes('c')]);
      router.sendSync(Message.multipart([request.frames[0], bytes('reply')]));
      expect(dealer.receiveSync().text, 'reply');
    });

    test('frames larger than the receive buffer keep their place', () {
      final (pull, push) = pair(testContext(), SocketType.pull, SocketType.push);
      final big = Uint8List(40000)..fillRange(0, 40000, 7);
      push.sendSync(Message.multipart([bytes('head'), big, bytes('tail')]));
      expect(pull.receiveSync().frames, [bytes('head'), big, bytes('tail')]);
    });

    test('a would-block final frame is finished, not glued to the next message', () async {
      // ROUTER checks the peer only when the whole message is submitted, so
      // a full peer blocks the final frame of [identity, body].
      final context = testContext();
      final router = context.socket(
        SocketType.router,
        const SocketOptions(linger: Duration.zero, routerMandatory: true, sendHwm: 1),
      );
      final dealer = context.socket(
        SocketType.dealer,
        SocketOptions(linger: Duration.zero, routingId: bytes('d'), receiveHwm: 1),
      );
      dealer.connect(router.bind(inproc('tail')));
      dealer.sendSync(Message.text('hi'));
      router.receiveSync();

      var accepted = 0;
      while (router.trySend(Message.multipart([bytes('d'), bytes('m$accepted')]))) {
        accepted++;
        if (accepted > 100000) fail('router never blocked');
      }
      final last = router.send(Message.multipart([bytes('d'), bytes('m$accepted')]));
      accepted++;

      for (var index = 0; index < accepted; index++) {
        final message = await dealer.receive(timeout: wait);
        expect(message.frames, [bytes('m$index')]);
      }
      await last.timeout(wait);
      router.sendSync(Message.multipart([bytes('d'), bytes('end')]));
      expect((await dealer.receive(timeout: wait)).text, 'end');
    });
  });

  group('socket types', () {
    test('pub/sub filters by prefix', () async {
      final (pub, sub) = pair(testContext(), SocketType.pub, SocketType.sub);
      sub.subscribe('a.');
      // Subscriptions propagate asynchronously; publish until one arrives.
      final first = sub.receive(timeout: wait);
      final timer = Timer.periodic(const Duration(milliseconds: 5), (_) {
        pub.trySend(Message.text('b.skip'));
        pub.trySend(Message.text('a.keep'));
      });
      try {
        expect((await first).text, 'a.keep');
      } finally {
        timer.cancel();
      }
    });

    test('server/client route by routing id', () async {
      final (server, client) = pair(testContext(), SocketType.server, SocketType.client);
      await client.send(Message.text('hi'));
      final request = await server.receive(timeout: wait);
      expect(request.routingId, isNot(0));
      await server.send(Message.text('back', routingId: request.routingId));
      expect((await client.receive(timeout: wait)).text, 'back');
    });

    test('radio/dish deliver joined groups', () async {
      final context = testContext();
      final dish = context.socket(SocketType.dish, quick);
      final radio = context.socket(SocketType.radio, quick);
      final endpoint = dish.bind(inproc('radio'));
      dish.join('weather');
      radio.connect(endpoint);
      final received = dish.receive(timeout: wait);
      final timer = Timer.periodic(const Duration(milliseconds: 5), (_) {
        radio.trySend(Message.text('rain', group: 'weather'));
      });
      try {
        final message = await received;
        expect(message.text, 'rain');
        expect(message.group, 'weather');
      } finally {
        timer.cancel();
      }
    });

    test('pair, channel, scatter/gather exchange messages', () async {
      final context = testContext();
      for (final (server, client) in [
        (SocketType.pair, SocketType.pair),
        (SocketType.channel, SocketType.channel),
        (SocketType.gather, SocketType.scatter),
      ]) {
        final (bound, connected) = pair(context, server, client, endpoint: inproc(server.name));
        await connected.send(Message.text(server.name));
        expect((await bound.receive(timeout: wait)).text, server.name);
      }
    });
  });

  group('options', () {
    test('typed getters read back setters', () {
      final socket = testContext().socket(SocketType.dealer, quick);
      socket.setInt(SocketOption.sendHwm, 123);
      expect(socket.getInt(SocketOption.sendHwm), 123);
      socket.setInt64(SocketOption.maxMessageSize, 1 << 20);
      expect(socket.getInt64(SocketOption.maxMessageSize), 1 << 20);
      socket.setBytes(SocketOption.routingId, bytes('me'));
      expect(socket.getBytes(SocketOption.routingId), bytes('me'));
      expect(socket.getInt(SocketOption.type), SocketType.dealer.value);
    });

    test('socket options apply at creation', () {
      final socket = testContext().socket(
        SocketType.router,
        const SocketOptions(linger: Duration(milliseconds: 7), receiveHwm: 9, immediate: true),
      );
      expect(socket.getInt(SocketOption.linger), 7);
      expect(socket.getInt(SocketOption.receiveHwm), 9);
      expect(socket.getInt(SocketOption.immediate), 1);
    });

    test('bind resolves a wildcard port', () {
      final socket = testContext().socket(SocketType.pull, quick);
      expect(socket.bind('tcp://127.0.0.1:*'), matches(RegExp(r'^tcp://127\.0\.0\.1:\d+$')));
    });

    test('native errors carry errno and text', () {
      final socket = testContext().socket(SocketType.pull, quick);
      expect(
        () => socket.connect('bogus://nowhere'),
        throwsA(isA<OmqException>().having((e) => e.message, 'message', isNotEmpty)),
      );
    });
  });

  group('lifecycle', () {
    test('closed sockets throw OmqClosedException', () {
      final socket = testContext().socket(SocketType.pull, quick);
      socket.close();
      socket.close();
      expect(socket.isClosed, isTrue);
      expect(socket.tryReceive, throwsA(isA<OmqClosedException>()));
    });

    test('context close closes its sockets', () {
      final context = Context();
      final socket = context.socket(SocketType.pull, quick);
      context.close();
      context.close();
      expect(socket.isClosed, isTrue);
      expect(() => context.socket(SocketType.push), throwsA(isA<OmqClosedException>()));
    });

    test('a shared context shares inproc endpoints', () async {
      final context = testContext();
      final joined = Context.fromShareKey(context.shareKey);
      addTearDown(joined.close);
      final pull = context.socket(SocketType.pull, quick);
      final push = joined.socket(SocketType.push, quick);
      final endpoint = inproc('shared');
      pull.bind(endpoint);
      push.connect(endpoint);
      await push.send(Message.text('across'));
      expect((await pull.receive(timeout: wait)).text, 'across');
    });

    test('version and capabilities', () {
      expect(zmqVersion().major, 4);
      expect(hasCapability('tcp'), isTrue);
    });
  });
}
