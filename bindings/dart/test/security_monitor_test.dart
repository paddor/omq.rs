import 'package:omq/omq.dart';
import 'package:test/test.dart';

import 'helpers.dart';

void main() {
  group('curve', () {
    test('key pairs round trip through the secret key', () {
      final keys = CurveKeyPair.generate();
      expect(keys.publicKey, hasLength(40));
      expect(keys.secretKey, hasLength(40));
      expect(CurveKeyPair.fromSecretKey(keys.secretKey).publicKey, keys.publicKey);
    });

    test('an encrypted connection delivers messages', () async {
      final context = testContext();
      final serverKeys = CurveKeyPair.generate();
      final server = context.socket(SocketType.pull, quick)
        ..configureCurveServer(serverKeys.secretKey);
      final client = context.socket(SocketType.push, quick)
        ..configureCurveClient(CurveKeyPair.generate(), serverKey: serverKeys.publicKey);
      client.connect(server.bind('tcp://127.0.0.1:*'));
      await client.send(Message.text('secret'));
      expect((await server.receive(timeout: wait)).text, 'secret');
    });
  });

  group('plain', () {
    test('the allowlist admits listed clients only', () async {
      final context = testContext();
      final server = context.socket(SocketType.pull, quick)
        ..configurePlainServer([const PlainCredential('alice', 'secret')]);
      final endpoint = server.bind('tcp://127.0.0.1:*');

      final denied = context.socket(SocketType.push, quick)
        ..configurePlainClient('mallory', 'guess')
        ..connect(endpoint);
      final allowed = context.socket(SocketType.push, quick)
        ..configurePlainClient('alice', 'secret')
        ..connect(endpoint);
      denied.trySend(Message.text('denied'));
      await allowed.send(Message.text('allowed'));
      expect((await server.receive(timeout: wait)).text, 'allowed');
      await expectLater(
        server.receive(timeout: const Duration(milliseconds: 100)),
        throwsA(anything),
      );
    });
  });

  group('monitor', () {
    test('reports listening, accepted, and handshake events', () async {
      final context = testContext();
      final server = context.socket(SocketType.pull, quick);
      final monitor = server.monitor();
      addTearDown(monitor.close);
      final endpoint = server.bind('tcp://127.0.0.1:*');
      context.socket(SocketType.push, quick).connect(endpoint);

      final seen = <MonitorEventType?>{};
      while (!seen.contains(MonitorEventType.handshakeSucceeded)) {
        final event = await monitor.receive(timeout: wait);
        seen.add(event.type);
        if (event.type == MonitorEventType.listening) expect(event.endpoint, endpoint);
      }
      expect(seen, containsAll([MonitorEventType.listening, MonitorEventType.accepted]));
    });

    test('filters events by type', () async {
      final context = testContext();
      final client = context.socket(SocketType.push, quick);
      final monitor = client.monitor(events: [MonitorEventType.connected]);
      addTearDown(monitor.close);
      final server = context.socket(SocketType.pull, quick);
      client.connect(server.bind('tcp://127.0.0.1:*'));
      final event = await monitor.receive(timeout: wait);
      expect(event.type, MonitorEventType.connected);
    });
  });
}
