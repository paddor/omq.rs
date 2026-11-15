import 'dart:convert';
import 'dart:typed_data';

import 'package:omq/omq.dart';
import 'package:test/test.dart';

const quick = SocketOptions(linger: Duration.zero);

/// A context closed after the current test.
Context testContext() {
  final context = Context();
  addTearDown(context.close);
  return context;
}

/// A bound and a connected socket of [server] and [client] type.
(Socket, Socket) pair(
  Context context,
  SocketType server,
  SocketType client, {
  String endpoint = 'tcp://127.0.0.1:*',
  SocketOptions serverOptions = quick,
  SocketOptions clientOptions = quick,
}) {
  final bound = context.socket(server, serverOptions);
  final connected = context.socket(client, clientOptions);
  connected.connect(bound.bind(endpoint));
  return (bound, connected);
}

/// A unique inproc endpoint.
String inproc(String name) => 'inproc://$name-${DateTime.now().microsecondsSinceEpoch}';

Uint8List bytes(String text) => utf8.encode(text);

const wait = Duration(seconds: 5);
