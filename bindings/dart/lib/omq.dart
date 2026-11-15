/// OMQ for Dart: ZeroMQ-compatible sockets on the OMQ engine.
///
/// ```dart
/// final context = Context();
/// final pull = context.socket(SocketType.pull);
/// final push = context.socket(SocketType.push);
/// push.connect(pull.bind('tcp://127.0.0.1:*'));
/// await push.send(Message.text('hello'));
/// print((await pull.receive()).text);
/// context.close();
/// ```
library;

export 'src/core.dart' show Context, Monitor, MonitorEvent, MonitorEventType, ShareKey, Socket;
export 'src/errors.dart' show OmqClosedException, OmqException;
export 'src/message.dart' show Message;
export 'src/options.dart' show ContextOption, SocketOption, SocketOptions, SocketType;
export 'src/security.dart' show CurveKeyPair, PlainCredential, hasCapability, zmqVersion;
