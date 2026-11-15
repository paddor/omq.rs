part of 'core.dart';

/// Socket monitor event types (`ZMQ_EVENT_*`).
enum MonitorEventType {
  /// A connection was established.
  connected(0x0001),

  /// A connect is pending.
  connectDelayed(0x0002),

  /// A reconnect is scheduled; the value is the delay in ms.
  connectRetried(0x0004),

  /// A bind succeeded.
  listening(0x0008),

  /// A bind failed.
  bindFailed(0x0010),

  /// A connection was accepted.
  accepted(0x0020),

  /// Accepting a connection failed.
  acceptFailed(0x0040),

  /// A connection was closed.
  closed(0x0080),

  /// Closing a connection failed.
  closeFailed(0x0100),

  /// A peer disconnected.
  disconnected(0x0200),

  /// The monitor stopped.
  monitorStopped(0x0400),

  /// A handshake failed without details.
  handshakeFailedNoDetail(0x0800),

  /// A handshake succeeded.
  handshakeSucceeded(0x1000),

  /// A handshake failed on a protocol error.
  handshakeFailedProtocol(0x2000),

  /// A handshake failed authentication.
  handshakeFailedAuth(0x4000);

  const MonitorEventType(this.value);

  /// The native event bit.
  final int value;

  /// The event type for a native [value], or null.
  static MonitorEventType? fromValue(int value) {
    for (final type in values) {
      if (type.value == value) return type;
    }
    return null;
  }
}

/// One monitor event.
final class MonitorEvent {
  /// Creates an event.
  const MonitorEvent(this.type, this.value, this.endpoint);

  /// The event type, or null for an event this binding does not know.
  final MonitorEventType? type;

  /// The event value: an fd, errno, or delay, depending on [type].
  final int value;

  /// The endpoint the event belongs to.
  final String endpoint;

  @override
  String toString() => 'MonitorEvent(${type?.name ?? 'unknown'}, $value, $endpoint)';
}

int _nextMonitor = 1;

/// Reads the native monitor of a socket. Close it before the socket.
final class Monitor {
  Monitor._(this.socket, Iterable<MonitorEventType>? events) {
    final handle = socket._require('socket_monitor');
    final mask = events == null ? 0xffff : events.fold(0, (mask, type) => mask | type.value);
    _endpoint = 'inproc://omq-dart-monitor-${handle.address.toRadixString(16)}-${_nextMonitor++}';
    using((arena) {
      final endpoint = _endpoint.toNativeUtf8(allocator: arena);
      check(zmq_socket_monitor(handle, endpoint, mask), 'socket_monitor');
    });
    try {
      _reader = socket.context.socket(SocketType.pair, const SocketOptions(linger: Duration.zero));
      _reader.connect(_endpoint);
    } catch (_) {
      zmq_socket_monitor(handle, nullptr, 0);
      rethrow;
    }
  }

  /// The monitored socket.
  final Socket socket;

  late final String _endpoint;
  late final Socket _reader;

  /// The next event if one is queued; null otherwise.
  MonitorEvent? tryReceive() => _reader.tryReceive().let(_parse);

  /// The next event. With [timeout], fails with a [TimeoutException].
  Future<MonitorEvent> receive({Duration? timeout}) async =>
      _parse(await _reader.receive(timeout: timeout));

  /// Events as they arrive; the stream ends when the monitor closes.
  Stream<MonitorEvent> get events => _reader.messages.map(_parse);

  /// Stops monitoring and closes the reader. Idempotent.
  void close() {
    if (_reader.isClosed) return;
    if (!socket.isClosed) zmq_socket_monitor(socket._handle, nullptr, 0);
    _reader.close();
  }

  static MonitorEvent _parse(Message message) {
    final head = ByteData.sublistView(message.frames[0]);
    final endpoint = message.frames.length > 1 ? utf8.decode(message.frames[1]) : '';
    return MonitorEvent(
      MonitorEventType.fromValue(head.getUint16(0, Endian.host)),
      head.getUint32(2, Endian.host),
      endpoint,
    );
  }
}

extension on Message? {
  MonitorEvent? let(MonitorEvent Function(Message) parse) {
    final message = this;
    return message == null ? null : parse(message);
  }
}
