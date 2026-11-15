part of 'core.dart';

/// An OMQ socket. Create one with [Context.socket]; use it from the isolate
/// that created it.
///
/// Each operation has three forms: `try*` never blocks, `*Sync` blocks the
/// isolate (honoring send and receive timeouts), and the plain form returns
/// a [Future] that waits on the native ready callback without holding a
/// thread.
///
/// A multipart message is committed once its first frame is accepted. If
/// its final frame would block, the socket keeps that frame and finishes the
/// message before any later send.
final class Socket {
  Socket._(this.context, this.type, this._handle) : _dish = type == SocketType.dish;

  /// The owning context.
  final Context context;

  /// The socket type.
  final SocketType type;

  Pointer<Void> _handle;
  final Readiness _readiness = Readiness();
  final bool _dish;

  /// The final frame of a committed multipart message that would block.
  Uint8List? _tail;
  bool _flushing = false;

  /// Has an inproc endpoint, so sends may run a Dart ready callback inline
  /// and must not be leaf calls.
  bool _inproc = false;

  /// Whether [close] was called, directly or by [Context.close].
  bool get isClosed => _handle == nullptr;

  // Endpoints.

  /// Binds to [endpoint] and returns the bound endpoint, with any wildcard
  /// port resolved.
  String bind(String endpoint) {
    _endpointCall('bind', endpoint, zmq_bind);
    return lastEndpoint;
  }

  /// Connects to [endpoint]. Connecting before the peer binds is fine.
  void connect(String endpoint) => _endpointCall('connect', endpoint, zmq_connect);

  /// Stops listening on [endpoint].
  void unbind(String endpoint) => _endpointCall('unbind', endpoint, zmq_unbind);

  /// Drops the connection to [endpoint].
  void disconnect(String endpoint) => _endpointCall('disconnect', endpoint, zmq_disconnect);

  /// SUB: receives messages starting with [prefix]; empty means all.
  void subscribe([String prefix = '']) => subscribeBytes(utf8.encode(prefix));

  /// SUB: receives messages starting with [prefix].
  void subscribeBytes(Uint8List prefix) => setBytes(SocketOption.subscribe, prefix);

  /// SUB: drops one earlier [subscribe] of [prefix].
  void unsubscribe([String prefix = '']) => unsubscribeBytes(utf8.encode(prefix));

  /// SUB: drops one earlier [subscribeBytes] of [prefix].
  void unsubscribeBytes(Uint8List prefix) => setBytes(SocketOption.unsubscribe, prefix);

  /// DISH: receives messages of [group].
  void join(String group) => _endpointCall('join', group, zmq_join);

  /// DISH: stops receiving messages of [group].
  void leave(String group) => _endpointCall('leave', group, zmq_leave);

  void _endpointCall(
    String operation,
    String text,
    int Function(Pointer<Void>, Pointer<Utf8>) call,
  ) {
    final handle = _require(operation);
    using((arena) => check(call(handle, text.toNativeUtf8(allocator: arena)), operation));
    if (text.startsWith('inproc://')) _inproc = true;
  }

  // Options.

  /// The last bound or connected endpoint.
  String get lastEndpoint => getString(SocketOption.lastEndpoint);

  /// Reads a C `int` option ([SocketOption]).
  int getInt(int option) => _getInt(_require('getsockopt'), option);

  /// Reads an `int64` option ([SocketOption]).
  int getInt64(int option) {
    _optSize.value = sizeOf<Int64>();
    check(zmq_getsockopt(_require('getsockopt'), option, _optValue.cast(), _optSize), 'getsockopt');
    return _optValue.value;
  }

  /// Reads a binary option of at most [capacity] bytes ([SocketOption]).
  Uint8List getBytes(int option, {int capacity = 256}) => using((arena) {
    final value = arena<Uint8>(capacity);
    final size = arena<Size>()..value = capacity;
    check(zmq_getsockopt(_require('getsockopt'), option, value.cast(), size), 'getsockopt');
    return Uint8List.fromList(value.asTypedList(size.value));
  });

  /// Reads a string option of at most [capacity] bytes ([SocketOption]).
  String getString(int option, {int capacity = 256}) {
    final bytes = getBytes(option, capacity: capacity);
    final end = bytes.indexOf(0);
    return utf8.decode(end < 0 ? bytes : Uint8List.sublistView(bytes, 0, end));
  }

  /// Sets a C `int` option ([SocketOption]).
  void setInt(int option, int value) => using((arena) {
    final pointer = arena<Int>()..value = value;
    _setOption(option, pointer.cast(), sizeOf<Int>());
  });

  /// Sets an `int64` option ([SocketOption]).
  void setInt64(int option, int value) => using((arena) {
    final pointer = arena<Int64>()..value = value;
    _setOption(option, pointer.cast(), sizeOf<Int64>());
  });

  /// Sets a binary option ([SocketOption]).
  void setBytes(int option, Uint8List value) => using((arena) {
    final pointer = arena<Uint8>(value.length);
    pointer.asTypedList(value.length).setAll(0, value);
    _setOption(option, pointer.cast(), value.length);
  });

  /// Sets a string option ([SocketOption]).
  void setString(int option, String value) => setBytes(option, utf8.encode(value));

  void _setOption(int option, Pointer<Void> value, int size) =>
      check(zmq_setsockopt(_require('setsockopt'), option, value, size), 'setsockopt');

  void _apply(SocketOptions options) {
    void millis(int option, Duration? value) {
      if (value != null) setInt(option, value.inMilliseconds);
    }

    void number(int option, int? value) {
      if (value != null) setInt(option, value);
    }

    void flag(int option, bool? value) {
      if (value != null) setInt(option, value ? 1 : 0);
    }

    millis(SocketOption.linger, options.linger);
    number(SocketOption.sendHwm, options.sendHwm);
    number(SocketOption.receiveHwm, options.receiveHwm);
    millis(SocketOption.sendTimeout, options.sendTimeout);
    millis(SocketOption.receiveTimeout, options.receiveTimeout);
    if (options.routingId case final value?) setBytes(SocketOption.routingId, value);
    if (options.connectRoutingId case final value?) {
      setBytes(SocketOption.connectRoutingId, value);
    }
    millis(SocketOption.reconnectInterval, options.reconnectInterval);
    millis(SocketOption.reconnectIntervalMax, options.reconnectIntervalMax);
    millis(SocketOption.handshakeInterval, options.handshakeInterval);
    millis(SocketOption.connectTimeout, options.connectTimeout);
    millis(SocketOption.heartbeatInterval, options.heartbeatInterval);
    millis(SocketOption.heartbeatTtl, options.heartbeatTtl);
    millis(SocketOption.heartbeatTimeout, options.heartbeatTimeout);
    if (options.maxMessageSize case final value?) {
      setInt64(SocketOption.maxMessageSize, value);
    }
    number(SocketOption.backlog, options.backlog);
    flag(SocketOption.immediate, options.immediate);
    flag(SocketOption.ipv6, options.ipv6);
    flag(SocketOption.conflate, options.conflate);
    flag(SocketOption.routerMandatory, options.routerMandatory);
    flag(SocketOption.routerHandover, options.routerHandover);
    flag(SocketOption.probeRouter, options.probeRouter);
    flag(SocketOption.reqCorrelate, options.reqCorrelate);
    flag(SocketOption.reqRelaxed, options.reqRelaxed);
    flag(SocketOption.xpubVerbose, options.xpubVerbose);
    flag(SocketOption.xpubNoDrop, options.xpubNoDrop);
    flag(SocketOption.xpubManual, options.xpubManual);
    flag(SocketOption.invertMatching, options.invertMatching);
    if (options.zapDomain case final value?) setString(SocketOption.zapDomain, value);
  }

  // Security.

  /// PLAIN server with a fixed allowlist; an empty list denies everyone.
  /// Call before bind or connect.
  void configurePlainServer(Iterable<PlainCredential> credentials) {
    final handle = _require('set_plain_server_credentials');
    final list = credentials.toList();
    using((arena) {
      final array = arena<PlainCredentialStruct>(list.isEmpty ? 1 : list.length);
      for (final (index, credential) in list.indexed) {
        final username = utf8.encode(credential.username);
        final password = utf8.encode(credential.password);
        final entry = (array + index).ref
          ..username = arena<Uint8>(username.length)
          ..usernameSize = username.length
          ..password = arena<Uint8>(password.length)
          ..passwordSize = password.length;
        entry.username.asTypedList(username.length).setAll(0, username);
        entry.password.asTypedList(password.length).setAll(0, password);
      }
      check(
        omq_socket_set_plain_server_credentials(handle, array, list.length),
        'set_plain_server_credentials',
      );
    });
  }

  /// PLAIN client credentials. PLAIN authenticates but does not encrypt.
  void configurePlainClient(String username, String password) {
    setString(SocketOption.plainUsername, username);
    setString(SocketOption.plainPassword, password);
  }

  /// CURVE server with the Z85 [secretKey].
  void configureCurveServer(String secretKey) {
    setInt(SocketOption.curveServer, 1);
    setString(SocketOption.curveSecretKey, secretKey);
  }

  /// CURVE client with its own [keys] and the server's Z85 public key.
  void configureCurveClient(CurveKeyPair keys, {required String serverKey}) {
    setString(SocketOption.curvePublicKey, keys.publicKey);
    setString(SocketOption.curveSecretKey, keys.secretKey);
    setString(SocketOption.curveServerKey, serverKey);
  }

  // Send.

  /// Sends [message] if the socket accepts it now; false otherwise.
  bool trySend(Message message) {
    final sent = _trySendNow(_require('send'), message);
    if (_tail != null) _flushLater();
    return sent;
  }

  /// Sends [message], blocking the isolate until the socket accepts it.
  ///
  /// Throws a [TimeoutException] when the send timeout expires before the
  /// message is committed.
  void sendSync(Message message) {
    final handle = _require('send');
    if (_tail case final tail?) {
      _sendTail(() => _sendFrameSync(handle, tail, 0));
    }
    if (!message.isMultipart) {
      if (!_trySendSingle(handle, message)) _sendSingleSync(handle, message);
      return;
    }
    final frames = message.frames;
    final last = frames.length - 1;
    if (!_sendFrameNow(handle, frames[0], zmqSndmore)) {
      _sendFrameSync(handle, frames[0], zmqSndmore);
    }
    _sendMiddle(handle, frames);
    if (_sendFrameNow(handle, frames[last], 0)) return;
    try {
      _sendFrameSync(handle, frames[last], 0);
    } on TimeoutException {
      _tail = frames[last];
      _flushLater();
    }
  }

  /// Sends [message] once the socket accepts it.
  Future<void> send(Message message) async {
    if (!_trySendNow(_require('send'), message)) return _sendLater(message);
    if (_tail != null) await _finishTail();
  }

  Future<void> _sendLater(Message message) async {
    var attempts = 0;
    while (!_trySendNow(_require('send'), message)) {
      await _waitFor(zmqPollout, attempts++);
    }
    if (_tail != null) await _finishTail();
  }

  bool _trySendNow(Pointer<Void> handle, Message message) {
    if (!_flushTail(handle)) return false;
    if (!message.isMultipart) return _trySendSingle(handle, message);
    final frames = message.frames;
    final last = frames.length - 1;
    if (!_sendFrameNow(handle, frames[0], zmqSndmore)) return false;
    _sendMiddle(handle, frames);
    if (!_sendFrameNow(handle, frames[last], 0)) _tail = frames[last];
    return true;
  }

  /// Sends the committed tail now; false if it still would block.
  bool _flushTail(Pointer<Void> handle) {
    final tail = _tail;
    return tail == null || _sendTail(() => _sendFrameNow(handle, tail, 0));
  }

  /// Runs one tail send. Native code drops the staged frames on any error
  /// but would-block, so the tail goes with them.
  bool _sendTail(Object? Function() send) {
    try {
      if (send() == false) return false;
    } on TimeoutException {
      rethrow;
    } catch (_) {
      _tail = null;
      rethrow;
    }
    _tail = null;
    return true;
  }

  Future<void> _finishTail() async {
    var attempts = 0;
    while (!_flushTail(_require('send'))) {
      await _waitFor(zmqPollout, attempts++);
    }
  }

  /// Finishes a committed tail in the background. Errors after the commit
  /// drop the message, like a lossy send.
  void _flushLater() {
    if (_flushing) return;
    _flushing = true;
    unawaited(
      _finishTail().then((_) {}, onError: (Object _) {}).whenComplete(() => _flushing = false),
    );
  }

  /// Frames after the first only join the staged message; they never block.
  void _sendMiddle(Pointer<Void> handle, List<Uint8List> frames) {
    for (var index = 1; index < frames.length - 1; index++) {
      if (!_sendFrameNow(handle, frames[index], zmqSndmore)) {
        throw OmqException('send', eagain);
      }
    }
  }

  bool _trySendSingle(Pointer<Void> handle, Message message) {
    if (message.routingId == 0 && message.group == null) {
      return _sendFrameNow(handle, message.data, 0);
    }
    final errno = _sendAddressed(handle, message, blocking: false);
    if (errno == 0) return true;
    if (errno == eagain) return false;
    throw error('send', errno);
  }

  void _sendSingleSync(Pointer<Void> handle, Message message) {
    if (message.routingId == 0 && message.group == null) {
      _sendFrameSync(handle, message.data, 0);
      return;
    }
    while (true) {
      final errno = _sendAddressed(handle, message, blocking: true);
      if (errno == 0) return;
      if (errno == eagain) throw TimeoutException('OMQ send timed out');
      if (errno != eintr) throw error('send', errno);
    }
  }

  /// Sends one frame without blocking; false if it would block.
  bool _sendFrameNow(Pointer<Void> handle, Uint8List frame, int flags) {
    final int rc;
    if (_inproc) {
      final copy = _nativeCopy(frame);
      rc = zmq_send(handle, copy, frame.length, flags | zmqDontwait);
      _releaseCopy(copy);
    } else {
      rc = zmq_send_leaf(handle, frame.address, frame.length, flags | zmqDontwait);
    }
    if (rc >= 0) return true;
    final errno = zmq_errno();
    return errno == eagain ? false : throw error('send', errno);
  }

  /// Sends one frame from a native copy, blocking until accepted.
  void _sendFrameSync(Pointer<Void> handle, Uint8List frame, int flags) {
    final copy = _nativeCopy(frame);
    try {
      while (zmq_send(handle, copy, frame.length, flags) < 0) {
        final errno = zmq_errno();
        if (errno == eagain) throw TimeoutException('OMQ send timed out');
        if (errno != eintr) throw error('send', errno);
      }
    } finally {
      _releaseCopy(copy);
    }
  }

  /// Sends a single frame with its routing ID or group through a
  /// `zmq_msg_t`; returns 0 or the errno.
  int _sendAddressed(Pointer<Void> handle, Message message, {required bool blocking}) {
    final data = message.data;
    check(zmq_msg_init_size(_sendMsg, data.length), 'msg_init_size');
    zmq_msg_data(_sendMsg).asTypedList(data.length).setAll(0, data);
    var rc = 0;
    if (message.routingId != 0) rc = zmq_msg_set_routing_id(_sendMsg, message.routingId);
    if (rc == 0 && message.group != null) {
      rc = using(
        (arena) => zmq_msg_set_group(_sendMsg, message.group!.toNativeUtf8(allocator: arena)),
      );
    }
    if (rc == 0) {
      final flags = blocking ? 0 : zmqDontwait;
      rc = blocking || _inproc
          ? zmq_msg_send(_sendMsg, handle, flags)
          : zmq_msg_send_leaf(_sendMsg, handle, flags);
    }
    if (rc >= 0) return 0;
    final errno = zmq_errno();
    zmq_msg_close(_sendMsg);
    return errno;
  }

  // Receive.

  /// The next message if one is queued; null otherwise.
  Message? tryReceive() {
    final handle = _require('receive');
    final size = _recvNow(handle);
    if (size >= 0) return _received(handle, size);
    final errno = zmq_errno();
    return errno == eagain ? null : throw error('receive', errno);
  }

  /// The next message, blocking the isolate until one arrives.
  ///
  /// Throws a [TimeoutException] when the receive timeout expires first.
  Message receiveSync() {
    final handle = _require('receive');
    var size = _recvNow(handle);
    if (size < 0) {
      if (zmq_errno() != eagain) fail('receive');
      size = _receiveFrameSync(handle);
    }
    return _received(handle, size);
  }

  /// The next message. With [timeout], fails with a [TimeoutException]
  /// and takes no message.
  Future<Message> receive({Duration? timeout}) async {
    if (tryReceive() case final message?) return message;
    return _receiveLater(timeout);
  }

  Future<Message> _receiveLater(Duration? timeout) async {
    final clock = timeout == null ? null : (Stopwatch()..start());
    var attempts = 0;
    while (true) {
      if (tryReceive() case final message?) return message;
      final remaining = clock == null ? null : timeout! - clock.elapsed;
      if (remaining != null && remaining <= Duration.zero) {
        throw TimeoutException('OMQ receive timed out', timeout);
      }
      await _waitFor(zmqPollin, attempts++, timeout: remaining);
    }
  }

  /// Messages as they arrive. Pausing the subscription stops receiving, so
  /// the socket applies backpressure; cancel it to stop. The stream ends
  /// when the socket closes.
  Stream<Message> get messages {
    late final StreamController<Message> controller;
    Waiter? waiting;
    var running = false;

    bool active() => controller.hasListener && !controller.isPaused && !isClosed;

    Future<void> pump() async {
      if (running) return;
      running = true;
      try {
        var attempts = 0;
        while (active()) {
          var received = 0;
          Message? message;
          while (received < 256 && active() && (message = tryReceive()) != null) {
            controller.add(message!);
            received++;
          }
          if (!active()) break;
          if (received == 256) {
            attempts = 0;
            await Future<void>.delayed(Duration.zero);
            continue;
          }
          if (received > 0) attempts = 0;
          final waiter = waiting = _arm(zmqPollin, attempts++);
          if (waiter != null) await waiter.future;
          waiting = null;
        }
      } on OmqClosedException {
        unawaited(controller.close());
      } catch (error, stack) {
        controller.addError(error, stack);
      } finally {
        running = false;
        waiting = null;
        if (isClosed && !controller.isClosed) unawaited(controller.close());
      }
    }

    controller = StreamController<Message>(
      sync: true,
      onListen: pump,
      onResume: pump,
      onCancel: () => waiting?.cancel(),
    );
    return controller.stream;
  }

  @pragma('vm:prefer-inline')
  int _recvNow(Pointer<Void> handle) =>
      omq_recv_into_leaf(handle, _recvBuffer, _recvCapacity, _recvMsg, zmqDontwait, _frameInfo);

  /// Builds the message whose first frame the last receive call returned.
  Message _received(Pointer<Void> handle, int size) {
    final info = _frameInfo.ref;
    final routingId = info.routingId;
    final group = _dish ? _frameGroup() : null;
    final first = _frame(size);
    if (info.more == 0) return receivedFrame(first, routingId, group);
    final frames = [first];
    while (true) {
      // The rest of a multipart message is already queued.
      var next = _recvNow(handle);
      if (next < 0) next = _receiveFrameSync(handle);
      final more = info.more != 0;
      frames.add(_frame(next));
      if (!more) return receivedFrames(frames);
    }
  }

  /// Copies out a received frame: from the receive buffer if it fit, else
  /// from the overflow message, which is released right away.
  @pragma('vm:prefer-inline')
  Uint8List _frame(int size) {
    if (size <= _recvCapacity) return _recvView.sublist(0, size);
    final frame = Uint8List.fromList(zmq_msg_data(_recvMsg).asTypedList(size));
    zmq_msg_close(_recvMsg);
    zmq_msg_init(_recvMsg);
    return frame;
  }

  String? _frameGroup() {
    final group = _frameInfo.cast<Uint8>() + frameInfoGroupOffset;
    return group.value == 0 ? null : group.cast<Utf8>().toDartString();
  }

  int _receiveFrameSync(Pointer<Void> handle) {
    while (true) {
      final size = omq_recv_into(handle, _recvBuffer, _recvCapacity, _recvMsg, 0, _frameInfo);
      if (size >= 0) return size;
      final errno = zmq_errno();
      if (errno == eagain) throw TimeoutException('OMQ receive timed out');
      if (errno != eintr) throw error('receive', errno);
    }
  }

  // Readiness.

  /// Waits until [events] may be ready after an operation would block.
  Future<void> _waitFor(int events, int attempts, {Duration? timeout}) async {
    final waiter = _arm(events, attempts, timeout: timeout);
    if (waiter != null) await waiter.future;
  }

  /// Arms the ready callback by reading `ZMQ_EVENTS`; null when [events]
  /// are ready now, else a wait for the next change.
  ///
  /// An operation that would block while `ZMQ_EVENTS` says ready (such as a
  /// ROUTER send to one full peer) retries once at once, then waits for the
  /// next change or a short delay instead of spinning.
  Waiter? _arm(int events, int attempts, {Duration? timeout}) {
    final handle = _require('wait');
    _readiness.register(handle);
    if (_getInt(handle, zmqEvents) & events == 0) return _readiness.wait(timeout: timeout);
    if (attempts == 0) return null;
    final delay = Duration(milliseconds: attempts < 8 ? 1 : 10);
    if (timeout != null && timeout < delay) return _readiness.wait(timeout: timeout);
    return _readiness.wait(timeout: delay, quiet: true);
  }

  // Monitor and lifetime.

  /// Monitors connection events of this socket: [events], or all.
  Monitor monitor({Iterable<MonitorEventType>? events}) => Monitor._(this, events);

  /// Closes the socket. Pending async waits fail with an
  /// [OmqClosedException]. Idempotent.
  void close() {
    if (isClosed) return;
    final handle = _handle;
    while (zmq_close(handle) < 0) {
      final errno = zmq_errno();
      if (errno != eintr) throw error('close', errno);
    }
    _handle = nullptr;
    _tail = null;
    context._sockets.remove(this);
    _readiness.close('wait');
  }

  Pointer<Void> _require(String operation) =>
      isClosed ? throw OmqClosedException(operation) : _handle;

  static int _getInt(Pointer<Void> handle, int option) {
    _optSize.value = sizeOf<Int>();
    check(zmq_getsockopt(handle, option, _optValue.cast(), _optSize), 'getsockopt');
    return _optValue.cast<Int>().value;
  }

  @override
  String toString() => 'Socket(${type.name}${isClosed ? ', closed' : ''})';
}
