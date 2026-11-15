part of 'core.dart';

/// A share key for [Context.fromShareKey].
typedef ShareKey = ({int high, int low});

/// Owns native I/O threads and the sockets created from it.
///
/// Close every context before its isolate exits: native I/O threads post
/// readiness to the isolate that created a socket.
final class Context {
  /// Creates a context with [ioThreads] native I/O threads.
  factory Context({int ioThreads = 1}) {
    if (ioThreads < 1) throw RangeError.range(ioThreads, 1, null, 'ioThreads');
    final handle = zmq_ctx_new();
    if (handle == nullptr) fail('ctx_new');
    final context = Context._(handle);
    if (ioThreads != 1) {
      try {
        context.setOption(ContextOption.ioThreads, ioThreads);
      } catch (_) {
        context.close();
        rethrow;
      }
    }
    return context;
  }

  /// Joins the native context of [key], from [shareKey] of a context in this
  /// process (any isolate or binding). Sockets of both see the same
  /// `inproc://` endpoints. Close each handle separately.
  factory Context.fromShareKey(ShareKey key) {
    final handle = omq_ctx_from_share_key(key.high, key.low);
    if (handle == nullptr) fail('ctx_from_share_key');
    return Context._(handle);
  }

  Context._(this._handle);

  Pointer<Void> _handle;
  final Set<Socket> _sockets = LinkedHashSet.identity();

  /// Whether [close] was called.
  bool get isClosed => _handle == nullptr;

  /// The key that lets another isolate or binding join this context.
  ShareKey get shareKey => using((arena) {
    final high = arena<Uint64>();
    final low = arena<Uint64>();
    check(omq_ctx_share_key(_require('ctx_share_key'), high, low), 'ctx_share_key');
    return (high: high.value, low: low.value);
  });

  /// Sets a native context option ([ContextOption]).
  void setOption(int option, int value) =>
      check(zmq_ctx_set(_require('ctx_set'), option, value), 'ctx_set');

  /// Reads a native context option ([ContextOption]).
  int getOption(int option) => check(zmq_ctx_get(_require('ctx_get'), option), 'ctx_get');

  /// Creates a socket of [type], configured with [options].
  Socket socket(SocketType type, [SocketOptions options = const SocketOptions()]) {
    final handle = zmq_socket(_require('socket'), type.value);
    if (handle == nullptr) fail('socket');
    final socket = Socket._(this, type, handle);
    try {
      // Isolates may resume on another thread; calls stay serialized.
      check(omq_socket_allow_thread_migration(handle), 'allow_thread_migration');
      socket._apply(options);
    } catch (_) {
      socket.close();
      rethrow;
    }
    _sockets.add(socket);
    return socket;
  }

  /// Closes all sockets of this context, then terminates it. Blocks while
  /// sockets linger to deliver queued messages. Idempotent.
  void close() {
    if (isClosed) return;
    for (final socket in _sockets.toList()) {
      socket.close();
    }
    while (zmq_ctx_term(_handle) < 0) {
      final errno = zmq_errno();
      if (errno != eintr) throw error('ctx_term', errno);
    }
    _handle = nullptr;
  }

  Pointer<Void> _require(String operation) =>
      isClosed ? throw OmqClosedException(operation) : _handle;
}
