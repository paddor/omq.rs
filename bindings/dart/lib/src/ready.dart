import 'dart:async';
import 'dart:ffi';

import 'errors.dart';
import 'native.dart';

// One NativeCallable.listener serves every socket of this isolate; the
// callback userdata is the socket's id. The listener posts each call to this
// isolate's event loop, so native I/O threads never run Dart code. It keeps
// the isolate alive only while some async wait is pending.

final Map<int, Readiness> _sockets = {};
NativeCallable<ReadyCallbackFn>? _listener;
int _pendingWaits = 0;
int _nextId = 1;

NativeCallable<ReadyCallbackFn> get _callback =>
    _listener ??= NativeCallable<ReadyCallbackFn>.listener(_dispatch)..keepIsolateAlive = false;

void _dispatch(Pointer<Void> userdata) => _sockets[userdata.address]?._wake();

void _waitStarted() {
  if (_pendingWaits++ == 0) _callback.keepIsolateAlive = true;
}

void _waitEnded() {
  if (--_pendingWaits == 0) _callback.keepIsolateAlive = false;
}

/// Async waits on one socket, woken by its native ready callback.
final class Readiness {
  /// Creates the wait state for a new socket.
  Readiness() : _id = _nextId++;

  final int _id;
  bool _registered = false;
  final List<Waiter> _waiters = [];

  /// Registers the callback on the first wait, so sockets without async
  /// waits never arm native write wakes. Registration arms one call.
  void register(Pointer<Void> socket) {
    if (_registered) return;
    _sockets[_id] = this;
    final rc = omq_socket_set_ready_callback(
      socket,
      _callback.nativeFunction,
      Pointer.fromAddress(_id),
    );
    if (rc < 0) {
      _sockets.remove(_id);
      fail('set_ready_callback');
    }
    _registered = true;
  }

  /// A wait that ends at the next callback, or after [timeout]. A timeout
  /// fails the wait with a [TimeoutException] unless [quiet] is set.
  ///
  /// Take it right after arming the callback by reading `ZMQ_EVENTS`: the
  /// callback is posted to the event loop, so it cannot run in between.
  Waiter wait({Duration? timeout, bool quiet = false}) {
    final waiter = Waiter._(this);
    _waiters.add(waiter);
    _waitStarted();
    if (timeout != null) {
      waiter._timer = Timer(
        timeout,
        () => waiter._finish(quiet ? null : TimeoutException('OMQ wait timed out', timeout)),
      );
    }
    return waiter;
  }

  void _wake() {
    if (_waiters.isEmpty) return;
    for (final waiter in _waiters.toList()) {
      waiter._finish(null);
    }
  }

  /// Fails pending waits after `zmq_close`; no callback runs or follows by
  /// then, and late posted ones find no entry.
  void close(String operation) {
    _sockets.remove(_id);
    for (final waiter in _waiters.toList()) {
      waiter._finish(OmqClosedException(operation));
    }
  }
}

/// One pending wait on a socket.
final class Waiter {
  Waiter._(this._owner);

  final Readiness _owner;
  final Completer<void> _completer = Completer<void>();
  Timer? _timer;

  /// Completes at the next readiness change.
  Future<void> get future => _completer.future;

  /// Ends the wait early without an error.
  void cancel() => _finish(null);

  void _finish(Object? error) {
    if (_completer.isCompleted) return;
    _timer?.cancel();
    _owner._waiters.remove(this);
    _waitEnded();
    if (error == null) {
      _completer.complete();
    } else {
      _completer.completeError(error);
    }
  }
}
