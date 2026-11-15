import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'native.dart';

/// A failed native OMQ call.
class OmqException implements Exception {
  /// Creates an exception for [operation] failing with [errno].
  OmqException(this.operation, this.errno) : message = _describe(errno);

  /// The failed operation, such as `bind` or `send`.
  final String operation;

  /// The native errno value.
  final int errno;

  /// The native error description.
  final String message;

  @override
  String toString() => 'OmqException($operation): $message (errno $errno)';
}

/// The socket or its context is closed, or the context was terminated.
final class OmqClosedException extends OmqException {
  /// Creates an exception for [operation] on a closed handle.
  OmqClosedException(String operation) : super(operation, eterm);
}

String _describe(int errno) {
  final text = zmq_strerror(errno);
  return text == nullptr ? 'errno $errno' : text.toDartString();
}

/// Throws the exception for the current native errno of [operation].
Never fail(String operation) => throw error(operation, zmq_errno());

/// The exception for [operation] failing with [errno].
OmqException error(String operation, int errno) =>
    errno == eterm ? OmqClosedException(operation) : OmqException(operation, errno);

/// Returns [rc], or throws for a negative return code.
int check(int rc, String operation) => rc < 0 ? fail(operation) : rc;
