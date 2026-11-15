/// Contexts, sockets, and monitors. They share one library so sockets can
/// stay registered with their context without a public hook.
library;

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'message.dart';
import 'native.dart';
import 'options.dart';
import 'ready.dart';
import 'security.dart';

part 'context.dart';
part 'monitor.dart';
part 'socket.dart';

// Per-isolate native scratch. An isolate runs one call at a time, and every
// call copies out before it returns, so all sockets of an isolate share it.
// It lives as long as the isolate.

/// Overflow message for frames larger than the receive buffer.
final Pointer<ZmqMsg> _recvMsg = () {
  final msg = malloc<Uint8>(zmqMsgSize).cast<ZmqMsg>();
  zmq_msg_init(msg);
  return msg;
}();

/// Send message for frames with a routing ID or group.
final Pointer<ZmqMsg> _sendMsg = malloc<Uint8>(zmqMsgSize).cast<ZmqMsg>();

/// `zmq_getsockopt` value and length for integer options.
final Pointer<Int64> _optValue = malloc<Int64>();
final Pointer<Size> _optSize = malloc<Size>();

/// Native copy of outgoing frames for non-leaf sends; grows up to
/// [_scratchMax].
Pointer<Uint8> _scratch = malloc<Uint8>(4096);
int _scratchSize = 4096;
const int _scratchMax = 1 << 16;

/// A native copy of [frame]: the shared scratch for frames up to
/// [_scratchMax], else an allocation that [_releaseCopy] frees.
Pointer<Uint8> _nativeCopy(Uint8List frame) {
  final length = frame.length;
  if (length > _scratchMax) return malloc<Uint8>(length)..asTypedList(length).setAll(0, frame);
  if (length > _scratchSize) {
    malloc.free(_scratch);
    _scratchSize = _scratchMax;
    _scratch = malloc<Uint8>(_scratchSize);
  }
  _scratch.asTypedList(length).setAll(0, frame);
  return _scratch;
}

void _releaseCopy(Pointer<Uint8> copy) {
  if (copy != _scratch) malloc.free(copy);
}

/// Receive buffer for `omq_recv_into`; larger frames go to [_recvMsg].
const int _recvCapacity = 16384;
final Pointer<Uint8> _recvBuffer = malloc<Uint8>(_recvCapacity);
final Uint8List _recvView = _recvBuffer.asTypedList(_recvCapacity);

/// Metadata of the frame from the last `omq_recv_into` call.
final Pointer<FrameInfo> _frameInfo = malloc<FrameInfo>();
