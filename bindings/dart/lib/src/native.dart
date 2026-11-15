// ignore_for_file: non_constant_identifier_names, public_member_api_docs

// C ABI declarations for libomq_zmq. Calls that never block are leaf calls;
// calls that may block or take long-held locks are not, so the VM can run GC
// and other isolates meanwhile. Typed data `.address` is only valid in leaf
// calls.
@DefaultAsset('package:omq/omq_zmq')
library;

import 'dart:ffi';
import 'dart:io' show Platform;

import 'package:ffi/ffi.dart';

/// Opaque 64-byte `zmq_msg_t`.
final class ZmqMsg extends Opaque {}

/// Size of `zmq_msg_t`.
const int zmqMsgSize = 64;

/// `omq_plain_credential_t`.
final class PlainCredentialStruct extends Struct {
  external Pointer<Uint8> username;
  @Size()
  external int usernameSize;
  external Pointer<Uint8> password;
  @Size()
  external int passwordSize;
}

/// `omq_frame_info_t`: metadata of a frame from `omq_recv_into`.
final class FrameInfo extends Struct {
  @Int()
  external int more;
  @Uint32()
  external int routingId;
  @Array(256)
  external Array<Char> group;
}

/// Byte offset of `omq_frame_info_t.group`.
const int frameInfoGroupOffset = 8;

typedef ReadyCallbackFn = Void Function(Pointer<Void>);

const int zmqDontwait = 1;
const int zmqSndmore = 2;
const int zmqPollin = 1;
const int zmqPollout = 2;
const int zmqEvents = 15;
const int zmqLastEndpoint = 32;

final int eagain = Platform.isMacOS || Platform.isIOS ? 35 : 11;
const int eintr = 4;
const int zmqHausnumero = 156384712;
const int efsm = zmqHausnumero + 51;
const int eterm = zmqHausnumero + 53;

// Library.

@Native<Void Function(Pointer<Int>, Pointer<Int>, Pointer<Int>)>(isLeaf: true)
external void zmq_version(Pointer<Int> major, Pointer<Int> minor, Pointer<Int> patch);

@Native<Int Function()>(isLeaf: true)
external int zmq_errno();

@Native<Pointer<Utf8> Function(Int)>(isLeaf: true)
external Pointer<Utf8> zmq_strerror(int errnum);

@Native<Int Function(Pointer<Utf8>)>(isLeaf: true)
external int zmq_has(Pointer<Utf8> capability);

@Native<Int Function(Pointer<Utf8>, Pointer<Utf8>)>(isLeaf: true)
external int zmq_curve_keypair(Pointer<Utf8> publicKey, Pointer<Utf8> secretKey);

@Native<Int Function(Pointer<Utf8>, Pointer<Utf8>)>(isLeaf: true)
external int zmq_curve_public(Pointer<Utf8> publicKey, Pointer<Utf8> secretKey);

// Context.

@Native<Pointer<Void> Function()>()
external Pointer<Void> zmq_ctx_new();

@Native<Int Function(Pointer<Void>)>()
external int zmq_ctx_term(Pointer<Void> context);

@Native<Int Function(Pointer<Void>)>()
external int zmq_ctx_shutdown(Pointer<Void> context);

@Native<Int Function(Pointer<Void>, Int, Int)>()
external int zmq_ctx_set(Pointer<Void> context, int option, int value);

@Native<Int Function(Pointer<Void>, Int)>(isLeaf: true)
external int zmq_ctx_get(Pointer<Void> context, int option);

@Native<Int Function(Pointer<Void>, Pointer<Uint64>, Pointer<Uint64>)>(isLeaf: true)
external int omq_ctx_share_key(Pointer<Void> context, Pointer<Uint64> high, Pointer<Uint64> low);

@Native<Pointer<Void> Function(Uint64, Uint64)>()
external Pointer<Void> omq_ctx_from_share_key(int high, int low);

// Socket.

@Native<Pointer<Void> Function(Pointer<Void>, Int)>()
external Pointer<Void> zmq_socket(Pointer<Void> context, int type);

@Native<Int Function(Pointer<Void>)>()
external int zmq_close(Pointer<Void> socket);

@Native<Int Function(Pointer<Void>)>(isLeaf: true)
external int omq_socket_allow_thread_migration(Pointer<Void> socket);

@Native<Int Function(Pointer<Void>, Pointer<NativeFunction<ReadyCallbackFn>>, Pointer<Void>)>()
external int omq_socket_set_ready_callback(
  Pointer<Void> socket,
  Pointer<NativeFunction<ReadyCallbackFn>> callback,
  Pointer<Void> userdata,
);

@Native<Int Function(Pointer<Void>, Pointer<PlainCredentialStruct>, Size)>()
external int omq_socket_set_plain_server_credentials(
  Pointer<Void> socket,
  Pointer<PlainCredentialStruct> credentials,
  int count,
);

@Native<Int Function(Pointer<Void>, Int, Pointer<Void>, Size)>()
external int zmq_setsockopt(Pointer<Void> socket, int option, Pointer<Void> value, int size);

@Native<Int Function(Pointer<Void>, Int, Pointer<Void>, Pointer<Size>)>(isLeaf: true)
external int zmq_getsockopt(
  Pointer<Void> socket,
  int option,
  Pointer<Void> value,
  Pointer<Size> size,
);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_bind(Pointer<Void> socket, Pointer<Utf8> endpoint);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_connect(Pointer<Void> socket, Pointer<Utf8> endpoint);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_unbind(Pointer<Void> socket, Pointer<Utf8> endpoint);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_disconnect(Pointer<Void> socket, Pointer<Utf8> endpoint);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_join(Pointer<Void> socket, Pointer<Utf8> group);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>)>()
external int zmq_leave(Pointer<Void> socket, Pointer<Utf8> group);

@Native<Int Function(Pointer<Void>, Pointer<Utf8>, Int)>()
external int zmq_socket_monitor(Pointer<Void> socket, Pointer<Utf8> endpoint, int events);

// Send and receive. The `_leaf` variants are leaf bindings of the same
// symbols, for ZMQ_DONTWAIT calls. A Dart FFI callback must not run inside a
// leaf call, and an inproc send runs the receiver's ready callback inline,
// so inproc sends use the non-leaf bindings.

@Native<Int Function(Pointer<Void>, Pointer<Uint8>, Size, Int)>(symbol: 'zmq_send', isLeaf: true)
external int zmq_send_leaf(Pointer<Void> socket, Pointer<Uint8> data, int size, int flags);

@Native<Int Function(Pointer<Void>, Pointer<Uint8>, Size, Int)>()
external int zmq_send(Pointer<Void> socket, Pointer<Uint8> data, int size, int flags);

@Native<Int Function(Pointer<ZmqMsg>)>(isLeaf: true)
external int zmq_msg_init(Pointer<ZmqMsg> msg);

@Native<Int Function(Pointer<ZmqMsg>, Size)>(isLeaf: true)
external int zmq_msg_init_size(Pointer<ZmqMsg> msg, int size);

@Native<Int Function(Pointer<ZmqMsg>)>(isLeaf: true)
external int zmq_msg_close(Pointer<ZmqMsg> msg);

@Native<Pointer<Uint8> Function(Pointer<ZmqMsg>)>(isLeaf: true)
external Pointer<Uint8> zmq_msg_data(Pointer<ZmqMsg> msg);

@Native<Int Function(Pointer<ZmqMsg>, Uint32)>(isLeaf: true)
external int zmq_msg_set_routing_id(Pointer<ZmqMsg> msg, int routingId);

@Native<Int Function(Pointer<ZmqMsg>, Pointer<Utf8>)>(isLeaf: true)
external int zmq_msg_set_group(Pointer<ZmqMsg> msg, Pointer<Utf8> group);

@Native<Int Function(Pointer<ZmqMsg>, Pointer<Void>, Int)>(symbol: 'zmq_msg_send', isLeaf: true)
external int zmq_msg_send_leaf(Pointer<ZmqMsg> msg, Pointer<Void> socket, int flags);

@Native<Int Function(Pointer<ZmqMsg>, Pointer<Void>, Int)>()
external int zmq_msg_send(Pointer<ZmqMsg> msg, Pointer<Void> socket, int flags);

@Native<
  Int Function(Pointer<Void>, Pointer<Uint8>, Size, Pointer<ZmqMsg>, Int, Pointer<FrameInfo>)
>(symbol: 'omq_recv_into', isLeaf: true)
external int omq_recv_into_leaf(
  Pointer<Void> socket,
  Pointer<Uint8> buffer,
  int size,
  Pointer<ZmqMsg> overflow,
  int flags,
  Pointer<FrameInfo> info,
);

@Native<
  Int Function(Pointer<Void>, Pointer<Uint8>, Size, Pointer<ZmqMsg>, Int, Pointer<FrameInfo>)
>()
external int omq_recv_into(
  Pointer<Void> socket,
  Pointer<Uint8> buffer,
  int size,
  Pointer<ZmqMsg> overflow,
  int flags,
  Pointer<FrameInfo> info,
);
