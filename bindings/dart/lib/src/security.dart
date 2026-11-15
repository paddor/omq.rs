import 'dart:ffi';

import 'package:ffi/ffi.dart';

import 'errors.dart';
import 'native.dart';

/// One username/password pair in a PLAIN server allowlist.
final class PlainCredential {
  /// Creates a credential. Each field is at most 255 ASCII bytes.
  const PlainCredential(this.username, this.password);

  /// The username.
  final String username;

  /// The password.
  final String password;
}

/// A CURVE key pair in Z85 text form (40 characters each).
final class CurveKeyPair {
  const CurveKeyPair._(this.publicKey, this.secretKey);

  /// Generates a new key pair.
  ///
  /// Throws an [OmqException] if CURVE is not available.
  factory CurveKeyPair.generate() => using((arena) {
    final public = arena<Uint8>(41).cast<Utf8>();
    final secret = arena<Uint8>(41).cast<Utf8>();
    check(zmq_curve_keypair(public, secret), 'curve_keypair');
    return CurveKeyPair._(public.toDartString(), secret.toDartString());
  });

  /// The key pair for an existing Z85 [secretKey].
  ///
  /// Throws an [OmqException] for an invalid key.
  factory CurveKeyPair.fromSecretKey(String secretKey) => using((arena) {
    final public = arena<Uint8>(41).cast<Utf8>();
    check(zmq_curve_public(public, secretKey.toNativeUtf8(allocator: arena)), 'curve_public');
    return CurveKeyPair._(public.toDartString(), secretKey);
  });

  /// The Z85 public key.
  final String publicKey;

  /// The Z85 secret key.
  final String secretKey;
}

/// The libzmq API version that libomq_zmq implements.
({int major, int minor, int patch}) zmqVersion() => using((arena) {
  final parts = arena<Int>(3);
  zmq_version(parts, parts + 1, parts + 2);
  return (major: parts[0], minor: parts[1], patch: parts[2]);
});

/// Whether libomq_zmq supports [capability], such as `curve`, `ipc`, or
/// `quic`.
bool hasCapability(String capability) =>
    using((arena) => zmq_has(capability.toNativeUtf8(allocator: arena)) != 0);
