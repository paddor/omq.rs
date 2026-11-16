// Prebuilt libomq_zmq from omq-libzmq GitHub releases, checked against the
// SHA-256 sums pinned in libomq_zmq.dart.
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';

import 'libomq_zmq.dart';

/// The Rust target triple of the prebuilt library for [os] and
/// [architecture], or null when releases have none.
String? targetTriple(OS os, Architecture architecture) => switch ((os, architecture)) {
  (OS.linux, Architecture.x64) => 'x86_64-unknown-linux-gnu',
  (OS.linux, Architecture.arm64) => 'aarch64-unknown-linux-gnu',
  (OS.macOS, Architecture.x64) => 'x86_64-apple-darwin',
  (OS.macOS, Architecture.arm64) => 'aarch64-apple-darwin',
  (OS.windows, Architecture.x64) => 'x86_64-pc-windows-msvc',
  _ => null,
};

/// One prebuilt library of an omq-libzmq release.
final class Prebuilt {
  /// The library for [target] on [os] in release [version], whose asset has
  /// the SHA-256 [sha256] (lowercase hex).
  const Prebuilt(this.version, this.target, this.os, this.sha256);

  /// The pinned library for [os] and [architecture], or null when the pinned
  /// release has none.
  static Prebuilt? pinned(OS os, Architecture architecture) {
    final target = targetTriple(os, architecture);
    final sha256 = libomqZmqSha256[target];
    if (target == null || sha256 == null) return null;
    return Prebuilt(libomqZmqVersion, target, os, sha256);
  }

  /// The omq-libzmq version.
  final String version;

  /// The Rust target triple.
  final String target;

  /// The target OS.
  final OS os;

  /// The SHA-256 of the asset.
  final String sha256;

  /// The file name the loader expects, such as `libomq_zmq.so`.
  String get fileName => os.dylibFileName('omq_zmq');

  /// The release asset name, such as `libomq_zmq-x86_64-unknown-linux-gnu.so`.
  String get assetName {
    final dot = fileName.lastIndexOf('.');
    return '${fileName.substring(0, dot)}-$target${fileName.substring(dot)}';
  }

  /// The release asset URL.
  Uri get url =>
      Uri.https('github.com', '/paddor/omq.rs/releases/download/omq-libzmq-v$version/$assetName');
}

/// Makes [file] hold the content of [url] with SHA-256 [expected]: keeps a
/// matching file, else downloads next to it, checks the digest, and moves the
/// download into place. A mismatch leaves no file behind.
Future<void> fetchVerified(Uri url, String expected, File file) async {
  if (file.existsSync() && await _digest(file) == expected) return;
  file.parent.createSync(recursive: true);
  final download = File('${file.path}.download');
  final client = HttpClient();
  try {
    final response = await (await client.getUrl(url)).close();
    if (response.statusCode != HttpStatus.ok) {
      await response.drain<void>();
      throw HttpException('HTTP ${response.statusCode}', uri: url);
    }
    await response.pipe(download.openWrite());
  } finally {
    client.close();
  }
  final actual = await _digest(download);
  if (actual != expected) {
    download.deleteSync();
    throw StateError('$url has SHA-256 $actual, expected $expected');
  }
  if (file.existsSync()) file.deleteSync();
  download.renameSync(file.path);
}

Future<String> _digest(File file) async => (await sha256.bind(file.openRead()).first).toString();
