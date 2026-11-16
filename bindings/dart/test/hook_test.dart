import 'dart:io';
import 'dart:typed_data';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';
import 'package:test/test.dart';

import '../hook/build.dart' as hook;
import 'package:omq/src/hook/libomq_zmq.dart';
import 'package:omq/src/hook/prebuilt.dart';

PackageUserDefines defines(Map<String, Object?> values) => PackageUserDefines(
  workspacePubspec: PackageUserDefinesSource(defines: values, basePath: Directory.current.uri),
);

Directory tempDir() {
  final dir = Directory.systemTemp.createTempSync('omq_hook_test');
  addTearDown(() => dir.deleteSync(recursive: true));
  return dir;
}

/// Serves [body] (or [status]) on loopback and counts requests.
Future<(Uri, int Function())> serve(Uint8List body, {int status = 200}) async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  addTearDown(() => server.close(force: true));
  var requests = 0;
  server.listen((request) {
    requests++;
    request.response.statusCode = status;
    if (status == 200) request.response.add(body);
    request.response.close();
  });
  return (Uri.parse('http://127.0.0.1:${server.port}/lib'), () => requests);
}

void main() {
  group('build hook', () {
    test('the library user-define bundles that file', () async {
      final library = File('${tempDir().path}/libomq_zmq.so')..writeAsStringSync('x');
      await testCodeBuildHook(
        mainMethod: hook.main,
        userDefines: defines({'library': library.path}),
        check: (input, output) {
          final asset = output.assets.code.single;
          expect(asset.linkMode, isA<DynamicLoadingBundled>());
          expect(asset.file, library.uri);
          expect(output.dependencies, contains(library.uri));
        },
      );
    });

    test('a missing library file fails the build', () async {
      await expectLater(
        testCodeBuildHook(
          mainMethod: hook.main,
          userDefines: defines({'library': '${tempDir().path}/missing.so'}),
          check: (_, _) {},
        ),
        throwsArgumentError,
      );
    });

    test('system: true loads through the system loader', () async {
      await testCodeBuildHook(
        mainMethod: hook.main,
        userDefines: defines({'system': true}),
        check: (input, output) {
          final linkMode = output.assets.code.single.linkMode;
          expect(linkMode, isA<DynamicLoadingSystem>());
          expect(
            (linkMode as DynamicLoadingSystem).uri,
            Uri.file(OS.current.dylibFileName('omq_zmq')),
          );
        },
      );
    });

    test('a target without a prebuilt library names the user-defines', () async {
      await expectLater(
        testCodeBuildHook(
          mainMethod: hook.main,
          targetOS: OS.android,
          targetArchitecture: Architecture.arm64,
          check: (_, _) {},
        ),
        throwsA(isA<UnsupportedError>().having((e) => e.message, 'message', contains('library'))),
      );
    });
  });

  group('prebuilt', () {
    test('asset names carry the target triple', () {
      final names = [
        for (final (os, architecture) in [
          (OS.linux, Architecture.x64),
          (OS.linux, Architecture.arm64),
          (OS.macOS, Architecture.x64),
          (OS.macOS, Architecture.arm64),
          (OS.windows, Architecture.x64),
        ])
          Prebuilt('1.2.3', targetTriple(os, architecture)!, os, '').assetName,
      ];
      expect(names, [
        'libomq_zmq-x86_64-unknown-linux-gnu.so',
        'libomq_zmq-aarch64-unknown-linux-gnu.so',
        'libomq_zmq-x86_64-apple-darwin.dylib',
        'libomq_zmq-aarch64-apple-darwin.dylib',
        'omq_zmq-x86_64-pc-windows-msvc.dll',
      ]);
      expect(
        Prebuilt('1.2.3', 'x86_64-unknown-linux-gnu', OS.linux, '').url.toString(),
        'https://github.com/paddor/omq.rs/releases/download/omq-libzmq-v1.2.3/'
        'libomq_zmq-x86_64-unknown-linux-gnu.so',
      );
    });

    test('the pin covers every target with a SHA-256', () {
      if (libomqZmqVersion.isEmpty) {
        expect(libomqZmqSha256, isEmpty);
        return;
      }
      expect(libomqZmqSha256.keys.toSet(), {
        'x86_64-unknown-linux-gnu',
        'aarch64-unknown-linux-gnu',
        'x86_64-apple-darwin',
        'aarch64-apple-darwin',
        'x86_64-pc-windows-msvc',
      });
      for (final digest in libomqZmqSha256.values) {
        expect(digest, matches(RegExp(r'^[0-9a-f]{64}$')));
      }
    });
  });

  group('fetchVerified', () {
    final body = Uint8List.fromList(List.generate(4096, (index) => index & 0xff));
    final digest = sha256.convert(body).toString();

    test('downloads and keeps a matching file', () async {
      final (url, requests) = await serve(body);
      final file = File('${tempDir().path}/lib/libomq_zmq.so');
      await fetchVerified(url, digest, file);
      expect(file.readAsBytesSync(), body);
      await fetchVerified(url, digest, file);
      expect(requests(), 1);
    });

    test('replaces a corrupted file', () async {
      final (url, requests) = await serve(body);
      final file = File('${tempDir().path}/libomq_zmq.so')..writeAsStringSync('stale');
      await fetchVerified(url, digest, file);
      expect(file.readAsBytesSync(), body);
      expect(requests(), 1);
    });

    test('rejects a digest mismatch and leaves no file', () async {
      final (url, _) = await serve(body);
      final dir = tempDir();
      final file = File('${dir.path}/libomq_zmq.so');
      await expectLater(fetchVerified(url, '0' * 64, file), throwsStateError);
      expect(dir.listSync(), isEmpty);
    });

    test('fails on an HTTP error', () async {
      final (url, _) = await serve(body, status: 404);
      final file = File('${tempDir().path}/libomq_zmq.so');
      await expectLater(fetchVerified(url, digest, file), throwsA(isA<HttpException>()));
      expect(file.existsSync(), isFalse);
    });
  });
}
