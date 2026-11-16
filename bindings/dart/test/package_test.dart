// Release checks on the package itself: what pub.dev accepts, what the
// prebuilt pin covers, and that the native library has every bound symbol.
import 'dart:ffi';
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:omq/src/hook/libomq_zmq.dart';
import 'package:omq/src/hook/prebuilt.dart';
import 'package:test/test.dart';

/// The C symbols the package binds with `@Native`, from its sources.
List<String> boundSymbols() {
  final symbols = <String>{};
  for (final file in Directory('lib/src').listSync(recursive: true).whereType<File>()) {
    final chunks = file.readAsStringSync().split('@Native').skip(1);
    for (final chunk in chunks) {
      final declaration = RegExp(r'^[\s\S]*?external [^;]*?(\w+)\(').firstMatch(chunk);
      if (declaration == null) continue;
      final annotation = declaration.group(0)!;
      symbols.add(
        RegExp(r"symbol: '(\w+)'").firstMatch(annotation)?.group(1) ?? declaration.group(1)!,
      );
    }
  }
  return symbols.toList()..sort();
}

void main() {
  test('hook/ holds only hook entry points, as pub.dev requires', () {
    final files = Directory('hook').listSync().map((entry) => entry.uri.pathSegments.last);
    expect(files, everyElement(isIn(['build.dart', 'link.dart'])));
  });

  test('the prebuilt pin covers every platform in pubspec.yaml', () {
    expect(libomqZmqVersion, matches(RegExp(r'^\d+\.\d+\.\d+')));
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final platforms = {
      'linux': [OS.linux],
      'macos': [OS.macOS],
      'windows': [OS.windows],
    };
    final architectures = {
      OS.linux: [Architecture.x64, Architecture.arm64],
      OS.macOS: [Architecture.x64, Architecture.arm64],
      OS.windows: [Architecture.x64],
    };
    for (final MapEntry(key: name, value: oses) in platforms.entries) {
      if (!RegExp('^  $name:', multiLine: true).hasMatch(pubspec)) continue;
      for (final os in oses) {
        for (final architecture in architectures[os]!) {
          final prebuilt = Prebuilt.pinned(os, architecture);
          expect(prebuilt, isNotNull, reason: '$os $architecture');
          expect(prebuilt!.sha256, matches(RegExp(r'^[0-9a-f]{64}$')));
        }
      }
    }
  });

  test('CHANGELOG.md has a section for a release version', () {
    final version = RegExp(
      r'^version: (\S+)$',
      multiLine: true,
    ).firstMatch(File('pubspec.yaml').readAsStringSync())!.group(1)!;
    if (version.contains('-')) return;
    expect(File('CHANGELOG.md').readAsStringSync(), contains('## [$version] - '));
  });

  test('libomq_zmq exports every symbol the package binds', () {
    final symbols = boundSymbols();
    expect(symbols, hasLength(greaterThan(30)));
    // The package's own tests load libomq_zmq through the system loader.
    final library = DynamicLibrary.open(OS.current.dylibFileName('omq_zmq'));
    expect(symbols.where((symbol) => !library.providesSymbol(symbol)), isEmpty);
  });
}
