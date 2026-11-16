// Registers libomq_zmq as the package's code asset. By default the hook
// downloads the prebuilt library of the pinned omq-libzmq release and checks
// its SHA-256. User-defines in the app's pubspec.yaml override that:
//
//   hooks:
//     user_defines:
//       omq:
//         library: path/to/libomq_zmq.so   # bundle this file
//         system: true                     # or load it via the system loader
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';

import 'package:omq/src/hook/prebuilt.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    output.assets.code.add(await _library(input, output));
  });
}

Future<CodeAsset> _library(BuildInput input, BuildOutputBuilder output) async {
  final os = input.config.code.targetOS;
  CodeAsset asset(LinkMode linkMode, [Uri? file]) =>
      CodeAsset(package: input.packageName, name: 'omq_zmq', linkMode: linkMode, file: file);

  if (input.userDefines.path('library') case final library?) {
    if (!File.fromUri(library).existsSync()) {
      throw ArgumentError('hooks.user_defines.omq.library: $library not found');
    }
    output.dependencies.add(library);
    return asset(DynamicLoadingBundled(), library);
  }
  if (input.userDefines['system'] == true) {
    return asset(DynamicLoadingSystem(Uri.file(os.dylibFileName('omq_zmq'))));
  }

  final architecture = input.config.code.targetArchitecture;
  final prebuilt = Prebuilt.pinned(os, architecture);
  if (prebuilt == null) {
    throw UnsupportedError(
      'No prebuilt libomq_zmq for $os $architecture. Set '
      'hooks.user_defines.omq.library to a libomq_zmq build, or system: true '
      'to load it through the system loader.',
    );
  }
  final file = File.fromUri(
    input.outputDirectoryShared.resolve(
      'libomq_zmq-${prebuilt.version}/${prebuilt.target}/${prebuilt.fileName}',
    ),
  );
  await fetchVerified(prebuilt.url, prebuilt.sha256, file);
  return asset(DynamicLoadingBundled(), file.uri);
}
