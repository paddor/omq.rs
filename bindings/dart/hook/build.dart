// Registers libomq_zmq as the package's code asset. By default the dynamic
// loader finds it (LD_LIBRARY_PATH, rpath, or system paths). The `library`
// user-define bundles a prebuilt file instead:
//
//   hooks:
//     user_defines:
//       omq:
//         library: path/to/libomq_zmq.so
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    final os = input.config.code.targetOS;
    final library = input.userDefines.path('library');
    if (library == null) {
      output.assets.code.add(
        CodeAsset(
          package: input.packageName,
          name: 'omq_zmq',
          linkMode: DynamicLoadingSystem(Uri.file(os.dylibFileName('omq_zmq'))),
        ),
      );
      return;
    }
    if (!File.fromUri(library).existsSync()) {
      throw ArgumentError('hooks.user_defines.omq.library: $library not found');
    }
    output.dependencies.add(library);
    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: 'omq_zmq',
        linkMode: DynamicLoadingBundled(),
        file: library,
      ),
    );
  });
}
