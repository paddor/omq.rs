#!/usr/bin/env ruby
# frozen_string_literal: true

# Test omq the way an app uses it: a scratch app depends on this package (or,
# with --hosted VERSION, on that pub.dev release) without user-defines, so
# the build hook downloads and checks the pinned prebuilt libomq_zmq. The
# app runs the package's socket tests, then AOT-builds and runs a smoke
# program with `dart build cli`.
#
# usage: ruby bindings/dart/scripts/test_prebuilt.rb [--hosted VERSION]
# Env: OMQ_DART selects the Dart command (default: dart).

require "fileutils"
require "rbconfig"
require "tmpdir"

PACKAGE = File.expand_path("..", __dir__)
# These need the package's own sources or its private hook code.
PACKAGE_ONLY_TESTS = %w[hook_test.dart package_test.dart].freeze
DART = ENV.fetch("OMQ_DART", "dart")
SMOKE = <<~DART
  import 'package:omq/omq.dart';

  Future<void> main() async {
    final context = Context();
    final pull = context.socket(SocketType.pull);
    final push = context.socket(SocketType.push);
    push.connect(pull.bind('tcp://127.0.0.1:*'));
    await push.send(Message.text('hello'));
    final text = (await pull.receive()).text;
    context.close();
    if (text != 'hello') throw StateError('received $text');
    print('smoke: $text');
  }
DART

hosted = ARGV[0] == "--hosted" ? ARGV.fetch(1) { abort "usage: #{$PROGRAM_NAME} [--hosted VERSION]" } : nil
dependency = hosted ? "  omq: #{hosted}\n" : "  omq:\n    path: #{PACKAGE.inspect}\n"

def run(*cmd, chdir:)
  # The system loader must not find a local libomq_zmq.
  env = { "LD_LIBRARY_PATH" => nil, "DYLD_LIBRARY_PATH" => nil }
  puts "::: #{cmd.join(" ")}"
  system(env, *cmd, chdir:, exception: true)
end

Dir.mktmpdir("omq_dart_prebuilt") do |app|
  File.write(File.join(app, "pubspec.yaml"), <<~YAML)
    name: omq_prebuilt_check
    publish_to: none
    environment:
      sdk: ^3.10.0
    dependencies:
    #{dependency.chomp}
    dev_dependencies:
      test: ^1.26.0
  YAML
  FileUtils.mkdir_p([File.join(app, "test"), File.join(app, "bin")])
  Dir[File.join(PACKAGE, "test", "*.dart")].each do |path|
    FileUtils.cp(path, File.join(app, "test")) unless PACKAGE_ONLY_TESTS.include?(File.basename(path))
  end
  File.write(File.join(app, "bin", "smoke.dart"), SMOKE)

  run(DART, "pub", "get", chdir: app)
  run(DART, "test", chdir: app)
  run(DART, "build", "cli", "--target", "bin/smoke.dart", chdir: app)
  windows = RbConfig::CONFIG["host_os"].match?(/mswin|mingw/)
  binary = Dir[File.join(app, "build", "cli", "*", "bundle", "bin", windows ? "smoke.exe" : "smoke")].first or
    abort "dart build cli produced no smoke binary"
  libraries = Dir[File.join(File.dirname(binary), "..", "lib", "*omq_zmq*")]
  abort "the bundle has no libomq_zmq: #{Dir[File.join(File.dirname(binary), "..", "**", "*")]}" if libraries.empty?
  run(binary, chdir: Dir.tmpdir)
end
puts "::: omq #{hosted || "(path)"} works with the prebuilt libomq_zmq"
