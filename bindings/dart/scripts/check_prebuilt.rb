#!/usr/bin/env ruby
# frozen_string_literal: true

# Check every prebuilt libomq_zmq the build hook pins: download it from the
# omq-libzmq release, compare its SHA-256 with the pin, and check that it is
# a library for its target that exports every C symbol the package binds
# with @Native. Covers targets CI cannot run. Needs llvm-nm and llvm-readobj
# (versioned names such as llvm-nm-19 work).
#
# usage: ruby bindings/dart/scripts/check_prebuilt.rb [CACHE_DIR]

require "digest"
require "fileutils"
require "open-uri"

PACKAGE = File.expand_path("..", __dir__)
CACHE = ARGV.fetch(0) { File.expand_path("../../../tmp/omq-dart-prebuilt", PACKAGE) }
# Expected `file` output per target.
KINDS = {
  "x86_64-unknown-linux-gnu" => /ELF 64-bit .* x86-64/,
  "aarch64-unknown-linux-gnu" => /ELF 64-bit .* aarch64/,
  "x86_64-apple-darwin" => /Mach-O 64-bit .*x86_64/,
  "aarch64-apple-darwin" => /Mach-O 64-bit .*arm64/,
  "x86_64-pc-windows-msvc" => /PE32\+ .* x86-64/,
}.freeze

def tool(name)
  candidates = [name] + (30.downto(14).map { |version| "#{name}-#{version}" })
  candidates.find { |candidate| system("command -v #{candidate} >/dev/null 2>&1") } or
    abort "#{name} not found"
end

# The C symbols bound with @Native: the `symbol:` argument or the name.
def bound_symbols
  source = Dir[File.join(PACKAGE, "lib/src/**/*.dart")].map { |path| File.read(path) }.join("\n")
  source.split("@Native").drop(1).filter_map do |chunk|
    declaration = chunk[/\A.*?external [^;]*?(\w+)\(/m] or next
    declaration[/symbol: '(\w+)'/, 1] || declaration[/(\w+)\(\z/, 1]
  end.uniq.sort
end

def exports(path, nm, readobj)
  if path.end_with?(".dll")
    `#{readobj} --coff-exports #{path}`.scan(/Name: (\w+)/).flatten
  else
    prefix = path.end_with?(".dylib") ? "_" : ""
    `#{nm} --defined-only --extern-only #{path}`.lines.map { |line| line.split.last.delete_prefix(prefix) }
  end
end

pin = File.read(File.join(PACKAGE, "lib/src/hook/libomq_zmq.dart"))
version = pin[/libomqZmqVersion = '([^']*)'/, 1].to_s
abort "no omq-libzmq release pinned" if version.empty?
pins = pin.scan(/'([\w-]+)': '(\h{64})'/).to_h
abort "pinned targets #{pins.keys.sort}, expected #{KINDS.keys.sort}" unless pins.keys.sort == KINDS.keys.sort

symbols = bound_symbols
abort "found only #{symbols.size} bound symbols" if symbols.size < 30
nm = tool("llvm-nm")
readobj = tool("llvm-readobj")
FileUtils.mkdir_p(File.join(CACHE, version))
puts "omq-libzmq #{version}: #{symbols.size} bound symbols"

failed = pins.map do |target, sha256|
  name = target.include?("windows") ? "omq_zmq-#{target}.dll" :
           "libomq_zmq-#{target}.#{target.include?("apple") ? "dylib" : "so"}"
  path = File.join(CACHE, version, name)
  unless File.exist?(path) && Digest::SHA256.file(path).hexdigest == sha256
    url = "https://github.com/paddor/omq.rs/releases/download/omq-libzmq-v#{version}/#{name}"
    File.binwrite(path, URI.parse(url).open(&:read))
  end
  problems = []
  problems << "SHA-256 mismatch" unless Digest::SHA256.file(path).hexdigest == sha256
  kind = `file -b #{path}`
  problems << "not #{KINDS[target].source}: #{kind.strip}" unless kind.match?(KINDS[target])
  missing = symbols - exports(path, nm, readobj)
  problems << "missing #{missing.join(" ")}" unless missing.empty?
  puts format("  %-26s %s", target, problems.empty? ? "ok" : problems.join("; "))
  !problems.empty?
end
exit(failed.any? ? 1 : 0)
