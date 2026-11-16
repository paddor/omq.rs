#!/usr/bin/env ruby
# frozen_string_literal: true

# Test OMQ.php in official PHP images: build bindings/php/native from this
# working tree in each image and run bindings/php/scripts/test-extension.sh
# there. Covers PHP 8.2 to 8.5, NTS and ZTS, glibc (Debian) and musl
# (Alpine). With --pie VERSION, install that release from Packagist with PIE
# instead, to check a published release.
#
# usage: ruby scripts/test-php-matrix.rb [options]
#   --all             every PHP version, NTS and ZTS, glibc and musl
#   --image IMAGE     an official php image tag, e.g. 8.4-zts-alpine (repeatable)
#   --arch ARCH       amd64 (default) or arm64 (slow under emulation)
#   --pie VERSION     install OMQ.php VERSION with PIE instead of building
#   --engine ENGINE   podman (default when installed) or docker
#   --rmi             remove images this run pulled
#   --clean           remove the Cargo cache volumes afterwards
#
# Jobs sharing a Cargo cache (same libc and arch) run one after another;
# other jobs run in parallel. Logs go to tmp/php-matrix/.

require "fileutils"
require "open3"
require "optparse"

REPO = File.expand_path("..", __dir__)
LOGS = File.join(REPO, "tmp", "php-matrix")
VERSIONS = %w[8.2 8.3 8.4 8.5].freeze
FLAVORS = %w[fpm zts fpm-alpine zts-alpine].freeze
QUICK = %w[8.4-fpm 8.4-zts-alpine].freeze

options = { images: [], arch: "amd64", engine: nil, pie: nil, rmi: false, clean: false }
OptionParser.new do |opts|
  opts.on("--all") { options[:images] = VERSIONS.product(FLAVORS).map { |v, f| "#{v}-#{f}" } }
  opts.on("--image IMAGE") { |image| options[:images] << image.delete_prefix("php:") }
  opts.on("--arch ARCH", %w[amd64 arm64]) { |arch| options[:arch] = arch }
  opts.on("--pie VERSION") { |version| options[:pie] = version }
  opts.on("--engine ENGINE", %w[podman docker]) { |engine| options[:engine] = engine }
  opts.on("--rmi") { options[:rmi] = true }
  opts.on("--clean") { options[:clean] = true }
end.parse!
images = options[:images].empty? ? QUICK : options[:images].uniq
engine = options[:engine] || (system("command -v podman >/dev/null 2>&1") ? "podman" : "docker")
arch = options[:arch]

def run_logged(cmd, log, stream)
  File.open(log, "w") do |file|
    Open3.popen2e(*cmd) do |_stdin, output, wait|
      output.each_line do |line|
        file.write(line)
        $stdout.write(line) if stream
      end
      wait.value.success?
    end
  end
end

def cache_key(image, arch) = "#{image.include?("alpine") ? "musl" : "glibc"}-#{arch}"

def docker_args(image, arch, pie)
  args = ["--rm", "--platform", "linux/#{arch}", "-v", "#{REPO}:/src:ro"]
  if pie
    args += ["-e", "OMQ_PIE_VERSION=#{pie}"]
    # PIE finds release assets through the GitHub API, which rate-limits
    # anonymous calls.
    args += ["-e", "GITHUB_TOKEN"] if ENV["GITHUB_TOKEN"]
  else
    key = cache_key(image, arch)
    args += ["-v", "omq-php-cargo-#{key}:/cargo", "-v", "omq-php-target-#{key}:/target",
             "-e", "CARGO_HOME=/cargo", "-e", "RUSTUP_HOME=/cargo/rustup", "-e", "CARGO_TARGET_DIR=/target"]
  end
  args + ["docker.io/library/php:#{image}", "sh", "/src/bindings/php/scripts/container-test.sh"]
end

FileUtils.mkdir_p(LOGS)
existing = `#{engine} images --format '{{.Repository}}:{{.Tag}}'`.split
stream = images.size == 1
results = Queue.new
groups = images.group_by { |image| options[:pie] ? image : cache_key(image, arch) }
groups.values.map do |group|
  Thread.new do
    group.each do |image|
      log = File.join(LOGS, "#{arch}-#{image}.log")
      started = Time.now
      ok = run_logged([engine, "run", *docker_args(image, arch, options[:pie])], log, stream)
      results << [image, ok, Time.now - started, log]
    end
  end
end.each(&:join)

if options[:rmi]
  images.each do |image|
    ref = "docker.io/library/php:#{image}"
    system(engine, "rmi", ref, out: File::NULL, err: File::NULL) unless existing.include?(ref)
  end
end
if options[:clean] && !options[:pie]
  volumes = images.map { |image| cache_key(image, arch) }.uniq.flat_map do |key|
    ["omq-php-cargo-#{key}", "omq-php-target-#{key}"]
  end
  system(engine, "volume", "rm", *volumes, out: File::NULL, err: File::NULL)
end

rows = []
rows << results.pop until results.empty?
source = options[:pie] ? "PIE #{options[:pie]}" : "working tree"
puts "\nOMQ.php tests (#{source}, #{arch}):"
rows.sort_by(&:first).each do |image, ok, seconds, log|
  puts format("  %-4s php:%-18s %4ds  %s", ok ? "ok" : "FAIL", image, seconds, ok ? "" : log)
end
exit(rows.all? { |row| row[1] } ? 0 : 1)
