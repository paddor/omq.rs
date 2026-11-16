#!/usr/bin/env ruby
# frozen_string_literal: true

# Export bindings/php/native into a checkout of the OMQ.php PIE repository
# (paddor/omq-binding.php) for release VERSION, then commit and tag there.
# The omq-libzmq path dependency becomes its crates.io release, so release
# omq-libzmq first. Push the result yourself; the tag starts that
# repository's release workflow.
#
# usage: ruby bindings/php/scripts/export_pie.rb VERSION PATH/TO/omq-binding.php

require "fileutils"
require "json"
require "net/http"
require "uri"

REPO = File.expand_path("../../..", __dir__)
NATIVE = File.join(REPO, "bindings", "php", "native")
README = File.join(REPO, "bindings", "php", "README.md")
LIBZMQ_PATH_DEP = 'path = "../../../omq-libzmq"'
SOURCE_URL = "https://github.com/paddor/omq.rs/blob/main/bindings/php/"

def git(dir, *args)
  out = IO.popen(["git", "-C", dir, *args], &:read)
  abort "git #{args.join(" ")} failed in #{dir}" unless $?.success?
  out
end

def crate_version(manifest)
  File.read(manifest)[/^version = "([^"]+)"/, 1] or abort "no version in #{manifest}"
end

def published?(crate, version)
  uri = URI("https://crates.io/api/v1/crates/#{crate}/#{version}")
  request = Net::HTTP::Get.new(uri, "User-Agent" => "omq-release-script")
  Net::HTTP.start(uri.host, uri.port, use_ssl: true) { |http| http.request(request) }.is_a?(Net::HTTPSuccess)
end

# Relative links in the binding README point into omq.rs.
def export_readme
  File.read(README).gsub(/\]\((?!https?:|#|mailto:)([^)]+)\)/) { "](#{URI.join(SOURCE_URL, Regexp.last_match(1))})" }
end

version, target = ARGV
abort "usage: #{$PROGRAM_NAME} VERSION PATH/TO/omq-binding.php" unless version && target
abort "bad version #{version}" unless version.match?(/\A\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?\z/)
native_version = crate_version(File.join(NATIVE, "Cargo.toml"))
abort "VERSION #{version}, bindings/php/native/Cargo.toml #{native_version}" unless version == native_version
libzmq = crate_version(File.join(REPO, "omq-libzmq", "Cargo.toml"))
abort "omq-libzmq #{libzmq} is not on crates.io; release it first" unless published?("omq-libzmq", libzmq)
abort "bindings/php has uncommitted changes" unless git(REPO, "status", "--porcelain", "--", "bindings/php").empty?
abort "#{target} is not a clean git checkout" unless git(target, "status", "--porcelain").empty?
abort "#{target} already has tag #{version}" unless git(target, "tag", "--list", version).empty?

git(target, "ls-files", "-z").split("\0").each { |path| FileUtils.rm_f(File.join(target, path)) }
git(NATIVE, "ls-files", "-z", ".").split("\0").each do |path|
  next if path == "Cargo.lock"

  FileUtils.mkdir_p(File.dirname(File.join(target, path)))
  FileUtils.cp(File.join(NATIVE, path), File.join(target, path))
end
manifest = File.join(target, "Cargo.toml")
cargo = File.read(manifest)
abort "Cargo.toml has no omq-libzmq path dependency" unless cargo.include?(LIBZMQ_PATH_DEP)
File.write(manifest, cargo.sub(LIBZMQ_PATH_DEP, %(version = "=#{libzmq}")))
FileUtils.cp(File.join(REPO, "LICENSE"), File.join(target, "LICENSE"))
File.write(File.join(target, "README.md"), export_readme)
system("cargo", "generate-lockfile", chdir: target, exception: true)

commit = git(REPO, "rev-parse", "HEAD").strip
git(target, "add", "--all")
git(target, "commit", "--quiet", "-m", "OMQ.php #{version}", "-m",
    "Exported from paddor/omq.rs #{commit} with omq-libzmq #{libzmq}.")
git(target, "tag", "--annotate", version, "-m", "OMQ.php #{version}")
puts "exported OMQ.php #{version} (omq-libzmq #{libzmq}) to #{target}"
puts "push: git -C #{target} push origin HEAD #{version}"
