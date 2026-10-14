# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

manifest_only = ARGV.delete("--manifest-only")
abort "Usage: ruby scripts/check-gem.rb [--manifest-only] PATH.gem" unless ARGV.size == 1

gem_file = File.realpath(ARGV.fetch(0))
package = Gem::Package.new(gem_file)
abort "Expected an omq-rs source gem" unless package.spec.name == "omq-rs" && package.spec.platform == Gem::Platform::RUBY
version = package.spec.version.to_s

Dir.mktmpdir("omq-rs-package-") do |directory|
  unpacked = File.join(directory, "unpacked")
  package.extract_files(unpacked)
  manifest = File.join(unpacked, "ext/omq_rs_native/Cargo.toml")
  output, error, status = Open3.capture3("cargo", "read-manifest", "--manifest-path", manifest, chdir: directory)
  abort error unless status.success?
  metadata = JSON.parse(output)
  abort "Unexpected native crate" unless metadata.fetch("name") == "omq_rs_native"
  abort "Native crate version differs from gem" unless metadata.fetch("version") == version
  puts "Standalone packaged manifest: OK (omq-rs #{version})"
  next if manifest_only

  gem_home = File.join(directory, "gems")
  environment = ENV.keys.grep(/\ABUNDLE_/).to_h { |key| [key, nil] }.merge(
    "GEM_HOME" => gem_home,
    "GEM_PATH" => gem_home,
    "RUBYLIB" => nil,
    "RUBYOPT" => nil
  )
  ruby = RbConfig.ruby
  system(environment, ruby, "-S", "gem", "install", gem_file, "--no-document", chdir: directory, exception: true)
  system(environment, ruby, "-e", <<~'SMOKE', version, chdir: directory, exception: true)
    require "rubygems"
    gem "omq-rs", ARGV.fetch(0)
    require "omq/rs"

    spec = Gem.loaded_specs.fetch("omq-rs")
    raise "Loaded gem outside isolated GEM_HOME" unless spec.full_gem_path.start_with?(ENV.fetch("GEM_HOME") + "/")
    raise "Wrong gem version" unless OMQ::Rust::VERSION == ARGV.fetch(0)
    %i[ipc inproc plain curve lz4 zstd ws].each do |feature|
      raise "Missing feature: #{feature}" unless OMQ::Rust.has(feature)
    end
    public_key, secret_key = OMQ::Rust.curve_keypair
    raise "CURVE key mismatch" unless OMQ::Rust.curve_public(secret_key) == public_key

    %w[tcp lz4+tcp zstd+tcp].each do |transport|
      OMQ.rs(:pull, linger: 0, recv_timeout: 5) do |pull|
        OMQ.rs(:push, linger: 0, send_timeout: 5) do |push|
          endpoint = pull.bind("#{transport}://127.0.0.1:0")
          push.connect(endpoint).wait_for_peer(timeout: 5)
          expected = ["", "binary\x00payload".b, "large" * 2048]
          push.send(*expected)
          raise "#{transport} multipart mismatch" unless pull.recv == expected
        end
      end
    end
    puts "Installed omq-rs #{OMQ::Rust::VERSION}: features, CURVE, TCP, LZ4, Zstd OK"
  SMOKE
end
