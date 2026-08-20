# frozen_string_literal: true

require_relative "lib/omq/rs/version"

Gem::Specification.new do |spec|
  spec.name        = "omq-rs"
  spec.version     = OMQ::Rust::VERSION
  spec.authors     = ["Patrik Wenger"]
  spec.email       = ["paddor@gmail.com"]
  spec.summary     = "Fast Ruby binding for OMQ.rs"
  spec.description = "Native Ruby binding for OMQ.rs. Provides brokerless ZeroMQ-compatible " \
                     "sockets backed by the memory-safe omq-tokio runtime."
  spec.homepage    = "https://github.com/paddor/omq.rs/tree/main/bindings/ruby"
  spec.license     = "ISC"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://www.rubydoc.info/gems/omq-rs"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.required_ruby_version = ">= 3.3"
  spec.required_rubygems_version = ">= 3.3.11"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rs,rb}",
    "ext/**/Cargo.toml",
    "README.md",
    "DEVELOPMENT.md",
    "LICENSE",
    "CHANGELOG.md",
  ]
  spec.require_paths = ["lib"]
  spec.extensions    = ["ext/omq_rs_native/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9"
end
