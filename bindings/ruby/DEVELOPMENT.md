# omq-rs development

Run commands from the repository root unless a section changes directory.

## Prerequisites

- Ruby 3.3 or newer with Bundler.
- Rust 1.93 or newer with Cargo, rustfmt, and Clippy.
- Python with pyzmq for the Python interoperability tests.
- CZMQ and libzmq development files for cztop interoperability and both
  benchmark baselines.

Install the Ruby dependencies:

```sh
cd bindings/ruby
bundle install
```

## Build and test

Run the normal binding test pass from the repository root:

```sh
./scripts/test-ruby.sh
```

Select a Ruby executable explicitly with `OMQ_RUBY`:

```sh
OMQ_RUBY=/path/to/ruby ./scripts/test-ruby.sh
```

The equivalent commands from the binding directory are:

```sh
cd bindings/ruby
bundle exec rake
```

Compile once, then run one test file:

```sh
bundle exec rake compile
bundle exec ruby -Ilib:test test/test_features.rb
```

The suite covers socket patterns, options, transports, CURVE, PLAIN,
compression, monitors, Fiber schedulers, Ractors, lifecycle behavior, and
pyzmq interoperability. pyzmq tests skip when pyzmq is unavailable. cztop
tests skip when CZMQ or cztop is unavailable. Ractor tests require Ruby 4.

## Soak tests

Run the mixed transport, security, compression, churn, and resource soak:

```sh
bindings/ruby/scripts/soak.sh 1h
```

The duration accepts `s`, `m`, `h`, or `d`. The test reports progress, RSS,
file descriptors, and Ruby thread counts. It fails on message corruption,
stalls, or sustained resource growth.

`scripts/test-all.sh` includes the Ruby pass in the full repository test run.

## Static checks

Run the same native checks as CI:

```sh
cargo fmt --manifest-path bindings/ruby/Cargo.toml --all --check
cargo clippy --manifest-path bindings/ruby/Cargo.toml \
  --all-targets --all-features -- -D warnings
```

Check Ruby syntax:

```sh
find bindings/ruby/lib bindings/ruby/test bindings/ruby/scripts \
  -name '*.rb' -print0 | xargs -0 -n1 ruby -cw
ruby -cw bindings/ruby/Rakefile
ruby -cw bindings/ruby/omq-rs.gemspec
```

Check public YARD coverage from `bindings/ruby` after installing YARD:

```sh
yard stats --no-save --no-cache --list-undoc --no-private \
  'lib/**/*.rb' 'lib/*.rb'
```

## Benchmarks

Run benchmarks on an otherwise idle machine. The harness starts separate Ruby
processes over TCP. PUSH/PULL throughput covers 16 B through 32 KiB. REQ/REP
mean latency covers 16 B through 4 KiB. The default run keeps the median of three
rounds for each implementation, pattern, and message size. Throughput uses a
0.5 second warmup and 2.5 second measurement window. Latency uses a 0.5 second
warmup and 1.5 second measurement window.

From the binding directory:

```sh
cd bindings/ruby
bundle exec rake compile
bundle exec ruby -Ilib scripts/update_perf.rb
```

The default run benchmarks `omq-rs`. It includes cztop when CZMQ is available
and ffi-rzmq when libzmq is available. Missing baselines are skipped.

Useful controls:

```sh
bundle exec ruby -Ilib scripts/update_perf.rb --quick
bundle exec ruby -Ilib scripts/update_perf.rb --impl omq-rs,cztop,ffi-rzmq
bundle exec ruby -Ilib scripts/update_perf.rb --patterns pushpull,reqrep
bundle exec ruby -Ilib scripts/update_perf.rb --sizes 16,128,1024,4096
bundle exec ruby -Ilib scripts/update_perf.rb --rounds 5
bundle exec ruby -Ilib scripts/update_perf.rb --duration 2.5 --warmup-duration 0.5
bundle exec ruby -Ilib scripts/update_perf.rb --latency-duration 1.5 --latency-warmup-duration 0.5
bundle exec ruby -Ilib scripts/update_perf.rb --no-record
```

`--quick` runs one round at 128 B and 1 KiB without recording results or
rewriting the chart.

## Chart generation

Benchmark rows append to:

```text
~/.cache/omq-rs/bindings.jsonl
```

Regenerate the SVG from the latest cached row for each implementation,
pattern, and message size:

```sh
cd bindings/ruby
bundle exec ruby -Ilib scripts/update_perf.rb --chart-only
```

The output is `bindings/ruby/doc/charts/bindings.svg`. Its hardware subtitle
comes from the repository-root `.chart_hw` file:

```text
prefix=Linux VM on a 2018 Mac Mini
postfix=6 cores, performance governor, turbo off
```

`.chart_hw` is local-only and gitignored.

## Packaging and release

Build the source gem from the binding directory:

```sh
cd bindings/ruby
bundle exec rake build
```

The gem is written to `bindings/ruby/pkg/`. Publication runs only through
`.github/workflows/release-rubygems.yml` using RubyGems trusted publishing.
The release tag must match the gem version:

```text
ruby-v<VERSION>
```
