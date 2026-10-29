# Development

Benchmarks and charts: [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md).
Fuzz tests, soak tests, and releases: [RELEASING.md](RELEASING.md).

## Building and linting

```sh
cargo build --workspace
cargo fmt
cargo clippy --workspace --all-targets
cargo clippy --workspace --all-targets --all-features
(cd bindings/pyomq && cargo clippy --all-targets)
```

Release, soak, and benchmark builds use `target-cpu=native` from
`.cargo/config.toml`. Lints: `missing_debug_implementations` and
`unsafe_op_in_unsafe_fn` deny; clippy `pedantic` warns.

## Tests

```sh
cargo test -p omq-proto
cargo test -p omq-tokio
cargo test -p omq-tokio --test omq_req_rep -- some_test_name
cargo test -p omq-tokio --features plain --test omq_plain
cargo test -p omq-tokio --features curve --test omq_curve
cargo test -p omq-tokio --features lz4 --test omq_lz4_tcp --test omq_lz4_pub_sub
cargo test -p omq-tokio --features dart --test omq_dart_transport --test omq_dart_pool
cargo test -p omq-tokio --all-features --test omq_network_rate_limit
cargo test -p omq-tokio --test omq_stress_connect_before_bind -- --test-threads=1
./scripts/test-cppzmq.sh
```

Loom and Miri:

```sh
cargo test -p omq-tokio --test omq_loom_signal
cargo test -p omq-tokio --features dart --test omq_loom_dart_pool
RUSTFLAGS="--cfg loom" cargo test -p omq-proto --features dart --test omq_proto_loom_dart
RUSTFLAGS="--cfg loom" cargo test -p omq-shm --test omq_shm_loom
cargo +nightly miri test -p omq-shm --lib
```

Full sweep:

```sh
./scripts/test-all.sh                  # everything, plus the local perf gate
OMQ_QUICK=1 ./scripts/test-all.sh      # default-feature workspace tests
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
OMQ_SKIP_PERF=1 ./scripts/test-all.sh
OMQ_LOOM=1 ./scripts/test-all.sh
```

The perf gate runs `omq_perf_verify` with thresholds from `.perf_hw`, or a
loose smoke gate without it. CI skips it. Focus it with `--list`, `--case NAME`,
`--repeat N`, or `--measure-only`; `OMQ_PERF_WARMUP_MS`, `OMQ_PERF_MEASURE_MS`,
and `OMQ_PERF_CPUS` set durations and CPUs. See
[perf-verification.md](doc/perf-verification.md).

Ruby binding tests: `bundle install --gemfile bindings/ruby/Gemfile`, then
`scripts/test-ruby.sh`. Set `OMQ_RUBY` when Ruby is not on `PATH`.

## Queue crates

`yring` and `fanring` live in [fanring.rs](https://github.com/paddor/fanring.rs);
run their tests, Loom models, and Miri checks there. To test unpublished
queue changes, override them locally (after the Cargo subcommand):

```sh
cargo test --config 'patch.crates-io.yring.path="../fanring/yring"' \
  --config 'patch.crates-io.fanring.path="../fanring"' -p omq-tokio
```

From `bindings/pyomq`, use `../../../fanring` paths. Never commit these
overrides. Publish the queue crates first, then refresh binding lockfiles.

## Receive lane backpressure

PEER, PULL, and GATHER share one receipt API:

```rust
recv_with_receipt(lane: Option<&ReceiveLane>) -> Result<(ReceiveReceipt, Message)>
try_recv_with_receipt(lane: Option<&ReceiveLane>) -> Result<(ReceiveReceipt, Message)>
unshift(receipt: ReceiveReceipt, message: Message) -> Result<(), UnshiftError>
```

- `None` picks an unpaused lane fairly; `Some(lane)` targets one lane and
  returns its held message first.
- A live receipt pauses its lane: no clone, ordinary, or bulk receive can
  overtake it. Dropping the receipt resumes the lane.
- `unshift` returns the message to one side slot; the lane stays paused until
  a targeted retry accepts it.
- A paused lane fills, its connection stops reading, and the sender's buffers
  fill. There are no credits and no socket-wide budget.
- PULL/GATHER lanes hold `recv_hwm` messages; PEER lanes also hold a memory
  permit. Use `max_message_size` to bound single messages.
- Claims belong to one connection generation. After a disconnect, targeting
  that lane fails with `Closed`.

Only PEER, PULL, and GATHER expose lane claims. SUB/XSUB, SERVER,
DEALER/CLIENT, and ROUTER could reuse the same fanring path. REQ/REP would
need protocol-aware receipts. XPUB, UDP DISH, and send-only sockets are out of
scope; conflate and external receive sinks are not covered.

A paused SUB lane pauses every topic on that publisher connection. It blocks
the publisher only with `xpub_nodrop`; otherwise the publisher drops at its
HWM.

## Continuous integration

PR and `main` CI run Linux fmt, clippy, default workspace tests, DART and QUIC
MSRV checks, binding smoke tests, and OMQ.ts checks plus browser interop. The
full matrix runs Saturdays and on `workflow_dispatch`: macOS, Windows, 32-bit
Linux, compression, CURVE, pyzmq and cppzmq interop, fuzz smoke, packaging,
and binding versions.

`extended.yml` runs Sundays and on `workflow_dispatch`: long fuzz runs, one
soak job per soak test, `--ignored` stress tests, libzmq draft interop
(libzmq built with `ENABLE_DRAFTS=ON`), and Ubuntu ARM64.

Interop tests skip when their peer is missing. CI sets
`OMQ_INTEROP_REQUIRED=1` so a missing peer fails instead.
