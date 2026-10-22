# Development

Benchmarks and charts: [RUNNING_BENCHMARKS.md](RUNNING_BENCHMARKS.md).
Fuzz tests, soak tests, and releases: [RELEASING.md](RELEASING.md).

## Building And Linting

```sh
cargo build --workspace
cargo clippy --workspace --all-targets
```

Release, soak, and benchmark builds use the local CPU:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "target-cpu=native"]
```

Lints: `missing_debug_implementations` = **deny**,
`unsafe_op_in_unsafe_fn` = **deny**, clippy `pedantic` = **warn**.

## Unit And Integration Tests

```sh
cargo test -p omq-tokio
cargo test -p omq-proto
cargo test -p omq-tokio --test omq_req_rep -- some_test_name
./scripts/test-cppzmq.sh
```

Feature-gated tests:

```sh
cargo test -p omq-tokio --features plain     --test omq_plain
cargo test -p omq-tokio --features curve     --test omq_curve
cargo test -p omq-tokio --features lz4       --test omq_lz4_tcp --test omq_lz4_pub_sub
cargo test -p omq-tokio --features dart     --test omq_dart_transport --test omq_dart_pool
cargo test -p omq-tokio --all-features --test omq_network_rate_limit
```

Loom checks:

```sh
cargo test -p omq-tokio --test omq_loom_signal
cargo test -p omq-tokio --features dart --test omq_loom_dart_pool
RUSTFLAGS="--cfg loom" cargo test -p omq-proto --features dart --test omq_proto_loom_dart
RUSTFLAGS="--cfg loom" cargo test -p omq-shm --test omq_shm_loom
cargo +nightly miri test -p omq-shm --lib
```

The `omq-tokio` Loom test models `StateSignal` and `DataSignal` lost-wake
races. The reusable queue crates are maintained in
[fanring.rs](https://github.com/paddor/fanring.rs). Run their unit tests,
Loom models, and Miri checks in that workspace.
Weekly extended CI checks SHM's production synchronization with Loom and
storage ownership with Miri; Linux tests cover kernel and process behavior.

For coordinated local changes, override published queue dependencies explicitly:

```sh
cargo test --config 'patch.crates-io.yring.path="../fanring/yring"' \
  --config 'patch.crates-io.fanring.path="../fanring"' -p omq-tokio
```

OMQ requires `fanring` 0.3.9 and `yring` 0.3.20 for
`Consumer::release_with_full()` and `AsyncProducer::poll_ready()`.
Workspace and binding builds resolve these APIs from the registry. Put
`--config` after the Cargo subcommand so clippy and nextest forward local
overrides. In `bindings/pyomq`, use paths
`../../../fanring/yring` and `../../../fanring` for the same overrides.

Keep these overrides local. Registry dependencies must be published before OMQ
CI or packaging can resolve them without overrides. Refresh binding lockfiles
against the registry after publishing a new queue version. Weekly CI covers
native and Python binding tests on Windows, Linux, and macOS.

Full sweep:

```sh
OMQ_QUICK=1 ./scripts/test-all.sh      # default-feature workspace tests
./scripts/test-all.sh
OMQ_SKIP_PYOMQ=1 ./scripts/test-all.sh
OMQ_SKIP_PERF=1 ./scripts/test-all.sh
OMQ_LOOM=1 ./scripts/test-all.sh
```

The full sweep runs `omq_perf_verify` locally. `.perf_hw` supplies hardware
thresholds; if absent, a smaller smoke gate runs. CI skips the gate.

Use `--list`, `--case NAME`, `--repeat N`, or `--measure-only` for focused checks.
`OMQ_PERF_WARMUP_MS` and `OMQ_PERF_MEASURE_MS` set durations;
`OMQ_PERF_CPUS` sets an ordered Linux CPU list. The verifier excludes warmup,
times bounded batches, and reports rates using actual elapsed time.
See [performance verification](doc/perf-verification.md) for threshold settings,
profile contracts, and the copy budget.

### Ruby Binding Tests

```sh
bundle install --gemfile bindings/ruby/Gemfile
scripts/test-ruby.sh
```

Set `OMQ_RUBY=/path/to/ruby` when Ruby is not on `PATH`.

## Receive lane backpressure

### Current contract

PEER, PULL, and GATHER expose the same API:

```rust
recv_with_receipt(lane: Option<&ReceiveLane>) -> Result<(ReceiveReceipt, Message)>
try_recv_with_receipt(lane: Option<&ReceiveLane>) -> Result<(ReceiveReceipt, Message)>
unshift(receipt: ReceiveReceipt, message: Message) -> Result<(), UnshiftError>
```

`receipt.lane()` exposes the claimed lane. `None` selects an unpaused lane fairly.
`Some(lane)` selects that lane, returning its held message first. A live receipt claims
that lane and prevents other socket clones, ordinary receives, and bulk
receives from overtaking it. Drop the receipt after admitting or discarding the
message to resume drainage. `unshift` transfers the claim and message into one
OMQ-owned side slot; the lane stays paused until a targeted retry is accepted.

Fanring stores a Boolean, not the popped message. The paused ring fills, the
connection stops reading, and its upstream buffers eventually fill. Resumption
makes the lane eligible for drainage; existing ring release batches return
consumed slots. There are no application credits or capacity grants.

PEER receipts retain the existing per-lane memory permit. PULL/GATHER retain
their message-count HWM behavior: a bounded ring, at most one claimed or held
message, and one pending message in each wire driver. Their receipt records the
original backing-storage size so a returned message cannot enlarge the side
slot. Set `max_message_size` to bound individual messages; its default for
PULL/GATHER remains unlimited. No socket-wide admission budget couples peers.

PULL/GATHER use one socket-owned fanring for wire and inproc connections. Wire
lanes use receiver HWM. Inproc lanes use sender HWM plus receiver HWM, rounded
up to a power of two. Inproc sends enqueue from the calling thread and receives
drain from the application thread. No IO task relays a message. Lane metadata
lives once per connection; plain slots contain only `Message`. Tagged receives
use fanring's borrowed `with_lane_ids()` view.

Inproc publications use the socket's existing fenced `DataSignal` instead of
also marking fanring's ready index. Blocking wakes reuse that publication fence.
Ready scalar receives skip the signal fence; an empty drain fences and rechecks
before allowing a wait. Scalar drains poll all unpaused lanes every 64 received
messages and recheck all lanes before reporting empty. Bulk drains poll before
each bounded batch. Wire drivers retain indexed batch publication.
The readiness poll changes no ring capacity or application admission state.

Targeted waits observe only their lane's data, claim, and lifecycle signals.
Unrelated lanes do not wake them. Cancellation consumes no message. After
disconnect is observed, targeting or returning to that lane fails with
`Closed`; it cannot select a replacement. PULL/GATHER discard a held retry on
disconnect while ordinary receives preserve an already published queue prefix.
Invalid returns preserve the caller's message through `UnshiftError`.

### Feasibility across all 20 socket types

These assessments follow the existing receive paths in `socket/handle.rs`,
`socket/actor/peer_materialize.rs`, `socket/recv.rs`, `routing/mod.rs`, and
`socket/udp.rs`. They describe possible extensions; only PEER, PULL, and GATHER
currently expose physical lane claims.

| Receiving type | Feasibility | Work and constraints |
|---|---|---|
| PEER | Implemented | Keep identity routing, generation fencing, and retained-memory permits. |
| PULL / GATHER | Implemented | Wire and direct inproc lanes share one fair drain. GATHER retains single-frame semantics. |
| SUB / XSUB | Straightforward next | Reuse the PULL/GATHER fanring and direct inproc path. Apply filters before admission. Subscription commands must continue while a data lane is paused. Preserve XSUB raw subscription sends. |
| SERVER | Feasible, small structural change | Register native inbound peers in lane-aware fanring. Keep `RecvSink::Server` attaching routing IDs; keep replies independent of inbound drainage. Physical lane IDs must not alias reused routing IDs. |
| DEALER / CLIENT | Feasible | Register inbound peers in the common lane-aware fanring. Preserve CLIENT's single-frame contract. Receive lane selection does not change existing outbound round-robin routing. |
| ROUTER | Feasible, more work | Move wire identity receive processing into a per-connection sink/drain, preserving identities, mandatory sends, and handover policy. Adapt direct inproc identity prefixes. Current `IdentitySocket` lane receives remain inert on ROUTER. |
| PAIR / CHANNEL | Feasible, limited fan-in benefit | One physical connection only. A held message can still support application admission retries. CHANNEL remains single-frame. |
| REQ / REP | Requires protocol-specific receipts | Coordinate retry and receipt acceptance with request correlation, alternation, REP envelopes, and saved reply routes. Existing `recv` admits protocol state before returning; a generic queue pause alone is insufficient. |
| DISH | Transport-dependent | TCP/inproc could pause per RADIO connection after group filtering, with a blocking sender policy. UDP cannot propagate lossless backpressure to a remote sender. |
| STREAM | Feasible with a dedicated adapter | Pause per TCP connection and preserve chunk FIFO. Keep connection/disconnection notifications reachable separately from paused byte data. A chunk is not a ZMTP message. |
| XPUB | No generic data-lane extension | Application receives subscription notifications. Subscription processing is control traffic and must remain reachable. Holding a notification must not pause processing peer subscriptions/cancellations. |
| PUSH / SCATTER | Not applicable | Application sends only. Existing outbound mute and HWM handling already propagate pressure from receivers. |
| PUB / RADIO | Not applicable to application receive | Application sends only. Their peer commands remain control traffic. Publish admission follows the configured drop or block policy. |

### SUB with a nondropping publisher

Pausing one SUB publisher connection can bound application backlog while
other publisher connections continue. With `xpub_nodrop` enabled on the PUB or
XPUB peer, filling that lane's receive path eventually blocks upstream
publication. Default PUB/XPUB drop behavior can lose publications once its
outbound HWM is reached. The receiver cannot change that policy or recover
already dropped messages. This matches the publisher-side
[ZMQ_XPUB_NODROP contract](https://zeromq.github.io/libzmq/zmq_setsockopt.html).

The pause granularity is one physical publisher connection, not one topic. If
one topic is held, other topics multiplexed on that connection also pause.
Separate connections provide separate lanes.

Nondropping fan-out also has a publisher-side consequence: a publication that
matches a stalled subscriber cannot complete until every required delivery
can progress. OMQ services ready peers independently while a blocking send is
pending, but the same sequential caller cannot submit its next publication
until that send completes. Concurrent nonblocking publication reserves all
required targets before publishing, preventing partial delivery on retry.
Per-lane receive pausing cannot promise independent publication throughput
for subscribers sharing one publisher and message stream.

### Implementation order

1. PULL/GATHER across every native connection path.
2. SUB/XSUB: enable the same fanring path and test PUB/XPUB both with and without
   `xpub_nodrop`, topic matching, subscription changes, and reconnect replay.
3. SERVER, then DEALER/CLIENT: reuse the adapter while preserving receive-side
   routing metadata and reverse-direction replies under saturation.
4. ROUTER: move identity transforms out of actor data delivery and test both
   handover policies, stale receipts, and `IdentitySocket` behavior.
5. Optional PAIR/CHANNEL and STREAM adapters. Give REQ/REP a separate receipt
   admission design. Keep XPUB subscription handling and UDP outside this
   lossless data-lane contract.

Conflate uses replacement semantics and cannot offer FIFO lane claims.
External receive sinks own drainage outside `Socket`; lane pausing needs a
separate adapter agreed with that consumer. Neither path is enabled implicitly.

## Stress Tests

```sh
cargo test -p omq-tokio --test omq_stress_connect_before_bind -- --test-threads=1
```

## Continuous Integration

PR and `main` CI runs Linux fmt, clippy, default workspace tests, MSRV
checks for DART and QUIC, and the current binding smoke tests on Linux.
The full platform and feature matrix runs Saturdays at 04:00 UTC and on
`workflow_dispatch`. It covers macOS, Windows, 32-bit Linux, compression,
CURVE, interop, packaging, and binding version matrices. macOS test jobs
run serially with `--test-threads=1`. Extended CI adds Ubuntu ARM64.

The scheduled `.github/workflows/ci.yml` run also runs, on Linux only:

| job | what |
|-----|------|
| `interop` | pyzmq NULL/STREAM + PLAIN + CURVE |
| `cppzmq` | `cppzmq` API tests against `libomq_zmq` |
| `fuzz-smoke` | parsers at 1M iters, socket actions at 200 |

`.github/workflows/extended.yml` runs Sundays at 03:00 UTC and on
`workflow_dispatch` (with `soak_duration_secs` / `fuzz_scale` inputs):
the fuzz targets at 100M / 2000 iters, the soak suite (one job per soak test),
the `--ignored` stress tests, libzmq draft interop (`ws://`,
RADIO/DISH, and draft socket types).

The interop tests skip when their peer is missing so local runs stay
green without pyzmq or the libzmq helper binaries. CI sets
`OMQ_INTEROP_REQUIRED=1`, which turns a missing peer into a failure so
the job cannot pass without testing anything.

The libzmq draft interop job builds libzmq from source with
`ENABLE_DRAFTS=ON` (distro packages omit it, and without drafts libzmq
has no `ws://` transport or draft socket API) and caches the install.
