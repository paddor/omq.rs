# Performance verification

Run the local performance gates with:

```text
cargo run --release -p omq-tokio --features 'quic dart' --bin omq_perf_verify
```

With `.perf_hw` present, TCP PUSH/PULL covers 16B, 256B, 1KiB, and
16KiB; PUB/SUB covers 16B, 256B, 1KiB, and 4KiB with four subscribers.
Both use 1 and 2 IO threads and one application thread per side.
The verifier also measures CT REQ/REP at 256B, 32-subscriber PUB/SUB
at 256B with 2 IO threads, and 16B inproc PUSH/PULL.
Four-subscriber rates count aggregate deliveries; the 32-subscriber
gate reports the average per subscriber. Inproc shares one context;
wire cases use separate contexts. Measurement excludes tagged warmup.

With their features enabled, QUIC covers PUSH/PULL and four-subscriber
PUB/SUB; DART covers SCATTER/GATHER. Both cover 16B, 256B, and 1KiB
with 1 and 2 IO threads, and run only for configured thresholds.
Prefix the section with `quic_` or `dart_`, for example
`[dart_scattergather_1io]`. QUIC generates a fresh
trusted loopback certificate using `openssl`; UDP buffers request 4 MiB.

Thresholds are machine-specific. Create the ignored `.perf_hw` file in the
repository root. Keys match the measurement names printed by the verifier:

```text
[reqrep_ct]
p50_256b_us=50

[pushpull_1io]
16b_msgs_s=9500000
256b_msgs_s=5000000
1k_msgs_s=3000000
16k_msgs_s=250000

[pushpull_2io]
16b_msgs_s=8000000
256b_msgs_s=5000000
1k_msgs_s=3000000
16k_msgs_s=250000

[pubsub_1io]
16b_msgs_s=1500000
256b_msgs_s=1500000
1k_msgs_s=1000000
4k_msgs_s=200000

[pubsub_2io]
16b_msgs_s=1100000
256b_msgs_s=1500000
1k_msgs_s=1000000
256b_32p_msgs_s=250000
4k_msgs_s=430000

[inproc_pushpull_1io]
16b_msgs_s=1000000
```

Use measured local baselines for throughput thresholds. A
missing file runs a smaller smoke gate with loose thresholds:

```text
[reqrep_ct]
p50_256b_us=1000

[pushpull_1io]
16b_msgs_s=1000000

[pubsub_1io]
16b_msgs_s=500000

[inproc_pushpull_1io]
16b_msgs_s=1000000
```

## Profile contracts

`contract_*` cases run with and without `.perf_hw`. Each measures one
workload under both workload profiles in the same run and checks the
favored profile's rate against the other's, with a minimum ratio of 0.85:

- `contract_rr.*`: lockstep round trips over TCP favor the latency profile.
  REQ/REP, DEALER/ROUTER, and PEER run with 1 and 8 requesters, PAIR with 1,
  at 256B and 4MiB.
- `contract_stream.*`: one-way PUSH/PULL, DEALER/ROUTER, and PAIR streams at
  64B and 16KiB favor the throughput profile.

A failing ratio is measured once more, keeping each profile's best rate. A
`.perf_hw` key with the case name overrides the minimum ratio.

## Copy budget

`omq_copy_budget` checks bytes copied per message across socket patterns,
peer counts, transports, profiles, and sizes. It needs the `copy-stats`
feature and runs weekly in `extended.yml`:

```text
cargo test -p omq-tokio --features copy-stats --test omq_copy_budget
```

`OMQ_COPY_BUDGET_VERBOSE=1` prints every cell.

`scripts/test-all.sh` runs the verifier locally, skips it when `CI` or
`GITHUB_ACTIONS` is set, and can skip it locally with `OMQ_SKIP_PERF=1`.
