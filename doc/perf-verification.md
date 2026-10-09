# Performance verification

Run the fast TCP core-path gate with:

```text
cargo run --release -p omq-tokio --bin omq_perf_verify
```

With `.perf_hw` present, the verifier measures CT REQ/REP latency at
256B, canonical 1-IO PUSH/PULL at 16B, 1KiB, and 16KiB, and canonical
1-IO PUB/SUB with four subscribers at 16B and 4KiB. It also checks the
2-IO variants, a 32-subscriber 2-IO PUB/SUB 256B fan-out gate, and
16B inproc PUSH/PULL. TCP cases use separate OMQ contexts and loopback
TCP. The inproc case uses one shared `ContextCore`, matching scoped
`inproc://` semantics.
Warmup and measurement windows are bounded.

Thresholds are machine-specific. Create the ignored `.perf_hw` file in the
repository root. Keys match the measurement names printed by the verifier:

```text
[reqrep_ct]
p50_256b_us=50

[pushpull_1io]
16b_msgs_s=9500000
1k_msgs_s=3000000
16k_msgs_s=250000

[pushpull_2io]
16b_msgs_s=8000000
1k_msgs_s=3000000
16k_msgs_s=250000

[pubsub_1io]
16b_msgs_s=1500000
4k_msgs_s=200000

[pubsub_2io]
16b_msgs_s=1100000
256b_32p_msgs_s=250000
4k_msgs_s=430000

[inproc_pushpull_1io]
16b_msgs_s=1000000
```

Use measured local baselines for the twelve throughput values. A
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
