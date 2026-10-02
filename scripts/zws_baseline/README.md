# Retained-transport baseline

Run one measurement at a time, with no concurrent builds, tests, or profilers.
Requires six available logical CPUs, `taskset`, Python 3, and OpenSSL. Profiles
also require `perf`. The script stops the active peers and exits on any warning,
error, timeout, invalid result, or lost profiling samples.

Build the comparison peer with `ws plain curve lz4 zstd`, after formatting and
warning-free clippy. Copy the executable into ignored local storage before
changing production code. Record its build flags, compiler, source revision,
dependency lockfile hash, and host configuration alongside the measurements.
Each JSONL row includes the frozen binary hash. Never commit binaries or keys.

```sh
python3 scripts/zws_baseline/run.py \
  --binary reference=tmp/zws-baseline/reference --rounds 5 \
  --output ~/.cache/omq/zws-reference.jsonl

python3 scripts/zws_baseline/run.py \
  --binary reference=tmp/zws-baseline/reference \
  --binary candidate=tmp/zws-baseline/candidate --rounds 5 \
  --output ~/.cache/omq/zws-comparison.jsonl

python3 scripts/zws_baseline/run.py \
  --binary reference=tmp/zws-baseline/reference \
  --case ws-pub-server-send-4096-8-2 --rounds 1 --duration 5 \
  --profile tmp/zws-baseline/profiles \
  --output ~/.cache/omq/zws-profiles.jsonl
```

The 60-case initial matrix covers TCP/WS/WSS, both PUSH directions, 16 B through
1 MiB, PUB/SUB and fan-in with 8/32 peers and two IO threads, REQ/REP latency,
and LZ4-TCP/LZ4-WS/Zstd-TCP. `--list` prints exact cases. Case and binary order
alternate between repetitions. Listener processes use CPUs 0-2; connector
processes use CPUs 3-5. JSON payload generation is deterministic. OMQ benchmark
environment overrides are cleared except for settings selected by this script.

WSS uses a generated local certificate trusted explicitly, with hostname
verification enabled. No insecure TLS override. Delete only the generated
`tmp/zws-baseline/tls/` credentials when they expire, then regenerate by rerunning.

Measurements count receiver-delivered payloads during a fixed interval after
warmup. They do not count sender queue admission. PUB reports aggregate delivered
bytes and minimum/maximum subscriber rates, not logical publisher ingress.
Payload integrity, exact end markers, slow-reader correctness, and drop policy
belong to separate functional tests. Receiver CPU covers its measurement;
child CPU includes both processes, setup, warmup, and termination. Profiles
include setup and warmup too, so use a longer measurement interval.

Keep profile results separate from unprofiled performance comparisons. Report
per-case medians and spread. At least five matched alternating repetitions are
required for the 10% gate: throughput >=90%, p50/p99 latency <=110% of both the
original reference and preceding implementation. Uncertainty across a threshold
requires longer/quieter paired runs; a historical median alone cannot pass it.

`report.py RESULTS.jsonl` prints medians and observed ranges. Add
`--compare reference candidate` for a conservative paired gate: at least five
pairs, every observed ratio within the threshold. Mixed passing/failing ratios
are inconclusive, not a confidence interval. Missing cases, unmatched settings,
duplicate rounds, changed executable hashes, and profiled results cannot pass.
Use the same `--suite`/`--case` selection as the experiment. The current limit
is 10%; `--regression-percent 5` reproduces the previous acceptance rule.

This initial matrix needs additional coverage. Shared-path changes
also require IPC/inproc and mixed-carrier cases; lane changes require 1/N IO
thread comparisons and slow-subscriber/control-tail measurements. Real browser
and libzmq interoperability, memory/allocation bounds, and fault injection
remain separate requirements in [the transport contract](../../doc/zws.md).
