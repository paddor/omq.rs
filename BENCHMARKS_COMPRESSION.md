# Compression Transport Benchmarks

Realistic JSON event-log payloads over TCP loopback (2-process setup).
LZ4 default compression. Dictionary auto-training is off by default
(2 KiB capacity when enabled).

Virtual throughput = msg/s x uncompressed size (effective app data rate
on a constrained link). Charts show projected throughput at 10 Gbps,
1 Gbps, 100 Mbps, and 10 Mbps.

- **Auto-dict off by default:** when enabled, the encoder trains a
  2 KiB dict from the first 100 messages, ships it once, then uses it
  for all subsequent compression. Small-message throughput improves
  significantly with dict.
- **Pure Rust:** lz4+tcp uses lz4rip, no C compiler required.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pubsub/lz4_tcp.svg" alt="PUB/SUB lz4+tcp fan-out: projected throughput at link speed" width="850">
</p>

### Compression thresholds

Messages below a minimum size skip compression entirely and pass
through as plaintext. The defaults reflect extensive benchmarking
across link speeds and dict sizes:

| Transport | No dict | With dict |
|-----------|---------|-----------|
| lz4+tcp   | 512 B   | 128 B     |

Operators on high-bandwidth links who send many small messages can
raise the threshold further via `Options::compression_threshold()` to
avoid the CPU overhead of compressing messages that already fit in a
single packet.

### Dict size

When enabled, auto-trained dict capacity defaults to 2 KiB. Benchmarks across dict
sizes (256 B to 8 KiB) show that a 2 KiB dict captures most of the
compressible structure in typical JSON payloads. Larger dicts (4-8 KiB)
produce marginally better wire ratios at 2 KiB+ message sizes but hurt
throughput at smaller sizes due to L1/L2 cache pressure during
compression. The default max accepted dict size from a peer is 8 KiB.

<details><summary>Test environment</summary>

Linux 6.12 (Debian 13) VM, Intel i7-8700B 3.2 GHz (turbo off,
governor=performance, 6 vCPU), Rust 1.95.0. Min wall time across
multiple runs with warmup. Link-speed projections computed from
measured compression ratio and CPU-limited throughput.

</details>

## Running the benchmarks

One bench run at full loopback speed measures CPU-limited msg/s and
wire bytes per message. The chart scripts project throughput at each
link speed: `effective_msgs_s = min(cpu_msgs_s, link_bytes_s / wire_bytes)`.
No kernel-level rate limiting is used; slow-link simulation via `tc`/`netem`
is unreliable on loopback due to kernel buffering.

```sh
cargo bench -p omq-tokio  --features lz4 --bench compression
cargo bench -p omq-compio --features lz4 --bench compression

# Generate charts
python3 scripts/gen_compression_chart.py --backend tokio
python3 scripts/gen_compression_chart.py --backend compio
```

Environment variables:

- `OMQ_BENCH_SIZES` -- override payload sizes (default: chart sizes, 8 B to 256 KiB)
- `OMQ_BENCH_ROUNDS` / `OMQ_BENCH_ROUND_MS` -- tune measurement duration
- `OMQ_BENCH_COMPRESSION_THRESHOLD` -- override minimum payload size for compression
- `OMQ_BENCH_DICT_SIZES` -- override dict sizes to bench (default: 2048; e.g. `256,512,1024,2048,4096,8192`)
