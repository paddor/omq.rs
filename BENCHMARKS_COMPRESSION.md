# Compression Transport Benchmarks

Realistic JSON event-log payloads over TCP loopback (2-process setup).
Dictionary auto-training is off by default. When enabled, the default
dictionary capacity is 2 KiB.

Virtual throughput = msg/s x uncompressed size (effective app data rate
on a constrained link). Charts show projected throughput at 1 Gbps,
100 Mbps, and 10 Mbps.

- `lz4+tcp://`: low CPU cost, high message rate, modest wire savings.
- `zstd+tcp://`: higher CPU cost, better wire ratio. Useful when the
  link is the bottleneck.
- Auto-dict: trains once, ships once per direction per connection, then
  lowers the small-message compression threshold.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/lz4_tcp.svg" alt="PUSH/PULL lz4+tcp: projected throughput at link speed" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/zstd_tcp.svg" alt="PUSH/PULL zstd+tcp: projected throughput at link speed" width="850">
</p>

Wire formats:

- [`lz4+tcp://` RFC](doc/lz4-rfc.md)
- [`zstd+tcp://` RFC](doc/zstd-rfc.md)

### Compression thresholds

Messages below a minimum size pass through as plaintext.

| Transport | No dict | With dict |
|-----------|---------|-----------|
| lz4+tcp   | 512 B   | 64 B      |
| zstd+tcp  | 512 B   | 64 B      |

`Options::compression_threshold()` overrides the transport default.

### Dict size

Auto-trained dict capacity defaults to 2 KiB. The receiver accepts at
most 8 KiB by default.

LZ4 trains from the first 100 messages. Zstd trains after 1000 samples
or 100 KiB, whichever comes first, ignoring samples larger than 2048
bytes. Both transports ship at most one dictionary per direction per
connection.
