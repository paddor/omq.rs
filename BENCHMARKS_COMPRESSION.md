# Compression Transport Benchmarks

Structural JSON payloads over TCP between two processes. Linux netem
emulates shared 1 Gbps, 100 Mbps, and 10 Mbps links with 1 ms delay in each
direction, MTU 1500, and segmentation offloads disabled. Charts use the
median of three measurements after active receiver warmup.
Dictionary auto-training is off by default. When enabled, the default
dictionary capacity is 2 KiB.

Payload throughput = received msg/s x uncompressed size. Sender CPU is
measured during the same receive interval; it includes application and IO.

- `lz4+tcp://`: low CPU cost, high message rate. With the 2 KiB
  dict, charted JSON payloads at 1 KiB and larger average ~3.2x wire
  reduction.
- `zstd+tcp://`: higher CPU cost, ~4.5x average wire reduction over the
  same points.
- Auto-dict: trains once, ships once per direction per connection, then
  lowers the small-message compression threshold.

### LZ4

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/lz4_tcp.svg" alt="PUSH/PULL lz4+tcp over netem links" width="850">
</p>

### Zstd

Zstd's extra compression ratio is visible, but so is the CPU cost: on
fast links it loses to LZ4 despite similar wire savings.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/zstd_tcp.svg" alt="PUSH/PULL zstd+tcp over netem links" width="850">
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
