# Compression Transport Benchmarks

Structured payloads over TCP between two processes. Linux netem
emulates shared 1 Gbps, 100 Mbps, and 10 Mbps links with 1 ms delay in each
direction, MTU 1500, and segmentation offloads disabled. Charts use the
median of three measurements after active receiver warmup.
Dictionary auto-training is off by default. When enabled, the default
dictionary capacity is 2 KiB.

Both codecs cycle a prebuilt corpus of distinct messages: medium-entropy binary
records at 16/32 B, JSON events with recurring keys and varying values at
64 B and above. Receivers verify the full sequence; dictionary training uses
separate samples. Earlier repeated-payload runs are excluded from both charts.

Payload throughput = received msg/s x uncompressed size. Sender CPU is
measured during the same receive interval; it includes application and IO.

- `lz4+tcp://` and `zstd+tcp://`: batch queued small messages into arenas
  (16 KiB LZ4, 32 KiB Zstd), sharing compression history without a dictionary.
  Larger messages use per-part compression.
- Zstd uses level 1.
- Auto-dict: trains once, ships once per direction per connection, then
  lowers the small-message compression threshold.

### LZ4

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/lz4_tcp.svg" alt="PUSH/PULL lz4+tcp over netem links" width="850">
</p>

### Zstd

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/zstd_tcp.svg" alt="PUSH/PULL zstd+tcp over netem links" width="850">
</p>

Wire formats:

- [`lz4+tcp://` RFC](doc/lz4-rfc.md)
- [`zstd+tcp://` RFC](doc/zstd-rfc.md)

### Compression thresholds

The minimum applies to each part, or to the aggregate frame bytes of a
compression arena. Smaller compression units use raw encoding.

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
