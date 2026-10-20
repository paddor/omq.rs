# Compression Transport Benchmarks

Structured payloads over TCP between two processes. The saturation chart
sends at full speed over Linux netem links: shared 1 Gbps, 100 Mbps, and
10 Mbps with 1 ms delay in each direction, MTU 1500, and segmentation
offloads disabled. The fixed-load chart paces the sender over loopback.
Charts use the median of three measurements after active receiver warmup.
Dictionary auto-training is off by default. When enabled, the default
dictionary capacity is 2 KiB.

Both codecs cycle a prebuilt corpus of distinct messages: medium-entropy binary
records at 16/32 B, JSON events with recurring keys and varying values at
64 B and above. Receivers verify the full sequence; dictionary training uses
separate samples. Repeated-payload runs are excluded.

Payload throughput = received msg/s x uncompressed size. Sender CPU is
measured during the same receive interval; it includes application and IO.
Wire ratio = payload bytes / TCP payload bytes received in that interval.

- `lz4+tcp://` and `zstd+tcp://`: batch queued messages up to 8 KiB into arenas
  (16 KiB LZ4, 32 KiB Zstd), sharing compression history without a dictionary.
  Larger messages use per-part compression.
- Zstd adapts its level within -7..4 to the sender bottleneck.
- Auto-dict: trains once, ships once per direction per connection, then
  lowers the small-message compression threshold.

### Saturated links

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/compression_tcp.svg" alt="PUSH/PULL lz4+tcp and zstd+tcp over netem links" width="850">
</p>

### Fixed send rates

At low rates, messages leave one at a time and compress alone. Small messages
then compress well only with a dictionary, which lowers the compression
threshold to 64 B and supplies shared history. When messages queue up, as at
1M msg/s, they share arenas, and messages from 256 B compress well without a
dictionary.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/doc/charts/pushpull/compression_fixed_load.svg" alt="PUSH/PULL lz4+tcp and zstd+tcp wire ratio at fixed send rates" width="850">
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
