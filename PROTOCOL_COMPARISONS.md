# Other Protocol Comparisons

These charts compare OMQ/ZMTP with other messaging and RPC protocols over
loopback. They measure one flow, not horizontal scaling. The chart filenames
retain `tcp` for compatibility; Aeron uses UDP and iroh uses QUIC.

## Setup

- OMQ/ZMTP and plaintext gRPC are direct process-to-process baselines.
- NATS uses transient NATS Core messaging.
- RabbitMQ uses nonpersistent AMQP 0-9-1 messages, auto-delete queues, and
  automatic consumer acknowledgments.
- Redis uses Redis Streams.
- Aeron uses UDP channels and one Media Driver in each process.
- zenoh uses a direct TCP peer link.
- iroh uses encrypted QUIC streams. Its throughput count measures fixed-size
  writes on a byte stream; QUIC does not preserve message boundaries.

Each data point uses an opaque byte payload. Throughput uses one sender, one
receiver, and one connection, queue, topic, or partition. Latency sends one
request at a time and measures requester-observed round-trip time. Delivery
and persistence semantics differ. The charts compare these concrete
low-overhead configurations, not equal durability guarantees. Aeron, zenoh,
and iroh each cover all 15 throughput and six latency sizes shown. A series
appears only where a measured data row exists.

OMQ, gRPC, NATS, Redis Streams, Aeron, zenoh, and iroh were refreshed on
2026-10-02. RabbitMQ throughput retains the 2026-09-01 run; no RabbitMQ
latency row is plotted. NATS used an 8 MiB `max_payload` setting to cover the
largest chart sizes.

## Producer/Consumer Throughput

<p align="center">
  <img src="doc/charts/main_mom_tcp.svg" alt="Producer/consumer throughput: direct, RPC, and brokered messaging" width="950">
</p>

## Request/Reply-Like Latency

<p align="center">
  <img src="doc/charts/main_mom_latency_tcp.svg" alt="Sequential request/reply-like latency: direct, RPC, and brokered messaging" width="850">
</p>
