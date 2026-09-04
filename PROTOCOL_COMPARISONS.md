# Other Protocol Comparisons

These charts compare OMQ/ZMTP with other messaging and RPC protocols over TCP
loopback. They measure one flow, not horizontal scaling.

## Setup

- OMQ/ZMTP and plaintext gRPC are direct process-to-process baselines.
- NATS uses transient NATS Core messaging.
- RabbitMQ uses nonpersistent AMQP 0-9-1 messages, auto-delete queues, and
  automatic consumer acknowledgments.
- Kafka runs against Redpanda.
- Redis uses Redis Streams.
- Iggy uses Apache Iggy streams and topics.

Each data point uses an opaque byte payload. Throughput uses one sender, one
receiver, and one connection, queue, topic, or partition. Latency sends one
request at a time and measures requester-observed round-trip time. Delivery
and persistence semantics differ. The charts compare these concrete
low-overhead configurations, not equal durability guarantees.

## Producer/Consumer Throughput

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/main_mom_tcp.svg" alt="Producer/consumer throughput: direct, RPC, and brokered messaging" width="950">
</p>

## Request/Reply-Like Latency

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/main_mom_latency_tcp.svg" alt="Sequential request/reply-like latency: direct, RPC, and brokered messaging" width="850">
</p>
