# Other Protocol Comparisons

These charts compare OMQ/ZMTP with other messaging and RPC protocols over
loopback. They measure one flow, not horizontal scaling. Aeron uses UDP and
iroh uses QUIC; OMQ's charts include TCP, QUIC, and DART with adaptive
congestion control.
External benchmark adapters and run instructions remain on the `other-moms`
branch.

## Setup

- OMQ/ZMTP and plaintext gRPC are direct process-to-process baselines.
- NATS uses transient NATS Core messaging.
- RabbitMQ uses nonpersistent AMQP 0-9-1 messages, auto-delete queues, and
  automatic consumer acknowledgments.
- Redis uses Redis Streams.
- Aeron uses UDP channels, one SHARED Media Driver per process, and polling
  applications.
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

OMQ was refreshed on 2026-10-10; external baselines retain their 2026-10-02 runs.
The VM has six vCPUs mapped to six distinct physical cores. Direct endpoints
ran in separate processes on disjoint vCPU sets (requester/sender 1-2,
responder/receiver 3-4). Brokered
senders used vCPU 1, receivers vCPU 2, and brokers vCPUs 3-5. No benchmark
peer or broker ran on vCPU 0. NATS used an 8 MiB `max_payload` setting.

Throughput uses receiver-side 3-second timed windows, with three passes for
OMQ and the direct rivals. OMQ TCP/QUIC warm up for 500 ms, DART and the other
direct rivals for 1 second, and Aeron's JVM for 3 seconds. Latency uses 2,000
warmup exchanges (200,000 for Aeron's JVM) and 10,000 measured round trips per size.
The plotted latency row is the middle p99 run from three passes (five for
zenoh); its p50 and p99.9 come from that same run.

The external latency runs used a KVM host `halt_poll_ns` of 2 ms. With the 200 μs
default, waking a blocked thread on this VM cost several times more once its
vCPU had idled longer than that, which inflated round trips that block on
every hop.

## Producer/Consumer Throughput

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/throughput.svg" alt="Producer/consumer throughput: direct, RPC, and brokered messaging" width="950">
</p>

## Request/Reply-Like Latency

OMQ uses CLIENT/SERVER, one owned IO thread, and the latency workload profile.
The first panel uses no explicit receive spin. The second uses 50 μs OMQ receive
spin; DART also spins its IO task for 50 μs. Aeron's applications poll
continuously. Colors identify the same implementation in both panels.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/latency.svg" alt="Echo RTT without explicit receive spinning" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/latency_spin.svg" alt="Echo RTT with receive spinning" width="850">
</p>

Lines show p99 round-trip latency. Whiskers span p50 to p99.9; values above
the linear axis are labeled at the plot edge.
