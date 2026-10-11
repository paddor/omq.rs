# Other Protocol Comparisons

These charts compare OMQ/ZMTP with other messaging and RPC protocols on one
host. One-flow and verified multi-reader fan-out charts are separate. Aeron uses loopback UDP,
iceoryx2 uses shared memory, and iroh uses loopback QUIC; OMQ includes TCP,
QUIC, and DART with adaptive congestion control. IPC appears in both latency panels.
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
- iceoryx2 0.10.0 copies byte buffers into loaned SHM samples, borrows on receive,
  and polls continuously. History and safe overflow are disabled.
- iroh uses encrypted QUIC streams. Its throughput count measures fixed-size
  writes on a byte stream; QUIC does not preserve message boundaries.

Each data point uses an opaque byte payload. Throughput uses one sender, one
receiver, and one connection, queue, topic, or partition. Latency sends one
request at a time and measures requester-observed round-trip time. Delivery
and persistence semantics differ. The charts compare these concrete
low-overhead configurations, not equal durability guarantees. Aeron, zenoh,
iroh, and iceoryx2 each cover all 15 throughput and six latency sizes shown.
A series appears only where a measured data row exists.

OMQ TCP/QUIC throughput was refreshed on 2026-10-10. OMQ DART throughput,
matched transport latency, and iceoryx2 were measured on 2026-10-11 on `er-dev`.
Other external baselines retain their 2026-10-02 runs;
cache rows were imported unchanged from `wu-dev`. Both hosts report an
Intel Core i7-8700B CPU.
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
zenoh); its p50 and p99.9 come from that same run. iceoryx2 uses one latency
pass. Its throughput checks message size, sequence, and a sentinel without
reading every payload byte; GB/s describes logical delivery.

The 2026-10-02 external latency runs used a KVM host `halt_poll_ns` of 2 ms. With the 200 μs
default, waking a blocked thread on this VM cost several times more once its
vCPU had idled longer than that, which inflated round trips that block on
every hop.

## Producer/Consumer Throughput

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/throughput.svg" alt="Producer/consumer throughput: direct, RPC, and brokered messaging" width="950">
</p>

## Request/Reply-Like Latency

OMQ uses CLIENT/SERVER, one owned IO thread, and the latency workload profile.
The first panel uses no explicit receive spin. The second uses 50 us OMQ receive
spin; DART also spins its IO task for 50 us. Aeron and iceoryx2 applications poll
continuously. Colors identify the same implementation in both panels.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/latency.svg" alt="Echo RTT without explicit receive spinning" width="850">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/latency_spin.svg" alt="Echo RTT with receive spinning" width="850">
</p>

Lines show p99 round-trip latency. Whiskers span p50 to p99.9; values above
the linear axis are labeled at the plot edge.
IPC is a 2026-10-11 baseline from the recorded OMQ binary. IPC latency is being
optimized; these measurements are not an achievable-latency ceiling.

## Verified Pub/Sub Fan-out

One publisher process sends the same message to 1, 4, or 32 subscriber sockets
or handles in one receiver process. Each reader has an application thread.
OMQ sockets share one context and IO thread, matching main's PUB/SUB layout.
Each subscriber verifies size, sequence, count, and a sentinel;
every offered message must reach every reader. Lines show the median of three
3-second windows after 1-second warmup. Whiskers span those windows.

Rates are publications/s per reader, including the slowest reader's drain:
`offered / max(sender elapsed, slowest reader elapsed)`. Aggregate delivered
messages/s equals this rate times reader count. GB/s is logical delivery;
receivers do not scan every payload byte. Publication copies in; iceoryx2
borrows on receive. These rates differ from the existing one-reader workloads.
The process layout follows main; this harness copies a changing sequence into
each publication and verifies delivery, so it is not a regression comparison
against main's constant-payload benchmark.

The publisher uses CPUs 1-2; the receiver uses CPUs 3-4.
32 subscribers measures oversubscription within four vCPUs. OMQ/libzmq use
no-drop fan-out, zenoh blocks on congestion, and iceoryx2 disables overflow
and history. DART uses RADIO/DISH with a shared group. libzmq sockets share one
context; zenoh subscribers share one session. Aeron subscribers share one client
and UDP image, with one SHARED driver per process. Aeron IPC uses one
publisher-owned driver; its mapped logs live in `/dev/shm`. zenoh and Aeron can
coalesce the network copy before local fan-out. This is a two-process logical
subscriber comparison, not fan-out to separate remote hosts.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/fanout_network.svg" alt="Verified fan-out over loopback TCP, QUIC, and UDP" width="950">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/other-moms/doc/charts/moms/fanout_local.svg" alt="Verified fan-out over local IPC and shared memory" width="950">
</p>

These charts compare direct pub/sub implementations. The existing iroh/gRPC
peers measure streams/RPC; RabbitMQ's single queue and Redis Streams consumer
groups distribute work. Their one-flow results remain above. Broker fan-out,
including loss-bounded NATS Core, needs its own matched adapter and rate policy.
