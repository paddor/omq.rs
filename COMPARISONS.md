# Comparisons

These charts compare OMQ with `libzmq`, `tmq`, `zmq.rs`, and `rzmq`. The
benchmark runner records throughput, latency, CPU time, and peer fairness where
the pattern has multiple peers.

Comparisons with other messaging and RPC protocols live in
[PROTOCOL_COMPARISONS.md](https://github.com/paddor/omq.rs/blob/other-moms/PROTOCOL_COMPARISONS.md).

## Setup

- `libzmq v4.3.5`
- `tmq v0.5.0`
- `zeromq v0.6.0`
- `rzmq v0.5.24` in its normal and io_uring modes
- OMQ from this repository

## Methodology

TCP and IPC charts run communicating sides in separate processes.

- Two-peer charts use two processes.
- PUB/SUB charts use two processes, with all subscriber sockets in one
  receiver process. Receive workers drain disjoint socket subsets.
- PUSH/PULL fan-in/fan-out charts use one process for each pusher or puller.
- `inproc` charts stay inside one process by definition.

PUB/SUB and PUSH/PULL multi-peer charts report total throughput. PUSH fan-out
charts also show peer fairness: whiskers mark the slowest and fastest puller
in a measured round.

Transport coverage differs by implementation. Missing lines mean that
implementation does not expose a usable peer for that transport and
pattern in this benchmark suite.

## Runtime modes

The charts benchmark three OMQ execution styles where relevant:
`blocking::Socket` with dedicated IO threads, Tokio with two background IO
threads, and Tokio current-thread (CT), where application and IO work share
one runtime thread. The benchmark peer on the uninteresting side uses the
blocking API.

### Thread labels

- **IO** keeps application code out of the IO runtime and scales
  linearly across independent IO threads.
- **UT** means user thread: an application OS thread that owns a
  socket. The canonical inproc setup uses separate user threads for the
  communicating sockets.

`Context::current()` embeds OMQ in an existing tokio runtime:

- **CT** means current-thread Tokio: application tasks and OMQ IO share one
  current-thread runtime and one OS thread.
- **MT** means machine-threaded: the implementation may use the available
  machine parallelism. The chart shows the detected CPU count.

## PUSH/PULL Throughput

<p align="center">
  <img src="doc/charts/main_pushpull_tcp.svg" alt="PUSH/PULL throughput: TCP implementations" width="950">
</p>

## Producer/Consumer Throughput

Direct ZMTP/TCP, gRPC streaming over HTTP/2, brokered queues, Redis Streams,
zenoh/TCP, Aeron/UDP, and iroh/QUIC over loopback. Transport and delivery
semantics differ; the legend identifies each transport.
See [other protocol comparisons](PROTOCOL_COMPARISONS.md) for the setup and
latency chart.

<p align="center">
  <img src="doc/charts/moms/throughput_network.svg" alt="Network producer/consumer throughput" width="950">
</p>

<p align="center">
  <img src="doc/charts/moms/throughput_local.svg" alt="Local producer/consumer throughput" width="950">
</p>

<p align="center">
  <img src="doc/charts/pushpull/tcp.svg" alt="PUSH/PULL throughput: TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/ipc.svg" alt="PUSH/PULL throughput: IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/inproc.svg" alt="PUSH/PULL throughput: inproc" width="850">
</p>

### Context byte limit

Two processes, one IO thread each, over a 1000 Mbit/s netem link.
The capped sender uses 800 Mbit/s egress (100 MB/s) with a 1 MB burst;
the receiver is unrestricted. The limiter counts ZMTP framing, while the
chart shows payload throughput.

<p align="center">
  <img src="doc/charts/pushpull/tcp_rate_limit.svg" alt="TCP PUSH/PULL on a 1000 Mbit/s link: uncapped OMQ versus 800 Mbit/s egress" width="950">
</p>

### Fan-Out

1-to-N PUSH/PULL over TCP. Whiskers show the slowest and fastest
puller in a measured round.

<p align="center">
  <img src="doc/charts/pushpull/fanout/tcp.svg" alt="PUSH fan-out: TCP" width="850">
</p>

### Fan-In

N-to-1 PUSH/PULL over TCP.

<p align="center">
  <img src="doc/charts/pushpull/fanin/tcp.svg" alt="PUSH fan-in: TCP" width="850">
</p>

## REQ/REP Latency

<p align="center">
  <img src="doc/charts/main_reqrep_tcp.svg" alt="REQ/REP latency: TCP implementations" width="950">
</p>

<p align="center">
  <img src="doc/charts/reqrep/tcp.svg" alt="REQ/REP latency: TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/ipc.svg" alt="REQ/REP latency: IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/inproc.svg" alt="REQ/REP latency: inproc" width="850">
</p>

## PUB/SUB Throughput

<p align="center">
  <img src="doc/charts/main_pubsub_tcp.svg" alt="PUB/SUB throughput: TCP implementations" width="950">
</p>

<p align="center">
  <img src="doc/charts/pubsub/tcp.svg" alt="PUB/SUB throughput: TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pubsub/ipc.svg" alt="PUB/SUB throughput: IPC" width="850">
</p>

The CURVE chart pins the publisher to CPUs 1-2 and the subscriber process to
CPUs 3-4.

<p align="center">
  <img src="doc/charts/pubsub/curve_tcp.svg" alt="CURVE PUB/SUB throughput: TCP" width="850">
</p>

## Shared Memory

OMQ over inproc, IPC, SHM, and SHM producer pools.

### RADIO/DISH Throughput

<p align="center">
  <img src="doc/charts/shm/pubsub.svg" alt="RADIO/DISH throughput: inproc vs IPC vs SHM, 4 and 32 readers" width="950">
</p>

### SCATTER/GATHER Throughput

<p align="center">
  <img src="doc/charts/shm/pushpull.svg" alt="SCATTER/GATHER throughput: inproc vs IPC vs SHM" width="950">
</p>

### CLIENT/SERVER Latency

<p align="center">
  <img src="doc/charts/shm/reqrep.svg" alt="CLIENT/SERVER latency: inproc vs IPC vs SHM, with and without receive spin" width="850">
</p>

## QUIC vs CURVE/TCP

These charts compare QUIC with CURVE over TCP on loopback.

### PUSH/PULL Throughput

<p align="center">
  <img src="doc/charts/quic/pushpull.svg" alt="PUSH/PULL throughput: QUIC vs CURVE/TCP" width="850">
</p>

### PUSH Fan-Out

<p align="center">
  <img src="doc/charts/quic/fanout.svg" alt="PUSH fan-out: QUIC vs CURVE/TCP" width="850">
</p>

### PUSH Fan-In

<p align="center">
  <img src="doc/charts/quic/fanin.svg" alt="PUSH fan-in: QUIC vs CURVE/TCP" width="850">
</p>

### REQ/REP Latency

<p align="center">
  <img src="doc/charts/quic/reqrep.svg" alt="REQ/REP latency: QUIC vs CURVE/TCP" width="850">
</p>

### PUB/SUB Throughput

<p align="center">
  <img src="doc/charts/quic/pubsub.svg" alt="PUB/SUB throughput: QUIC vs CURVE/TCP" width="850">
</p>

## DART

Loopback comparisons of OMQ DART-LAN, DART-adaptive, and TCP, with Aeron
over UDP for SCATTER/GATHER and CLIENT/SERVER.

### SCATTER/GATHER Throughput

<p align="center">
  <img src="doc/charts/dart/scattergather.svg" alt="SCATTER/GATHER throughput: DART vs TCP vs Aeron" width="950">
</p>

### CLIENT/SERVER Latency

<p align="center">
  <img src="doc/charts/dart/clientserver.svg" alt="CLIENT/SERVER latency: DART vs TCP vs Aeron" width="950">
</p>

### RADIO/DISH Throughput

Per-subscriber throughput with sender and receive payload pools on all transports.

<p align="center">
  <img src="doc/charts/dart/radiodish.svg" alt="RADIO/DISH throughput: DART vs TCP, 4 and 32 subscribers" width="950">
</p>

### Lossy Links

DART and QUIC under simulated packet loss on a 100 Mbps link with 1 ms
delay each way.

<p align="center">
  <img src="doc/charts/dart/lossy.svg" alt="DART vs QUIC throughput and latency under packet loss" width="950">
</p>
