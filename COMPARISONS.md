# Comparisons

PUSH/PULL throughput and REQ/REP latency across Rust ZMQ implementations. Two-process benchmarks (inproc: single-process), 3 s timed window after 500 ms warmup, median of recent runs.

Implementations tested:
- **libzmq** v4.3.5 (C, `scripts/libzmq_bench_peer.c`)
- **omq-tokio** (this project, single-threaded tokio runtime)
- **zmq.rs** (zeromq crate v0.6.0, `scripts/zmqrs_bench_peer/`)
- **rzmq** v0.5.18 (`scripts/rzmq_bench_peer/`)

For omq backend comparisons (compio vs tokio vs tokio-mt), see the collapsed sections in [README.md](README.md).

## PUSH/PULL throughput

### TCP

<p align="center">
  <img src="doc/charts/pushpull/alt_tcp.svg" alt="PUSH/PULL throughput: TCP" width="850">
</p>

### IPC

<p align="center">
  <img src="doc/charts/pushpull/alt_ipc.svg" alt="PUSH/PULL throughput: IPC" width="850">
</p>

### inproc

<p align="center">
  <img src="doc/charts/pushpull/alt_inproc.svg" alt="PUSH/PULL throughput: inproc" width="850">
</p>

zmq.rs (zeromq v0.6.0) does not implement inproc.

## REQ/REP latency

### TCP

<p align="center">
  <img src="doc/charts/reqrep/alt_tcp.svg" alt="REQ/REP latency: TCP" width="850">
</p>

### IPC

<p align="center">
  <img src="doc/charts/reqrep/alt_ipc.svg" alt="REQ/REP latency: IPC" width="850">
</p>

### inproc

<p align="center">
  <img src="doc/charts/reqrep/alt_inproc.svg" alt="REQ/REP latency: inproc" width="850">
</p>

## PUB/SUB throughput

### TCP

<p align="center">
  <img src="doc/charts/pubsub/alt_tcp.svg" alt="PUB/SUB throughput: TCP" width="850">
</p>

## Fan-out and fan-in

1-to-N and N-to-1 PUSH/PULL over TCP. These charts show only libzmq and omq backends (compio, tokio, tokio-mt).

<p align="center">
  <img src="doc/charts/pushpull/fanout_tcp.svg" alt="PUSH fan-out: TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/fanin_tcp.svg" alt="PUSH fan-in: TCP" width="850">
</p>
