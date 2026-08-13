# OMQ.cr Architecture

Crystal binding for `omq-libzmq`. The public shard lives in
`bindings/crystal/src/omq.cr`.

Benchmark comparison uses separate OMQ and zeromq-crystal peer binaries. This
avoids linking `libomq_zmq` and system `libzmq` into one process, where
duplicate `zmq_*` symbols would invalidate the result.

## Source Layout

```text
src/omq.cr              public Crystal API and direct FFI declarations
spec/                   Crystal API and pyzmq interop specs
scripts/bench_peer.cr   benchmark peer process
scripts/bench_peer_zeromq.cr
                        zeromq-crystal/libzmq benchmark peer process
scripts/update_perf.py  append-only bench runner and SVG chart generator
```

## Threading

Each socket follows the libzmq rule: one socket, one application thread.
`Socket#close` is idempotent. `Context#term` refuses to terminate while live
sockets exist, so sockets cannot keep calling into a terminated context.

`omq-libzmq` owns the OMQ context and IO threads. Crystal fibers do not own
transport, reconnect, ZMTP, compression, or routing state.

## Data Path

Send:

```text
Crystal Socket#send
  -> zmq_send
  -> omq-libzmq send path
  -> omq-tokio socket/routing/send pipe
  -> IO thread encodes and writes transport
```

Receive:

```text
IO thread reads and decodes transport
  -> omq-libzmq receive queue
  -> zmq_msg_recv / zmq_recv
  -> Crystal String allocation
```

Single-part messages up to OMQ's inline cutoff stay inline in OMQ message
storage. The Crystal binding still allocates a Crystal `String` for every
received frame.
