# OMQ.cr Development

## Prerequisites

- Crystal 1.21 or newer.
- Rust toolchain with Cargo.
- Python 3 for benchmark chart generation.

## Test

```sh
./scripts/test-crystal.sh
```

This builds `omq-libzmq`, checks Crystal formatting, and runs the Crystal
specs with the correct native library path.

The spec suite covers basic API behavior, pyzmq interop, socket-type parity,
socket-option parity, draft sockets, poll/poller, monitor stubs, CURVE, Z85,
and stream raw TCP behavior.

Manual equivalent:

```sh
cargo build -p omq-libzmq
export LIBRARY_PATH="$PWD/target/debug:$LIBRARY_PATH"
export LD_LIBRARY_PATH="$PWD/target/debug:$LD_LIBRARY_PATH"
crystal spec bindings/crystal/spec --link-flags "-L$PWD/target/debug -Wl,-rpath,$PWD/target/debug"
```

## Benchmarks

Quick local chart:

```sh
bindings/crystal/scripts/update_perf.py --quick
```

Full local chart:

```sh
bindings/crystal/scripts/update_perf.py
```

The benchmark runner builds separate OMQ and zeromq-crystal peer binaries, so
`libomq_zmq` and system `libzmq` symbols cannot interpose on each other.
`zeromq-crystal` is used through its `LibZMQ` FFI layer because the high-level
socket wrapper does not compile on Crystal 1.21.

The comparison line uses `zeromq-crystal`'s `LibZMQ` FFI layer. Its high-level
`ZMQ::Socket` wrapper still references Crystal APIs removed before Crystal 1.21.

Rows append to `~/.cache/omq.cr/bindings.jsonl`. The chart is written to
`bindings/crystal/doc/charts/bindings.svg`.

Default measurement settings:

- Throughput: 3 measured rounds, 2.5 s per round, 0.5 s per-round warmup.
- Latency: 3 measured rounds, 1.5 s per round, 0.5 s per-round warmup.
- Cell result: median measured round by `msg/s` for throughput and median
  measured round by p50 latency for latency.
