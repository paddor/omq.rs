# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq port. Drop-in pyzmq replacement on the common path.

## Install

```sh
pip install pyomq
# Optional extras (built into the wheel via cargo features):
pip install 'pyomq[curve]'
pip install 'pyomq[blake3zmq,lz4,zstd]'
pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

## Usage

```python
import pyomq as zmq  # drop-in for `import zmq` from pyzmq

ctx = zmq.Context()
push = ctx.socket(zmq.PUSH)
push.connect("tcp://127.0.0.1:5555")
push.send(b"hello")
push.close()
ctx.term()
```

For asynchronous code:

```python
import pyomq.asyncio as zmq_async

ctx = zmq_async.Context()
sock = ctx.socket(pyomq.PUSH)
await sock.connect("tcp://127.0.0.1:5555")
await sock.send(b"hello")
await sock.close()
```

## Status

Sync and `asyncio` APIs both ship in this release. All 19 ZMTP socket types are wired:

- **Standard (RFC 28 + 47)**: PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB.
- **Draft**: SERVER, CLIENT (RFC 41), RADIO, DISH (RFC 48), GATHER, SCATTER (RFC 49), PEER, CHANNEL (RFC 51).

Transports: `tcp://`, `ipc://`, `inproc://`, and `udp://` (RADIO/DISH only). Optional features built into the wheel: `curve`, `blake3zmq`, `lz4`, `zstd`.

DISH groups: use `socket.join(b"group")` / `socket.leave(b"group")` to manage subscriptions; messages are sent as multipart `[group, body]`.

## Backend

pyomq is built on `omq-compio` (single-threaded io_uring on Linux). The runtime runs on a dedicated background thread; every Python call releases the GIL across the runtime trip. This is the only backend pyomq supports — the `omq-tokio` backend exists in the upstream Rust workspace for callers that need a multi-thread tokio integration, but pyomq's per-call overhead is shaped around compio's single-thread invariant.

## Performance

Loopback PUSH/PULL throughput vs pyzmq, on a 2019 MacBook Pro VM (Linux 6.12, single core for both peers):

| Size  | inproc pyomq  | inproc pyzmq | ratio | tcp pyomq | tcp pyzmq | ratio |
|-------|--------------:|-------------:|------:|----------:|----------:|------:|
| 128 B | 1.13 M/s      | 178 k/s      | **6.32×** | 604 k/s  | 229 k/s   | **2.64×** |
| 512 B | 1.05 M/s      | 165 k/s      | **6.35×** | 571 k/s  | 223 k/s   | **2.56×** |
| 2 KiB | 974 k/s       | 154 k/s      | **6.32×** | 417 k/s  | 187 k/s   | **2.23×** |
| 8 KiB | 833 k/s       | 129 k/s      | **6.46×** | 178 k/s  | 104 k/s   | **1.70×** |
| 32 KiB| 469 k/s       | 76 k/s       | **6.16×** | 53 k/s   | 41 k/s    | **1.31×** |

Run `pytest tests/test_perf.py -v -s` to reproduce on your hardware.

At small sizes, the per-call PyO3 + flume hop is shorter than pyzmq's libzmq round-trip, so pyomq pulls ahead by a wide margin. At 32 KiB the two implementations both hit memory-bandwidth and converge (small lead from compio's writev + io_uring batching).

## Develop

```sh
cd bindings/pyomq
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```
