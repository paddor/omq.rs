# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq
port. Drop-in pyzmq replacement on the common path.

## Install

```sh
uv pip install pyomq
uv pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

The published wheel includes optional features: plain, curve, lz4.
Use `pyomq.has("curve")` at runtime to check availability.

Published wheels currently target Linux. Other platforms can build from
sdist when the local Rust/Python toolchain supports them. Windows pyomq
support is not complete on `main` yet.

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
import pyomq
import pyomq.asyncio as zmq_async

ctx = zmq_async.Context()
sock = ctx.socket(pyomq.PUSH)
await sock.connect("tcp://127.0.0.1:5555")
await sock.send(b"hello")
await sock.close()
```

## Status

Sync and `asyncio` APIs both ship in this release. All 20 ZMTP socket types are wired:

- **Standard (RFC 28 + 47)**: PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH, XPUB, XSUB, STREAM.
- **Draft**: SERVER, CLIENT (RFC 41), RADIO, DISH (RFC 48), GATHER, SCATTER (RFC 49), PEER, CHANNEL (RFC 51).

Transports: `tcp://`, `ipc://`, `inproc://`, and `udp://` (RADIO/DISH only).
Optional features built into the wheel: `plain`, `curve`, `lz4`.

DISH groups: use `socket.join(b"group")` / `socket.leave(b"group")` to manage
subscriptions; messages are sent as multipart `[group, body]`.

## Backend

pyomq is built on `omq-tokio` (multi-threaded tokio runtime). The runtime
runs on a dedicated background thread; every Python call releases the GIL
across the runtime trip.

## Performance

See [COMPARISONS.md](https://github.com/paddor/omq.rs/blob/main/COMPARISONS.md) for full tables.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/pyomq/doc/charts/bindings.svg" alt="pyomq vs pyzmq performance" width="850">
</p>

2-process loopback throughput and latency vs pyzmq, measured on Linux 6.12
(Debian 13), Intel i7-8700B 3.2 GHz, Rust 1.95.0.

### `zmq.proxy()` forwarding (128 B, TCP)

<!-- PROXY_PERF:START -->
|                    | pyomq     | pyzmq     | ratio     |
|--------------------|----------:|----------:|----------:|
| PUSH/PULL msg/s    |  2.77 M/s |  1.53 M/s | **1.81×** |
| REQ/REP rt/s       |   8,360/s |   4,511/s | **1.85×** |
<!-- PROXY_PERF:END -->

pyomq's `proxy()` forwards directly between sockets on the tokio runtime,
no Python per-message overhead. pyzmq's `zmq.proxy()` calls libzmq's
C-level `zmq_proxy`. PUSH/PULL forwarding is throughput-bound and pyomq is
~1.8x faster. REQ/REP proxy is latency-bound (4 TCP hops per round-trip);
pyomq is ~1.9x faster thanks to direct socket forwarding.

Run `scripts/update_perf.py` (after `maturin develop --release`) to re-measure, regenerate the chart, and update the proxy table.

## Compression transports

OMQ.rs adds a transparent LZ4 compression transport on top of TCP: `lz4+tcp://`.
Swap the scheme in your endpoint string and everything else stays the same:

```python
push = ctx.socket(zmq.PUSH)
push.bind("lz4+tcp://127.0.0.1:5555")

pull = ctx.socket(zmq.PULL)
pull.connect("lz4+tcp://127.0.0.1:5555")
```

Both peers must use a matching compression endpoint. Payloads below ~512 B are
sent as-is (the codec detects that compression would expand them).

`lz4+tcp://` supports dictionary auto-training (off by default). When enabled,
it samples the first 100 outbound messages, builds a 2 KiB dict, and ships it
to the peer once. After that the compression threshold drops from 512 B to
128 B, so small structured messages start compressing too. Pure Rust (lz4rip),
no C compiler required.

Enable it on sockets that send compressible traffic before `bind()`/`connect()`:

```python
push.compression_auto_train = 1
# or: push.setsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN, 1)
```

See [BENCHMARKS_COMPRESSION.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS_COMPRESSION.md) for throughput charts and benchmark details.
Wire format: [LZ4 transport RFC](https://github.com/paddor/omq.rs/blob/main/doc/lz4-rfc.md).

## CURVE authentication

CURVE encrypts traffic and authenticates the server to the client. To also
authenticate clients to the server, call `set_curve_auth()` before
`bind()`/`connect()`:

```python
server_pub, server_sec = zmq.curve_keypair()
client_pub, client_sec = zmq.curve_keypair()

pull = ctx.socket(zmq.PULL)
pull.curve_server = 1
pull.curve_publickey = server_pub
pull.curve_secretkey = server_sec

# Option 1: allow specific client keys (checked in Rust, no GIL overhead)
pull.set_curve_auth([client_pub])

# Option 2: custom callback. PeerInfo has .public_key (Z85) and
# .identity (bytes or None).
pull.set_curve_auth(lambda peer: peer.public_key in allowed_keys)

# Option 3: accept any valid CURVE client (the default)
pull.set_curve_auth(None)
```

No ZAP, no filesystem key management. The callback runs during the CURVE
handshake; returning a falsy value rejects the client.

## Develop

```sh
cd bindings/pyomq
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```
