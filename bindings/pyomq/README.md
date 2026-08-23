# pyomq

Python binding for [omq.rs](https://github.com/paddor/omq.rs), a Rust libzmq
port. Drop-in pyzmq replacement on the common path.

## Highlights

- Sync and `asyncio` APIs with all 20 ZMTP socket types.
- Standard sockets: PAIR, PUB, SUB, REQ, REP, DEALER, ROUTER, PULL, PUSH,
  XPUB, XSUB, and STREAM.
- Draft sockets: SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, PEER, and
  CHANNEL.
- `tcp://`, `ipc://`, `inproc://`, and `udp://` transports (RADIO/DISH only).
- Optional `plain`, `curve`, `lz4`, and `zstd` features in the published
  wheel.
- Built on [`omq-tokio`](https://github.com/paddor/omq.rs/tree/main/omq-tokio);
  runtime work runs on a dedicated background thread and Python calls release
  the GIL across the runtime trip.
- DISH groups use `socket.join()` / `socket.leave()` and multipart group
  messages.

## Install

```sh
uv pip install pyomq
uv pip install 'pyomq[test]'   # adds pytest, pyzmq for the interop suite
```

The published wheel includes optional features: plain, curve, lz4, zstd.
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

Zguide-style runnable examples live in [examples/zguide/](examples/zguide/).

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
| PUSH/PULL msg/s    |  2.93 M/s |  1.57 M/s | **1.87x** |
| REQ/REP rt/s       |   7,817/s |   4,348/s | **1.80x** |
<!-- PROXY_PERF:END -->

pyomq's `proxy()` forwards directly between sockets on the tokio runtime,
no Python per-message overhead. pyzmq's `zmq.proxy()` calls libzmq's
C-level `zmq_proxy`. PUSH/PULL forwarding is throughput-bound and pyomq is
~1.8x faster. REQ/REP proxy is latency-bound (4 TCP hops per round-trip);
pyomq is ~1.9x faster thanks to direct socket forwarding.

Run `scripts/update_perf.py` (after `maturin develop --release`) to re-measure, regenerate the chart, and update the proxy table.

## Compression transports

OMQ.rs adds transparent compression transports on top of TCP:
`lz4+tcp://` and experimental `zstd+tcp://`.
Swap the scheme in your endpoint string and everything else stays the same:

```python
push = ctx.socket(zmq.PUSH)
push.bind("lz4+tcp://127.0.0.1:5555")

pull = ctx.socket(zmq.PULL)
pull.connect("lz4+tcp://127.0.0.1:5555")
```

Both peers must use a matching compression endpoint. Payloads below the
transport threshold are sent as-is when compression would not help.

Compression transports support static dictionaries and dictionary
auto-training (off by default). Auto-training samples outbound messages,
builds a 2 KiB dict, and ships it once per connection. Static dicts are set
with `compression_dict`. `zstd+tcp://` also accepts `compression_level`.
Pure Rust (`lz4rip` / `zrip`), no C compiler required.

Enable it on sockets that send compressible traffic before `bind()`/`connect()`:

```python
push.compression_auto_train = 1
# or: push.setsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN, 1)
push.compression_level = 1  # zstd+tcp only
```

See [BENCHMARKS_COMPRESSION.md](https://github.com/paddor/omq.rs/blob/main/BENCHMARKS_COMPRESSION.md) for throughput charts and benchmark details.
Wire formats: [LZ4](https://github.com/paddor/omq.rs/blob/main/doc/lz4-rfc.md),
[Zstd](https://github.com/paddor/omq.rs/blob/main/doc/zstd-rfc.md).

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
