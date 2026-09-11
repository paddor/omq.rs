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
- RADIO/DISH groups use `socket.join()` / `socket.leave()`,
  `socket.send(body, group="...")`, and `Frame.group` on receive.

## Install

Requires Python **3.12+**. Wheels use the `cp312-abi3` stable ABI.

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
sock.connect("tcp://127.0.0.1:5555")
await sock.send(b"hello")
sock.close()
```

Zguide-style runnable examples live in [examples/zguide/](examples/zguide/).

### Buffers, tracking, and types

`send()` accepts `pyomq.Sendable`: buffer-protocol objects and `Frame`.
`send_multipart()` also accepts generators of these values. Buffers must
be contiguous; multidimensional and multibyte buffers are sent as raw bytes.
Use `send_string()` for text. `__bytes__` alone does not make an object sendable.

With `copy=False, track=True`, sends return a `MessageTracker`; asyncio
sends resolve to that tracker when awaited. `tracker.done` and
`tracker.wait(timeout)` indicate when borrowed buffers are no longer in use,
not peer delivery. Timeouts are in seconds; `wait()` raises `NotDone` when
the deadline expires. Do not
mutate borrowed data before completion. Keeping a zero-copy `Frame`, an
exported view, or an inproc receiver's frame alive can delay completion.
Multipart tracking covers every part. Copied buffer sends return `None`,
even with `track=True`. Sending a `Frame` returns its own tracker; requesting
tracking on an untracked frame raises `ValueError`.

`wait()` blocks its calling thread. In asyncio code, awaiting `send()` only
waits for queue admission. Use `await asyncio.to_thread(tracker.wait)` when
you need buffer completion without blocking the event loop.

Python-defined buffer exporters may run `__release_buffer__` on an I/O
thread. Release callbacks must not make blocking socket or context calls:
those calls can deadlock the I/O thread. Use `copy=True` for such exporters
or hand their cleanup work to an application thread. This limitation also
applies when tracking is disabled.

Tracking adds bookkeeping only when requested. For repeated sends of unchanged
data, a tracked `Frame` reuses its tracking state across sends. Its tracker
still cannot finish until the frame and every outstanding user release it.

The package includes native stubs and precise sync/async receive overloads.
`copy=False` returns `Frame` values, and a runtime boolean yields the union
of byte and frame result types. Serialization callbacks retain their return
types; socket options, authentication policies, and monitor events are typed.

PLAIN servers require an explicit policy before bind. Use fixed credentials
with `pull.plain_server = 1; pull.set_plain_auth([("alice", "secret")])`, or
pass a callable to `set_plain_auth`. Clients keep using `plain_username` and
`plain_password`. PLAIN does not encrypt traffic.

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
| REQ/REP rt/s       |   8,161/s |   4,348/s | **1.88x** |
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
