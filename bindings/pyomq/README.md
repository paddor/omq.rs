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

## Backend

v0.1 ships a single backend (`omq-compio`, single-threaded io_uring on Linux). All Python calls release the GIL across the runtime trip; the runtime itself runs on a dedicated background thread.

A `tokio-backend` extra is planned for v0.2.

## Status

v0.1 covers the sync surface: PUSH/PULL, PUB/SUB, XPUB/XSUB, REQ/REP, DEALER/ROUTER, PAIR over `tcp://`, `ipc://`, `inproc://`. `pyomq.asyncio` is reserved for v0.2 and currently raises `NotImplementedError` on access.

## Develop

```sh
cd bindings/pyomq
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest pyzmq
maturin develop --release
pytest -v
```
