"""Async (asyncio) facade for pyomq.

Use::

    import pyomq
    import pyomq.asyncio as zmq_async

    ctx = zmq_async.Context()
    sock = ctx.socket(pyomq.PUSH)
    await sock.connect("tcp://127.0.0.1:5555")
    await sock.send(b"hello")
    msg = await sock.recv()
    await sock.close()

The async classes share the underlying socket state with the sync
classes - `setsockopt` / `getsockopt` set on a sync socket carry over
if you reconstruct via the same Context-equivalent factory call.
"""

from ._native import (  # type: ignore[attr-defined]
    AsyncContext as Context,
    AsyncSocket as Socket,
)

__all__ = ["Context", "Socket"]
