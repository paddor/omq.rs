"""Async (asyncio) facade for pyomq.

Use::

    import pyomq
    import pyomq.asyncio as zmq_async

    ctx = zmq_async.Context()
    sock = ctx.socket(pyomq.PUSH)
    sock.connect("tcp://127.0.0.1:5555")
    sock.send(b"hello")
    msg = await sock.recv()
    sock.close()
"""

import asyncio
import errno as _errno
import json
import os
import pickle
import select as _select
import sys
import threading
import weakref
from collections import deque

from . import _native  # type: ignore[attr-defined]
from . import error
from . import Context as _SyncContext
from . import _next_ctx_id
from . import (
    FD,
    POLLIN,
    SNDHWM,
    RCVHWM,
    LINGER,
    LAST_ENDPOINT,
    _TYPE_NAMES,
    _SOCKOPT_NAMES,
)

_IS_WINDOWS = sys.platform == "win32"
_WAKEUP_MODE_NONE = 0
_WAKEUP_MODE_ASYNC = 1
_WAKEUP_MODE_SYNC = 2


def _resolved_future(result):
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    fut.set_result(result)
    return fut


_EAGAIN = _errno.EAGAIN
_MISSING = object()


class _DoneFuture:
    """Lightweight awaitable that resolves immediately to None."""

    def __await__(self):
        return
        yield  # makes this a generator

    def result(self):
        return None

    def done(self):
        return True


_SEND_DONE = _DoneFuture()


class _RecvFuture:
    """Supports both ``await fut`` (event-loop) and ``fut.result()`` (blocking)."""

    __slots__ = ("_try_fn", "_fd", "_result", "_exception")

    def __init__(self, try_fn, fd):
        self._try_fn = try_fn
        self._fd = fd
        self._result = _MISSING
        self._exception = None

    def done(self):
        if self._result is not _MISSING or self._exception is not None:
            return True
        try:
            r = self._try_fn()
        except Exception as e:
            self._exception = e
            return True
        if r is not None:
            self._result = r
            return True
        return False

    def result(self):
        if self._exception is not None:
            raise self._exception
        if self._result is not _MISSING:
            return self._result
        try:
            while True:
                _select.select([self._fd], [], [])
                try:
                    os.read(self._fd, 8)
                except OSError:
                    pass
                try:
                    r = self._try_fn()
                except Exception as e:
                    self._exception = e
                    raise
                if r is not None:
                    self._result = r
                    return r
        finally:
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1

    def __await__(self):
        if self.done():
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1
            if self._exception is not None:
                raise self._exception
            return self._result

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fd = self._fd
        self._fd = -1
        try_fn = self._try_fn

        def _on_readable():
            try:
                os.read(fd, 8)
            except OSError:
                pass
            try:
                r = try_fn()
            except Exception as e:
                loop.remove_reader(fd)
                os.close(fd)
                if not fut.done():
                    fut.set_exception(e)
                return
            if r is not None:
                try:
                    os.write(fd, b"\x01\x00\x00\x00\x00\x00\x00\x00")
                except OSError:
                    pass
                loop.remove_reader(fd)
                os.close(fd)
                if not fut.done():
                    fut.set_result(r)

        def _on_cancel(f):
            if f.cancelled():
                loop.remove_reader(fd)
                os.close(fd)

        fut.add_done_callback(_on_cancel)
        loop.add_reader(fd, _on_readable)
        return (yield from fut.__await__())


class Socket:
    _sock: _native.AsyncSocket
    _context: "Context"
    _closed: bool

    def _init_socket_state(self, _sock, _context):
        self._sock = _sock
        self._context = _context
        self._closed = False
        self._last_endpoint = None
        if _IS_WINDOWS:
            self._loop = None
            self._recv_waiters = deque()
            self._send_waiters = deque()
            self._recv_wakeup_event = threading.Event()
            self._send_wakeup_event = threading.Event()
            self._wakeup_registered = False

    def __init__(self, _sock, _context):
        self._init_socket_state(_sock, _context)

    def __getattr__(self, name):
        opt = _SOCKOPT_NAMES.get(name)
        if opt is not None:
            if opt == LAST_ENDPOINT:
                return self._last_endpoint
            return self.getsockopt(opt)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        opt = _SOCKOPT_NAMES.get(name)
        if opt is not None:
            self.setsockopt(opt, value)
            return
        object.__setattr__(self, name, value)

    def __repr__(self):
        st = _TYPE_NAMES.get(self.socket_type, str(self.socket_type))
        return f"<pyomq.asyncio.Socket(pyomq.{st}) at {id(self):#x}>"

    @property
    def closed(self):
        return self._closed

    @property
    def context(self):
        return self._context

    @property
    def last_endpoint(self):
        return self._last_endpoint

    @property
    def socket_type(self):
        return self._sock.getsockopt(_native.TYPE)

    @property
    def underlying(self):
        return self

    # ── I/O ──────────────────────────────────────────────────────────

    def fileno(self):
        return self.getsockopt(FD)

    def bind(self, endpoint):
        try:
            ep = self._sock.bind(self._context._namespace_inproc(endpoint))
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            return ep
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def connect(self, endpoint):
        try:
            self._sock.connect(self._context._namespace_inproc(endpoint))
            self._last_endpoint = (
                endpoint.encode() if isinstance(endpoint, str) else endpoint
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unbind(self, endpoint):
        try:
            return self._sock.unbind(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def disconnect(self, endpoint):
        try:
            return self._sock.disconnect(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send(self, data, flags=0, copy=True, track=False):
        try:
            self._sock.send(data, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_with_backpressure(data, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv(self, flags=0, copy=True, track=False):
        if not copy:
            from pyomq import Frame

            async def _wrap():
                data = await self._add_recv_event(self._sock._try_recv)
                return Frame(data)

            return asyncio.ensure_future(_wrap())
        return self._add_recv_event(self._sock._try_recv)

    def send_multipart(self, parts, flags=0, copy=True, track=False):
        try:
            self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_multipart_with_backpressure(parts, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv_multipart(self, flags=0, copy=True, track=False):
        if not copy:
            from pyomq import Frame

            async def _wrap():
                parts = await self._add_recv_event(self._sock._try_recv_multipart)
                return [Frame(p) for p in parts]

            return asyncio.ensure_future(_wrap())
        return self._add_recv_event(self._sock._try_recv_multipart)

    if _IS_WINDOWS:

        def _register_wakeup_hooks(self):
            if not self._wakeup_registered:
                self._sock._set_wakeup_hooks(
                    recv_async=self._schedule_recv_drain,
                    recv_event=self._recv_wakeup_event,
                    send_async=self._schedule_send_drain,
                    send_event=self._send_wakeup_event,
                )
                self._wakeup_registered = True

        def _set_wakeup_modes(self, *, recv_mode=None, send_mode=None):
            self._sock._set_wakeup_modes(recv_mode=recv_mode, send_mode=send_mode)

        def _clear_wakeup_modes(self, *, recv_mode=None, send_mode=None):
            self._sock._clear_wakeup_modes(recv_mode=recv_mode, send_mode=send_mode)

        def _schedule_recv_drain(self):
            loop = self._loop
            if loop is None or loop.is_closed():
                self._clear_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_recv_drain_complete()
                return
            loop.call_soon_threadsafe(self._drain_recv_waiters)

        def _schedule_send_drain(self):
            loop = self._loop
            if loop is None or loop.is_closed():
                self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_send_drain_complete()
                return
            loop.call_soon_threadsafe(self._drain_send_waiters)

        def _drain_recv_waiters(self):
            """Invoke each waiter until one returns False (not ready)."""
            waiters = self._recv_waiters
            try:
                while waiters and waiters[0]():
                    waiters.popleft()
            finally:
                if not waiters:
                    self._clear_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_recv_drain_complete()
                # Ensure we drain any notification that arrived in between
                # the end of the try block and the call to _mark_recv_drain_complete.
                while waiters and waiters[0]():
                    waiters.popleft()
                if waiters:
                    self._set_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC)
                else:
                    self._clear_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC)

        def _drain_send_waiters(self):
            """Invoke each waiter until one returns False (not ready)."""
            waiters = self._send_waiters
            try:
                while waiters and waiters[0]():
                    waiters.popleft()
            finally:
                if not waiters:
                    self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_send_drain_complete()
                # Ensure we drain any notification that arrived in between
                # the end of the try block and the call to _mark_send_drain_complete.
                while waiters and waiters[0]():
                    waiters.popleft()
                if waiters:
                    self._set_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)
                else:
                    self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)

        def _add_waitable(self, try_fn, waiters, set_mode, clear_mode):
            """Register a Windows waiter that resolves when try_fn returns
            non-None. try_fn must return None when not ready and raise on
            real errors."""
            loop = asyncio.get_running_loop()
            self._loop = loop

            result = try_fn()
            if result is not None:
                return _resolved_future(result)

            self._register_wakeup_hooks()
            fut = loop.create_future()

            def _waiter() -> bool:
                if fut.done():
                    return True
                try:
                    result = try_fn()
                except Exception as e:
                    if not fut.done():
                        fut.set_exception(e)
                    return True
                if result is not None and not fut.done():
                    fut.set_result(result)
                    return True
                return False

            waiters.append(_waiter)
            set_mode()
            # Try once more immediately; if ready, remove from queue before returning.
            # This must happen before we return the future to the caller.
            # Catch ValueError in case drain callback already processed this waiter (race).
            try:
                if _waiter():
                    # Successfully resolved, remove from queue so drain doesn't process it.
                    waiters.remove(_waiter)
            except ValueError:
                # Queue might have been modified by drain callback (race condition).
                # This is OK - drain already processed the waiter and marked it done.
                pass
            if not waiters:
                clear_mode()

            return fut

        def _add_recv_event(self, try_fn):
            def safe_try():
                try:
                    return try_fn()
                except _native.ZMQError as e:
                    raise error.from_native(e) from None

            return self._add_waitable(
                safe_try,
                self._recv_waiters,
                lambda: self._set_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC),
                lambda: self._clear_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC),
            )

        def _send_with_backpressure(self, data, flags):
            def try_send():
                try:
                    self._sock.send(data, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _errno.EAGAIN:
                        return None
                    raise error.from_native(e) from None

            return self._add_waitable(
                try_send,
                self._send_waiters,
                lambda: self._set_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC),
                lambda: self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC),
            )

        def _send_multipart_with_backpressure(self, parts, flags):
            def try_send():
                try:
                    self._sock.send_multipart(parts, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _errno.EAGAIN:
                        return None
                    raise error.from_native(e) from None

            return self._add_waitable(
                try_send,
                self._send_waiters,
                lambda: self._set_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC),
                lambda: self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC),
            )
    else:

        def _add_recv_event(self, try_fn):
            # Fast path: message already available, no event loop needed.
            try:
                result = try_fn()
            except _native.ZMQError as e:
                raise error.from_native(e) from None
            if result is not None:
                return _resolved_future(result)

            fd = self._sock._recv_fd()

            try:
                result = try_fn()
            except _native.ZMQError as e:
                os.close(fd)
                raise error.from_native(e) from None
            if result is not None:
                os.close(fd)
                return _resolved_future(result)

            return _RecvFuture(try_fn, fd)

        def _send_with_backpressure(self, data, flags):
            fd = self._sock._send_fd()

            def try_send():
                try:
                    self._sock.send(data, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _errno.EAGAIN:
                        return None
                    raise

            return _RecvFuture(try_send, fd)

        def _send_multipart_with_backpressure(self, parts, flags):
            fd = self._sock._send_fd()

            def try_send():
                try:
                    self._sock.send_multipart(parts, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _errno.EAGAIN:
                        return None
                    raise

            return _RecvFuture(try_send, fd)

    # ── Serialization helpers ────────────────────────────────────────

    def send_string(self, u, flags=0, encoding="utf-8"):
        return self.send(u.encode(encoding), flags)

    async def recv_string(self, flags=0, encoding="utf-8"):
        return (await self.recv(flags)).decode(encoding)

    def send_json(self, obj, flags=0, **kwargs):
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    async def recv_json(self, flags=0, **kwargs):
        return json.loads(await self.recv(flags), **kwargs)

    def send_pyobj(self, obj, flags=0, protocol=-1):
        return self.send(pickle.dumps(obj, protocol), flags)

    async def recv_pyobj(self, flags=0):
        return pickle.loads(await self.recv(flags))

    def send_serialized(self, msg, serialize, flags=0, copy=True, **kwargs):
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    async def recv_serialized(self, deserialize, flags=0, copy=True):
        frames = await self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    # ── Options (sync -- matches pyzmq) ──────────────────────────────

    def setsockopt(self, option, value):
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def getsockopt(self, option):
        from pyomq import LAST_ENDPOINT

        if option == LAST_ENDPOINT:
            return self._last_endpoint
        try:
            return self._sock.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option, value):
        return self.setsockopt(option, value)

    def get(self, option):
        return self.getsockopt(option)

    def setsockopt_string(self, option, value, encoding="utf-8"):
        return self.setsockopt(option, value.encode(encoding))

    def getsockopt_string(self, option, encoding="utf-8"):
        v = self.getsockopt(option)
        if isinstance(v, bytes):
            return v.decode(encoding)
        return str(v)

    set_string = setsockopt_string
    get_string = getsockopt_string

    def set_curve_auth(self, auth):
        try:
            return self._sock.set_curve_auth(auth)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        except AttributeError:
            from . import ZMQNotImplementedError

            raise ZMQNotImplementedError("curve feature not compiled")

    def set_hwm(self, value):
        self.setsockopt(SNDHWM, value)
        self.setsockopt(RCVHWM, value)

    def get_hwm(self):
        return self.getsockopt(SNDHWM)

    hwm = property(get_hwm, set_hwm)

    # ── Subscriptions ────────────────────────────────────────────────

    def subscribe(self, prefix):
        try:
            return self._sock.subscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unsubscribe(self, prefix):
        try:
            return self._sock.unsubscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def join(self, group):
        try:
            return self._sock.join(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def leave(self, group):
        try:
            return self._sock.leave(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self, linger=None):
        if not self._closed:
            self._closed = True
            self._sock.close(linger)

    def __del__(self):
        self.close()

    async def poll(self, timeout=None, flags=POLLIN):
        p = Poller()
        p.register(self, flags)
        evts = await p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close()
        return False


class Poller:
    def __init__(self):
        self._sockets = {}

    def register(self, socket, flags=POLLIN):
        self._sockets[socket._sock.socket_id()] = (socket, flags)

    def unregister(self, socket):
        self._sockets.pop(socket._sock.socket_id(), None)

    def modify(self, socket, flags):
        k = socket._sock.socket_id()
        if k in self._sockets:
            self._sockets[k] = (socket, flags)

    @property
    def sockets(self):
        return [(s, f) for s, f in self._sockets.values()]

    async def poll(self, timeout=None):
        if not self._sockets:
            return []
        pollin_socks = [s._sock for k, (s, f) in self._sockets.items() if f & POLLIN]
        if not pollin_socks:
            return []
        t = None if (timeout is None or timeout < 0) else int(timeout)
        loop = asyncio.get_running_loop()
        ready_ids = await loop.run_in_executor(None, _native.wait_any, pollin_socks, t)
        return [
            (self._sockets[rid][0], POLLIN) for rid in ready_ids if rid in self._sockets
        ]


class Context(_SyncContext):
    _socket_class = None

    def __init__(self, io_threads=1, *, _shadow_ctx=None):
        if _shadow_ctx is not None:
            self._ctx = _shadow_ctx._ctx
            self._is_shadow = True
        else:
            self._ctx = _native.AsyncContext(io_threads)
            self._is_shadow = False
        self._closed = False
        self._sockets = weakref.WeakSet()
        self._ctx_id = next(_next_ctx_id)

    @property
    def closed(self):
        return self._closed

    def socket(self, socket_type, socket_class=None, **kwargs):
        native = self._ctx.socket(socket_type)
        cls = socket_class or Socket
        s = object.__new__(cls)
        s._sock = native
        s._context = self
        s._closed = False
        s._last_endpoint = None
        s._pid = os.getpid()
        s._binds = []
        s._connects = []
        s._init_socket_state(native, self)
        self._sockets.add(s)
        return s

    def term(self):
        self._closed = True
        for s in list(self._sockets):
            if not s.closed:
                s.close()
        self._sockets.clear()

    def destroy(self, linger=None):
        for s in list(self._sockets):
            if not s.closed:
                if linger is not None:
                    s.setsockopt(LINGER, linger)
                s.close()
        self._sockets.clear()
        self.term()

    def __del__(self):
        if not self._closed:
            self.term()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.term()
        return False


Context._socket_class = Socket

__all__ = ["Context", "Socket", "Poller"]
