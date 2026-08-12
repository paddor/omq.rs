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

from __future__ import annotations

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
from typing import Any, Awaitable, Callable, Final
from . import _native  # type: ignore[attr-defined]
from . import error
from . import Context as _SyncContext
from . import _next_ctx_id
from . import (
    LINGER,
    _TYPE_NAMES,
    POLLIN,
    POLLOUT,
    _BaseSocket,
)

_IS_WINDOWS = sys.platform == "win32"
_WAKEUP_MODE_NONE = 0
_WAKEUP_MODE_ASYNC = 1
_WAKEUP_MODE_SYNC = 2


def _resolved_future(result: Any) -> asyncio.Future[Any]:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()
    fut.set_result(result)
    return fut


_EAGAIN: Final[int] = _errno.EAGAIN
_MISSING: Final[object] = object()


class _DoneFuture:
    """Lightweight awaitable that resolves immediately to None."""

    def __await__(self) -> Any:
        return
        yield  # makes this a generator

    def result(self) -> None:
        return None

    def done(self) -> bool:
        return True


_SEND_DONE: Final[_DoneFuture] = _DoneFuture()


class _RecvFuture:
    """Supports both ``await fut`` (event-loop) and ``fut.result()`` (blocking)."""

    __slots__ = ("_try_fn", "_fd", "_result", "_exception")

    _try_fn: Callable[[], Any]
    _fd: int
    _result: Any
    _exception: Exception | None

    def __init__(self, try_fn: Callable[[], Any], fd: int) -> None:
        self._try_fn = try_fn
        self._fd = fd
        self._result = _MISSING
        self._exception = None

    def done(self) -> bool:
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

    def result(self) -> Any:
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

    def __await__(self) -> Any:
        if self.done():
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1
            if self._exception is not None:
                raise self._exception
            return self._result

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        fd = self._fd
        self._fd = -1
        try_fn = self._try_fn

        def _on_readable() -> None:
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

        def _on_cancel(f: asyncio.Future[Any]) -> None:
            if f.cancelled():
                loop.remove_reader(fd)
                os.close(fd)

        fut.add_done_callback(_on_cancel)
        loop.add_reader(fd, _on_readable)
        return (yield from fut.__await__())


class Socket(_BaseSocket):
    """Async ZMQ socket wrapper."""

    _sock: _native.AsyncSocket
    _context: Context
    _closed: bool
    _loop: asyncio.AbstractEventLoop | None
    _recv_waiters: deque[Callable[[], bool]]
    _send_waiters: deque[Callable[[], bool]]
    _recv_wakeup_event: threading.Event
    _send_wakeup_event: threading.Event
    _wakeup_registered: bool

    def _init_socket_state(self, _sock: _native.AsyncSocket, _context: Context) -> None:
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

    def __init__(self, _sock: _native.AsyncSocket, _context: Context) -> None:
        self._init_socket_state(_sock, _context)

    def __repr__(self) -> str:
        st = _TYPE_NAMES.get(self.socket_type, str(self.socket_type))
        return f"<pyomq.asyncio.Socket(pyomq.{st}) at {id(self):#x}>"

    def send(
        self,
        data: bytes | str,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Awaitable[Any | None]:
        try:
            self._sock.send(data, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_with_backpressure(data, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> Awaitable[bytes | Any]:
        if not copy:
            return self._add_recv_event(self._sock._try_recv_frame)
        return self._add_recv_event(self._sock._try_recv)

    def send_multipart(
        self,
        parts: list[bytes | str],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Awaitable[Any | None]:
        try:
            self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_multipart_with_backpressure(parts, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> Awaitable[list[bytes] | list[Any]]:
        if not copy:
            return self._add_recv_event(self._sock._try_recv_multipart_frames)
        return self._add_recv_event(self._sock._try_recv_multipart)

    if _IS_WINDOWS:

        def _register_wakeup_hooks(self) -> None:
            if not self._wakeup_registered:
                self._sock._set_wakeup_hooks(
                    recv_async=self._schedule_recv_drain,
                    recv_event=self._recv_wakeup_event,
                    send_async=self._schedule_send_drain,
                    send_event=self._send_wakeup_event,
                )
                self._wakeup_registered = True

        def _set_wakeup_modes(
            self,
            *,
            recv_mode: int | None = None,
            send_mode: int | None = None,
        ) -> None:
            self._sock._set_wakeup_modes(recv_mode=recv_mode, send_mode=send_mode)

        def _clear_wakeup_modes(
            self,
            *,
            recv_mode: int | None = None,
            send_mode: int | None = None,
        ) -> None:
            self._sock._clear_wakeup_modes(recv_mode=recv_mode, send_mode=send_mode)

        def _schedule_recv_drain(self) -> None:
            loop = self._loop
            if loop is None or loop.is_closed():
                self._clear_wakeup_modes(recv_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_recv_drain_complete()
                return
            loop.call_soon_threadsafe(self._drain_recv_waiters)

        def _schedule_send_drain(self) -> None:
            loop = self._loop
            if loop is None or loop.is_closed():
                self._clear_wakeup_modes(send_mode=_WAKEUP_MODE_ASYNC)
                self._sock._mark_send_drain_complete()
                return
            loop.call_soon_threadsafe(self._drain_send_waiters)

        def _drain_recv_waiters(self) -> None:
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

        def _drain_send_waiters(self) -> None:
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

        def _add_waitable(
            self,
            try_fn: Callable[[], Any],
            waiters: deque[Callable[[], bool]],
            set_mode: Callable[[], None],
            clear_mode: Callable[[], None],
        ) -> asyncio.Future[Any]:
            """Register a Windows waiter that resolves when try_fn returns
            non-None. try_fn must return None when not ready and raise on
            real errors."""
            loop = asyncio.get_running_loop()
            self._loop = loop

            result = try_fn()
            if result is not None:
                return _resolved_future(result)

            self._register_wakeup_hooks()
            fut: asyncio.Future[Any] = loop.create_future()

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

        def _add_recv_event(self, try_fn: Callable[[], Any]) -> asyncio.Future[Any]:
            def safe_try() -> Any:
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

        def _send_with_backpressure(
            self, data: bytes | str, flags: int
        ) -> asyncio.Future[Any]:
            def try_send() -> bool | None:
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

        def _send_multipart_with_backpressure(
            self, parts: list[bytes | str], flags: int
        ) -> asyncio.Future[Any]:
            def try_send() -> bool | None:
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

        def _add_recv_event(
            self, try_fn: Callable[[], Any]
        ) -> asyncio.Future[Any] | _RecvFuture:
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

        def _send_with_backpressure(self, data: bytes | str, flags: int) -> _RecvFuture:
            fd = self._sock._send_fd()

            def try_send() -> bool | None:
                try:
                    self._sock.send(data, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _EAGAIN:
                        return None
                    raise

            return _RecvFuture(try_send, fd)

        def _send_multipart_with_backpressure(
            self, parts: list[bytes | str], flags: int
        ) -> _RecvFuture:
            fd = self._sock._send_fd()

            def try_send() -> bool | None:
                try:
                    self._sock.send_multipart(parts, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _EAGAIN:
                        return None
                    raise

            return _RecvFuture(try_send, fd)

    # ── Serialization helpers ────────────────────────────────────────

    def send_string(
        self, u: str, flags: int = 0, encoding: str = "utf-8"
    ) -> Awaitable[Any | None]:
        return self.send(u.encode(encoding), flags)

    async def recv_string(self, flags: int = 0, encoding: str = "utf-8") -> str:
        return (await self.recv(flags)).decode(encoding)

    def send_json(
        self, obj: Any, flags: int = 0, **kwargs: Any
    ) -> Awaitable[Any | None]:
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    async def recv_json(self, flags: int = 0, **kwargs: Any) -> Any:
        return json.loads(await self.recv(flags), **kwargs)

    def send_pyobj(
        self, obj: Any, flags: int = 0, protocol: int = -1
    ) -> Awaitable[Any | None]:
        return self.send(pickle.dumps(obj, protocol), flags)

    async def recv_pyobj(self, flags: int = 0) -> Any:
        return pickle.loads(await self.recv(flags))

    def send_serialized(
        self,
        msg: Any,
        serialize: Callable[[Any], list[bytes | str]],
        flags: int = 0,
        copy: bool = True,
        **kwargs: Any,
    ) -> Awaitable[Any | None]:
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    async def recv_serialized(
        self,
        deserialize: Callable[[list[bytes]], Any],
        flags: int = 0,
        copy: bool = True,
    ) -> Any:
        frames = await self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    # ── Subscriptions ────────────────────────────────────────────────

    # Note: subscribe(), unsubscribe(), join(), leave() inherited from base

    # ── Monitoring ───────────────────────────────────────────────────

    # Note: monitor(), connections(), connection_info() inherited from base

    # ── Lifecycle ────────────────────────────────────────────────────

    async def poll(self, timeout: int | None = None, flags: int = POLLIN) -> int:
        p = Poller()
        p.register(self, flags)
        evts = await p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0

    async def __aenter__(self) -> Socket:
        return self

    async def __aexit__(self, *args: Any) -> bool:
        self.close()
        return False


class Poller:
    """Async poller for ZMQ sockets."""

    _sockets: dict[int, tuple[Socket, int]]

    def __init__(self) -> None:
        self._sockets = {}

    def register(self, socket: Socket, flags: int = POLLIN) -> None:
        self._sockets[socket._sock.socket_id()] = (socket, flags)

    def unregister(self, socket: Socket) -> None:
        self._sockets.pop(socket._sock.socket_id(), None)

    def modify(self, socket: Socket, flags: int) -> None:
        k = socket._sock.socket_id()
        if k in self._sockets:
            self._sockets[k] = (socket, flags)

    @property
    def sockets(self) -> list[tuple[Socket, int]]:
        return [(s, f) for s, f in self._sockets.values()]

    async def poll(self, timeout: int | None = None) -> list[tuple[Socket, int]]:
        if not self._sockets:
            return []
        ready: dict[int, int] = {
            k: POLLOUT for k, (_, f) in self._sockets.items() if f & POLLOUT
        }
        pollin_socks = [s._sock for k, (s, f) in self._sockets.items() if f & POLLIN]
        if not pollin_socks:
            return [(s, ready[k]) for k, (s, _) in self._sockets.items() if k in ready]
        t = None if (timeout is None or timeout < 0) else int(timeout)
        if ready:
            t = 0
        loop = asyncio.get_running_loop()
        ready_ids = await loop.run_in_executor(None, _native.wait_any, pollin_socks, t)
        for rid in ready_ids:
            ready[rid] = ready.get(rid, 0) | POLLIN
        return [
            (s, ready[k]) for k, (s, _) in self._sockets.items() if k in ready
        ]


class Context(_SyncContext):
    """Async context for creating ZMQ sockets."""

    _socket_class: type | None = None
    _ctx: _native.AsyncContext
    _is_shadow: bool
    _closed: bool
    _sockets: weakref.WeakSet[Socket]
    _ctx_id: int

    def __init__(
        self, io_threads: int = 1, *, _shadow_ctx: _SyncContext | None = None
    ) -> None:
        if _shadow_ctx is not None:
            if isinstance(_shadow_ctx._ctx, _native.Context):
                self._ctx = _native.AsyncContext.shadow_sync(_shadow_ctx._ctx)
            else:
                self._ctx = _shadow_ctx._ctx
            self._is_shadow = True
        else:
            self._ctx = _native.AsyncContext(io_threads)
            self._is_shadow = False
        self._closed = False
        self._sockets = weakref.WeakSet()
        self._ctx_id = (
            _shadow_ctx._ctx_id if _shadow_ctx is not None else next(_next_ctx_id)
        )

    @property
    def closed(self) -> bool:
        return self._closed

    def socket(
        self,
        socket_type: int,
        socket_class: type[Socket] | None = None,
        **kwargs: Any,
    ) -> Socket:  # ty: ignore
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

    @classmethod
    def from_share_key(cls, key: int) -> Context:
        obj = object.__new__(cls)
        obj._ctx = _native.AsyncContext.from_share_key(key)
        obj._is_shadow = True
        obj._closed = False
        obj._sockets = weakref.WeakSet()
        obj._ctx_id = next(_next_ctx_id)
        return obj

    def term(self) -> None:
        self._closed = True
        for s in list(self._sockets):
            if not s.closed:
                s.close()
        self._sockets.clear()
        if not self._is_shadow:
            self._ctx.term()

    def destroy(self, linger: int | None = None) -> None:
        for s in list(self._sockets):
            if not s.closed:
                if linger is not None:
                    s.setsockopt(LINGER, linger)
                s.close()
        self._sockets.clear()
        self.term()

    def __del__(self) -> None:
        if not self._closed:
            self.term()

    def __enter__(self) -> Context:
        return self

    def __exit__(self, *args: Any) -> bool:
        self.term()
        return False


Context._socket_class = Socket

__all__ = ["Context", "Socket", "Poller"]
