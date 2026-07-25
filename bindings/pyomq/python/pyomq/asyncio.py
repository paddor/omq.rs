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
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

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
    _TYPE_NAMES,
    _SocketOptionsBase,
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


_EAGAIN = _errno.EAGAIN
_MISSING = object()


class _DoneFuture:
    """Lightweight awaitable that resolves immediately to None."""

    def __await__(self) -> Any:
        return
        yield  # makes this a generator

    def result(self) -> None:
        return None

    def done(self) -> bool:
        return True


_SEND_DONE = _DoneFuture()


class _RecvFuture:
    """Supports both ``await fut`` (event-loop) and ``fut.result()`` (blocking)."""

    __slots__ = ("_try_fn", "_fd", "_result", "_exception")

    _try_fn: Callable[[], Any]
    _fd: int
    _result: Any
    _exception: Optional[Exception]

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


class Socket(_SocketOptionsBase):
    """Async ZMQ socket wrapper."""

    _sock: _native.AsyncSocket
    _context: Context
    _closed: bool
    _loop: Optional[asyncio.AbstractEventLoop]
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

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def context(self) -> Context:
        return self._context

    @property
    def last_endpoint(self) -> Optional[Union[bytes, str]]:
        return self._last_endpoint

    @property
    def socket_type(self) -> int:
        return self._sock.getsockopt(_native.TYPE)

    @property
    def underlying(self) -> Socket:
        return self

    # ── I/O ──────────────────────────────────────────────────────────

    def fileno(self) -> int:
        return self.getsockopt(FD)

    def bind(self, endpoint: Union[str, bytes]) -> Union[str, bytes]:
        try:
            ep = self._sock.bind(self._context._namespace_inproc(endpoint))
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            return ep
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def connect(self, endpoint: Union[str, bytes]) -> None:
        try:
            self._sock.connect(self._context._namespace_inproc(endpoint))
            self._last_endpoint = (
                endpoint.encode() if isinstance(endpoint, str) else endpoint
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unbind(self, endpoint: Union[str, bytes]) -> None:
        try:
            return self._sock.unbind(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def disconnect(self, endpoint: Union[str, bytes]) -> None:
        try:
            return self._sock.disconnect(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send(
        self,
        data: Union[bytes, str],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Awaitable[Optional[Any]]:
        try:
            self._sock.send(data, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_with_backpressure(data, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> Awaitable[Union[bytes, Any]]:
        if not copy:
            from pyomq import Frame

            async def _wrap() -> Any:
                data = await self._add_recv_event(self._sock._try_recv)
                return Frame(data)

            return asyncio.ensure_future(_wrap())
        return self._add_recv_event(self._sock._try_recv)

    def send_multipart(
        self,
        parts: List[Union[bytes, str]],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> Awaitable[Optional[Any]]:
        try:
            self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            if e.errno == _EAGAIN:
                return self._send_multipart_with_backpressure(parts, flags)
            raise error.from_native(e) from None
        return _SEND_DONE

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> Awaitable[Union[List[bytes], List[Any]]]:
        if not copy:
            from pyomq import Frame

            async def _wrap() -> List[Any]:
                parts = await self._add_recv_event(self._sock._try_recv_multipart)
                return [Frame(p) for p in parts]

            return asyncio.ensure_future(_wrap())
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
            recv_mode: Optional[int] = None,
            send_mode: Optional[int] = None,
        ) -> None:
            self._sock._set_wakeup_modes(recv_mode=recv_mode, send_mode=send_mode)

        def _clear_wakeup_modes(
            self,
            *,
            recv_mode: Optional[int] = None,
            send_mode: Optional[int] = None,
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
            self, data: Union[bytes, str], flags: int
        ) -> asyncio.Future[Any]:
            def try_send() -> Optional[bool]:
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
            self, parts: List[Union[bytes, str]], flags: int
        ) -> asyncio.Future[Any]:
            def try_send() -> Optional[bool]:
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
        ) -> Union[asyncio.Future[Any], _RecvFuture]:
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

        def _send_with_backpressure(
            self, data: Union[bytes, str], flags: int
        ) -> _RecvFuture:
            fd = self._sock._send_fd()

            def try_send() -> Optional[bool]:
                try:
                    self._sock.send(data, flags)
                    return True
                except _native.ZMQError as e:
                    if e.errno == _EAGAIN:
                        return None
                    raise

            return _RecvFuture(try_send, fd)

        def _send_multipart_with_backpressure(
            self, parts: List[Union[bytes, str]], flags: int
        ) -> _RecvFuture:
            fd = self._sock._send_fd()

            def try_send() -> Optional[bool]:
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
    ) -> Awaitable[Optional[Any]]:
        return self.send(u.encode(encoding), flags)

    async def recv_string(self, flags: int = 0, encoding: str = "utf-8") -> str:
        return (await self.recv(flags)).decode(encoding)

    def send_json(
        self, obj: Any, flags: int = 0, **kwargs: Any
    ) -> Awaitable[Optional[Any]]:
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    async def recv_json(self, flags: int = 0, **kwargs: Any) -> Any:
        return json.loads(await self.recv(flags), **kwargs)

    def send_pyobj(
        self, obj: Any, flags: int = 0, protocol: int = -1
    ) -> Awaitable[Optional[Any]]:
        return self.send(pickle.dumps(obj, protocol), flags)

    async def recv_pyobj(self, flags: int = 0) -> Any:
        return pickle.loads(await self.recv(flags))

    def send_serialized(
        self,
        msg: Any,
        serialize: Callable[[Any], List[Union[bytes, str]]],
        flags: int = 0,
        copy: bool = True,
        **kwargs: Any,
    ) -> Awaitable[Optional[Any]]:
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    async def recv_serialized(
        self,
        deserialize: Callable[[List[bytes]], Any],
        flags: int = 0,
        copy: bool = True,
    ) -> Any:
        frames = await self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    # ── Options (sync -- matches pyzmq) ──────────────────────────────

    def setsockopt(self, option: int, value: Any) -> Any:
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def getsockopt(self, option: int) -> Any:
        from pyomq import LAST_ENDPOINT

        if option == LAST_ENDPOINT:
            return self._last_endpoint
        try:
            return self._sock.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option: int, value: Any) -> Any:
        return self.setsockopt(option, value)

    def get(self, option: int) -> Any:
        return self.getsockopt(option)

    def setsockopt_string(
        self, option: int, value: str, encoding: str = "utf-8"
    ) -> Any:
        return self.setsockopt(option, value.encode(encoding))

    def getsockopt_string(self, option: int, encoding: str = "utf-8") -> str:
        v = self.getsockopt(option)
        if isinstance(v, bytes):
            return v.decode(encoding)
        return str(v)

    set_string = setsockopt_string
    get_string = getsockopt_string

    def set_curve_auth(self, auth: Any) -> Any:
        try:
            return self._sock.set_curve_auth(auth)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        except AttributeError:
            from . import ZMQNotImplementedError

            raise ZMQNotImplementedError("curve feature not compiled")

    def set_hwm(self, value: int) -> None:
        self.setsockopt(SNDHWM, value)
        self.setsockopt(RCVHWM, value)

    def get_hwm(self) -> int:
        return self.getsockopt(SNDHWM)

    hwm = property(get_hwm, set_hwm)

    # ── Subscriptions ────────────────────────────────────────────────

    def subscribe(self, prefix: Union[bytes, str]) -> None:
        try:
            return self._sock.subscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unsubscribe(self, prefix: Union[bytes, str]) -> None:
        try:
            return self._sock.unsubscribe(prefix)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def join(self, group: Union[bytes, str]) -> None:
        try:
            return self._sock.join(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def leave(self, group: Union[bytes, str]) -> None:
        try:
            return self._sock.leave(group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Monitoring ───────────────────────────────────────────────────

    def monitor(self) -> Any:
        return self._sock.monitor()

    def connections(self) -> Any:
        return self._sock.connections()

    def connection_info(self, connection_id: int) -> Any:
        return self._sock.connection_info(connection_id)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self, linger: Optional[int] = None) -> None:
        if not self._closed:
            self._closed = True
            self._sock.close(linger)

    def __del__(self) -> None:
        self.close()

    async def poll(self, timeout: Optional[int] = None, flags: int = POLLIN) -> int:
        p = Poller()
        p.register(self, flags)
        evts = await p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0

    def __enter__(self) -> Socket:
        return self

    def __exit__(self, *args: Any) -> bool:
        self.close()
        return False

    async def __aenter__(self) -> Socket:
        return self

    async def __aexit__(self, *args: Any) -> bool:
        self.close()
        return False


class Poller:
    """Async poller for ZMQ sockets."""

    _sockets: Dict[int, Tuple[Socket, int]]

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
    def sockets(self) -> List[Tuple[Socket, int]]:
        return [(s, f) for s, f in self._sockets.values()]

    async def poll(self, timeout: Optional[int] = None) -> List[Tuple[Socket, int]]:
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
    """Async context for creating ZMQ sockets."""

    _socket_class: Optional[type] = None
    _ctx: _native.AsyncContext
    _closed: bool
    _sockets: weakref.WeakSet[Socket]
    _ctx_id: int

    def __init__(
        self, io_threads: int = 1, *, _shadow_ctx: Optional[_SyncContext] = None
    ) -> None:
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
    def closed(self) -> bool:
        return self._closed

    def socket(
        self, socket_type: int, socket_class: Optional[type] = None, **kwargs: Any
    ) -> Any:
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

    def term(self) -> None:
        self._closed = True
        for s in list(self._sockets):
            if not s.closed:
                s.close()
        self._sockets.clear()

    def destroy(self, linger: Optional[int] = None) -> None:
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
