"""pyomq - Python binding for omq.rs.

Drop-in pyzmq replacement on the common path. Use as::

    import pyomq as zmq

The Socket / Context API mirrors pyzmq's surface; constants
(``zmq.PUSH``, ``zmq.SUBSCRIBE``, ``zmq.LINGER`` ...) match libzmq's
integer values, so existing pyzmq code typically just works.

For asynchronous code::

    import pyomq.asyncio as zmq_async
"""

from __future__ import annotations

import errno as _errno
import itertools
import json
import os
import pickle
import select as _select
import sys
import threading
import weakref
from typing import (
    Any,
    Callable,
    cast,
    Final,
    Iterable,
    Iterator,
    Protocol,
    Self,
    overload,
)

from . import _native  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]
from . import error as error  # noqa: F401

from ._native import (  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]
    backend_name,
    version,
    Frame,
    # Socket types
    PAIR,
    PUB,
    SUB,
    REQ,
    REP,
    DEALER,
    ROUTER,
    PULL,
    PUSH,
    XPUB,
    XSUB,
    STREAM,
    # Draft socket types (RFC 41 / 48 / 49 / 51 + PEER)
    SERVER,
    CLIENT,
    RADIO,
    DISH,
    GATHER,
    SCATTER,
    PEER,
    CHANNEL,
    # Option constants
    AFFINITY,
    IDENTITY,
    SUBSCRIBE,
    UNSUBSCRIBE,
    RCVMORE,
    TYPE,
    LINGER,
    RECONNECT_IVL,
    RECONNECT_IVL_MAX,
    BACKLOG,
    MAXMSGSIZE,
    SNDHWM,
    RCVHWM,
    RCVTIMEO,
    SNDTIMEO,
    ROUTER_MANDATORY,
    IMMEDIATE,
    IPV6,
    HEARTBEAT_IVL,
    HEARTBEAT_TTL,
    HEARTBEAT_TIMEOUT,
    HANDSHAKE_IVL,
    CONFLATE,
    TCP_KEEPALIVE,
    TCP_KEEPALIVE_IDLE,
    TCP_KEEPALIVE_CNT,
    TCP_KEEPALIVE_INTVL,
    SNDMORE,
    NOBLOCK,
    DONTWAIT,
    # CURVE option ids
    CURVE_SERVER,
    CURVE_PUBLICKEY,
    CURVE_SECRETKEY,
    CURVE_SERVERKEY,
    # omq-specific options
    OMQ_ON_MUTE,
    OMQ_COMPRESSION_LEVEL,
    OMQ_COMPRESSION_DICT,
    OMQ_COMPRESSION_AUTO_TRAIN,
    OMQ_ON_MUTE_BLOCK,
    OMQ_ON_MUTE_DROP_NEWEST,
    OMQ_ON_MUTE_DROP_OLDEST,
)

from .error import (  # noqa: F401  re-exports
    ZMQBaseError,
    ZMQError,
    Again,
    ContextTerminated,
    ZMQBindError,
    ZMQVersionError,
    InterruptedSystemCall,
    NotImplementedError as ZMQNotImplementedError,
)

# ── Constants ─────────────────────────────────────────────────────────

POLLIN: Final[int] = 1
POLLOUT: Final[int] = 2
POLLERR: Final[int] = 4
POLLPRI: Final[int] = 32
HWM: Final[int] = 1

# Windows specific constants
_IS_WINDOWS: Final[bool] = sys.platform == "win32"
_WAKEUP_MODE_NONE: Final[int] = 0
_WAKEUP_MODE_ASYNC: Final[int] = 1
_WAKEUP_MODE_SYNC: Final[int] = 2

ROUTING_ID: Final[int] = 5
LAST_ENDPOINT: Final[int] = 32
FD: Final[int] = 14
EVENTS: Final[int] = 15
MECHANISM: Final[int] = 43
SNDBUF: Final[int] = 11
RCVBUF: Final[int] = 12
RATE: Final[int] = 8
CONNECT_TIMEOUT: Final[int] = 79
XPUB_VERBOSE: Final[int] = 40
PROBE_ROUTER: Final[int] = 51
REQ_CORRELATE: Final[int] = 52
REQ_RELAXED: Final[int] = 53
ROUTER_HANDOVER: Final[int] = 56
IPV4ONLY: Final[int] = 31
TCP_ACCEPT_FILTER: Final[int] = 38
TCP_MAXRT: Final[int] = 80
MULTICAST_HOPS: Final[int] = 25
RECOVERY_IVL: Final[int] = 9
RECONNECT_STOP: Final[int] = 109
PLAIN_SERVER: Final[int] = 44
PLAIN_USERNAME: Final[int] = 45
PLAIN_PASSWORD: Final[int] = 46
ZAP_DOMAIN: Final[int] = 55

FORWARDER: Final[int] = 2
QUEUE: Final[int] = 3
STREAMER: Final[int] = 1

NULL: Final[int] = 0
PLAIN: Final[int] = 1
CURVE: Final[int] = 2

ETERM: Final[int] = 156384765
ENOTSOCK: Final[int] = 108
COPY_THRESHOLD: Final[int] = 65536

# errno constants (pyzmq exposes these at top level)
EAGAIN: Final[int] = _errno.EAGAIN
ENOTSUP: Final[int] = _errno.ENOTSUP
EINVAL: Final[int] = _errno.EINVAL
EFAULT: Final[int] = _errno.EFAULT
ENOMEM: Final[int] = _errno.ENOMEM
ENODEV: Final[int] = _errno.ENODEV
EMSGSIZE: Final[int] = _errno.EMSGSIZE
EAFNOSUPPORT: Final[int] = _errno.EAFNOSUPPORT
ENETUNREACH: Final[int] = _errno.ENETUNREACH
ECONNABORTED: Final[int] = _errno.ECONNABORTED
ECONNRESET: Final[int] = _errno.ECONNRESET
ENOTCONN: Final[int] = _errno.ENOTCONN
ETIMEDOUT: Final[int] = _errno.ETIMEDOUT
EHOSTUNREACH: Final[int] = _errno.EHOSTUNREACH
ENETRESET: Final[int] = _errno.ENETRESET
EADDRINUSE: Final[int] = _errno.EADDRINUSE
EADDRNOTAVAIL: Final[int] = _errno.EADDRNOTAVAIL

__version__: Final[str] = version()
zmq_version_info: Final[tuple[int, int, int]] = (4, 3, 4)


# ── Top-level functions ──────────────────────────────────────────────


def strerror(errnum: int) -> str:
    return os.strerror(errnum)


def zmq_version() -> str:
    return "%d.%d.%d" % zmq_version_info


def pyomq_version() -> str:
    return __version__


def pyomq_version_info() -> tuple[int, ...]:
    parts = __version__.split(".")
    return tuple(int(p) for p in parts[:3])


def has(capability: str) -> bool:
    cap = capability.lower()
    if cap in ("ipc", "inproc"):
        return True
    if hasattr(_native, "has_feature"):
        return _native.has_feature(cap)
    return False


def curve_keypair() -> tuple[bytes, bytes]:
    if not hasattr(_native, "curve_keypair"):
        raise ZMQNotImplementedError("curve feature not compiled")
    return _native.curve_keypair()


def curve_public(secret: bytes | str) -> bytes:
    if not hasattr(_native, "curve_public"):
        raise ZMQNotImplementedError("curve feature not compiled")
    if isinstance(secret, str):
        secret = secret.encode("ascii")
    return _native.curve_public(secret)


if hasattr(_native, "PeerInfo"):
    PeerInfo = _native.PeerInfo


# ── Socket option attribute map ──────────────────────────────────────

_TYPE_NAMES: Final[dict[int, str]] = {
    PAIR: "PAIR",
    PUB: "PUB",
    SUB: "SUB",
    REQ: "REQ",
    REP: "REP",
    DEALER: "DEALER",
    ROUTER: "ROUTER",
    PULL: "PULL",
    PUSH: "PUSH",
    XPUB: "XPUB",
    XSUB: "XSUB",
    SERVER: "SERVER",
    CLIENT: "CLIENT",
    RADIO: "RADIO",
    DISH: "DISH",
    GATHER: "GATHER",
    SCATTER: "SCATTER",
    PEER: "PEER",
    CHANNEL: "CHANNEL",
    STREAM: "STREAM",
}


# ── MessageTracker / Message / Frame (pyzmq compat) ─────────────────


class NotDone(ZMQBaseError):
    pass


class MessageTracker:
    """Tracks the delivery status of a message (pyzmq compatibility)."""

    done: bool

    def __init__(self, *args: Any, _pending: bool = False, **kwargs: Any) -> None:
        self.done = not _pending

    def wait(self, timeout: int | None = None) -> None:
        if not self.done:
            raise NotDone


Message = Frame


# ── Socket wrapper ───────────────────────────────────────────────────


class _NativeSocket(Protocol):
    """Protocol for native socket implementation (sync or async)."""

    def getsockopt(self, option: int) -> Any: ...

    def setsockopt(self, option: int, value: Any) -> Any: ...

    def bind(self, endpoint: str | bytes) -> str | bytes: ...

    def connect(self, endpoint: str | bytes) -> None: ...

    def unbind(self, endpoint: str | bytes) -> None: ...

    def disconnect(self, endpoint: str | bytes) -> None: ...

    def subscribe(self, prefix: bytes | str) -> None: ...

    def unsubscribe(self, prefix: bytes | str) -> None: ...

    def join(self, group: bytes | str) -> None: ...

    def leave(self, group: bytes | str) -> None: ...

    def monitor(self) -> Any: ...

    def connections(self) -> Any: ...

    def connection_info(self, connection_id: int) -> Any: ...

    def set_curve_auth(self, auth: Any) -> Any: ...

    def close(self, linger: int | None = None) -> None: ...


# Socket option descriptor for IDE autocomplete support
class _SocketOptionDescriptor:
    """Descriptor for socket options providing IDE autocomplete."""

    def __init__(self, option_code: int) -> None:
        self.option_code = option_code

    def __get__(self, obj: Any, objtype: type[Any] | None = None) -> Any:
        if obj is None:
            return self
        if self.option_code == LAST_ENDPOINT:
            return obj._last_endpoint
        return obj.getsockopt(self.option_code)

    def __set__(self, obj: Any, value: Any) -> None:
        obj.setsockopt(self.option_code, value)


class _SocketOptionsBase:
    """Base class with socket option descriptors and shared methods."""

    # Attributes (subclasses must define these)
    _sock: _NativeSocket
    _context: Context
    _closed: bool
    _last_endpoint: bytes | str | None

    # Socket options
    affinity = _SocketOptionDescriptor(AFFINITY)
    identity = _SocketOptionDescriptor(IDENTITY)
    routing_id = _SocketOptionDescriptor(ROUTING_ID)
    rcvmore = _SocketOptionDescriptor(RCVMORE)
    sndhwm = _SocketOptionDescriptor(SNDHWM)
    rcvhwm = _SocketOptionDescriptor(RCVHWM)
    linger = _SocketOptionDescriptor(LINGER)
    reconnect_ivl = _SocketOptionDescriptor(RECONNECT_IVL)
    reconnect_ivl_max = _SocketOptionDescriptor(RECONNECT_IVL_MAX)
    backlog = _SocketOptionDescriptor(BACKLOG)
    maxmsgsize = _SocketOptionDescriptor(MAXMSGSIZE)
    rcvtimeo = _SocketOptionDescriptor(RCVTIMEO)
    sndtimeo = _SocketOptionDescriptor(SNDTIMEO)
    ipv6 = _SocketOptionDescriptor(IPV6)
    immediate = _SocketOptionDescriptor(IMMEDIATE)
    router_mandatory = _SocketOptionDescriptor(ROUTER_MANDATORY)
    tcp_keepalive = _SocketOptionDescriptor(TCP_KEEPALIVE)
    tcp_keepalive_idle = _SocketOptionDescriptor(TCP_KEEPALIVE_IDLE)
    tcp_keepalive_cnt = _SocketOptionDescriptor(TCP_KEEPALIVE_CNT)
    tcp_keepalive_intvl = _SocketOptionDescriptor(TCP_KEEPALIVE_INTVL)
    heartbeat_ivl = _SocketOptionDescriptor(HEARTBEAT_IVL)
    heartbeat_ttl = _SocketOptionDescriptor(HEARTBEAT_TTL)
    heartbeat_timeout = _SocketOptionDescriptor(HEARTBEAT_TIMEOUT)
    handshake_ivl = _SocketOptionDescriptor(HANDSHAKE_IVL)
    conflate = _SocketOptionDescriptor(CONFLATE)
    curve_server = _SocketOptionDescriptor(CURVE_SERVER)
    curve_publickey = _SocketOptionDescriptor(CURVE_PUBLICKEY)
    curve_secretkey = _SocketOptionDescriptor(CURVE_SECRETKEY)
    curve_serverkey = _SocketOptionDescriptor(CURVE_SERVERKEY)
    on_mute = _SocketOptionDescriptor(OMQ_ON_MUTE)
    compression_level = _SocketOptionDescriptor(OMQ_COMPRESSION_LEVEL)
    compression_dict = _SocketOptionDescriptor(OMQ_COMPRESSION_DICT)
    compression_auto_train = _SocketOptionDescriptor(OMQ_COMPRESSION_AUTO_TRAIN)
    sndbuf = _SocketOptionDescriptor(SNDBUF)
    rcvbuf = _SocketOptionDescriptor(RCVBUF)
    mechanism = _SocketOptionDescriptor(MECHANISM)
    plain_server = _SocketOptionDescriptor(PLAIN_SERVER)
    plain_username = _SocketOptionDescriptor(PLAIN_USERNAME)
    plain_password = _SocketOptionDescriptor(PLAIN_PASSWORD)

    # ── Shared properties ────────────────────────────────────────────

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def context(self) -> Context:
        return self._context

    @property
    def socket_type(self) -> int:
        return self._sock.getsockopt(TYPE)

    @property
    def last_endpoint(self) -> bytes | str | None:
        return self._last_endpoint

    @property
    def underlying(self) -> _SocketOptionsBase:
        return self

    # ── Options ──────────────────────────────────────────────────────

    def setsockopt(self, option: int, value: Any) -> Any:
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def getsockopt(self, option: int) -> Any:
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


class _BaseSocket(_SocketOptionsBase):
    """Base class for Socket and asyncio.Socket.

    Split from _SocketOptionsBase since _ShadowSocket has a smaller API.

    """

    def set_curve_auth(self, auth: Any) -> Any:
        try:
            return self._sock.set_curve_auth(auth)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        except AttributeError:
            raise ZMQNotImplementedError("curve feature not compiled")

    def set_hwm(self, value: int) -> None:
        self.setsockopt(SNDHWM, value)
        self.setsockopt(RCVHWM, value)

    def get_hwm(self) -> int:
        return self.getsockopt(SNDHWM)

    hwm = property(get_hwm, set_hwm)

    # ── Shared I/O methods ───────────────────────────────────────────

    def fileno(self) -> int:
        return self.getsockopt(FD)

    @overload
    def bind(self, endpoint: str) -> str: ...

    @overload
    def bind(self, endpoint: bytes) -> bytes: ...

    def bind(self, endpoint):
        try:
            ep = self._sock.bind(self._context._namespace_inproc(endpoint))
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            return ep
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def bind_to_random_port(
        self,
        addr: str,
        min_port: int = 49152,
        max_port: int = 65536,
        max_tries: int = 100,
    ) -> int:
        ep = self.bind(f"{addr}:0")
        if isinstance(ep, bytes):
            ep = ep.decode()
        return int(ep.rsplit(":", 1)[1])

    def connect(self, endpoint: str | bytes) -> None:
        if isinstance(endpoint, bytes):
            endpoint = endpoint.decode("utf-8")
        try:
            self._sock.connect(self._context._namespace_inproc(endpoint))
            self._last_endpoint = (
                endpoint.encode() if isinstance(endpoint, str) else endpoint
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unbind(self, endpoint: str | bytes) -> None:
        try:
            return self._sock.unbind(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def disconnect(self, endpoint: str | bytes) -> None:
        try:
            return self._sock.disconnect(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Subscriptions ────────────────────────────────────────────────

    def subscribe(self, prefix: bytes | str) -> None:
        try:
            return self._sock.subscribe(
                prefix.encode() if isinstance(prefix, str) else prefix
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def unsubscribe(self, prefix: bytes | str) -> None:
        try:
            return self._sock.unsubscribe(
                prefix.encode() if isinstance(prefix, str) else prefix
            )
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def join(self, group: bytes | str) -> None:
        try:
            return self._sock.join(group.encode() if isinstance(group, str) else group)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def leave(self, group: bytes | str) -> None:
        try:
            return self._sock.leave(group.encode() if isinstance(group, str) else group)
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

    def close(self, linger: int | None = None) -> None:
        if not self._closed:
            self._closed = True
            self._sock.close(linger)

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> bool:
        self.close()
        return False


class _SocketMeta(type):
    """Metaclass for Socket that allows checking for async Socket instances."""

    def __instancecheck__(cls, instance: Any) -> bool:
        if type.__instancecheck__(cls, instance):
            return True
        if cls is Socket:
            amod = sys.modules.get("pyomq.asyncio")
            if amod is not None and type.__instancecheck__(amod.Socket, instance):
                return True
        return False


class Socket(_BaseSocket, metaclass=_SocketMeta):
    """Synchronous ZMQ socket wrapper."""

    _sock: _native.Socket
    _context: Context
    _closed: bool
    _pid: int
    _binds: list[str | bytes]
    _connects: list[str | bytes]

    def __init__(self, _sock: _native.Socket, _context: Context) -> None:
        self._sock = _sock
        self._context = _context
        self._closed = False
        self._last_endpoint = None
        self._pid = os.getpid()
        self._binds = []
        self._connects = []

    def __class_getitem__(cls, item: Any) -> type[Socket]:
        return cls

    @classmethod
    def shadow(cls, socket: Any) -> Socket | _ShadowSocket:
        from . import asyncio as _zmq_async

        if isinstance(socket, _zmq_async.Socket):
            return _ShadowSocket(socket)
        return socket

    def __repr__(self) -> str:
        st = _TYPE_NAMES.get(self.socket_type, str(self.socket_type))
        return f"<pyomq.Socket(pyomq.{st}) at {id(self):#x}>"

    # ── I/O ──────────────────────────────────────────────────────────

    def send(
        self,
        data: bytes | str,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        try:
            self._sock.send(data, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        if track:
            return MessageTracker(_pending=True)
        return None

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame:
        try:
            if copy:
                return self._sock.recv(flags)
            return self._sock.recv_frame(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send_multipart(
        self,
        parts: Iterable[bytes | str],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        parts = [p.encode("utf-8") if isinstance(p, str) else p for p in parts]
        try:
            self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        if track:
            return MessageTracker(_pending=True)
        return None

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes | Frame]:
        try:
            if copy:
                return self._sock.recv_multipart(flags)
            return self._sock.recv_multipart_frames(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    # ── Serialization helpers ────────────────────────────────────────

    def send_string(
        self, u: str, flags: int = 0, encoding: str = "utf-8"
    ) -> MessageTracker | None:
        return self.send(u.encode(encoding), flags)

    def recv_string(self, flags: int = 0, encoding: str = "utf-8") -> str:
        return self.recv(flags).decode(encoding)

    def send_json(
        self, obj: Any, flags: int = 0, **kwargs: Any
    ) -> MessageTracker | None:
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    def recv_json(self, flags: int = 0, **kwargs: Any) -> Any:
        return json.loads(self.recv(flags), **kwargs)

    def send_pyobj(
        self, obj: Any, flags: int = 0, protocol: int = -1
    ) -> MessageTracker | None:
        return self.send(pickle.dumps(obj, protocol), flags)

    def recv_pyobj(self, flags: int = 0) -> Any:
        return pickle.loads(self.recv(flags))

    def send_serialized(
        self,
        msg: Any,
        serialize: Callable[[Any], list[bytes]],
        flags: int = 0,
        copy: bool = True,
        **kwargs: Any,
    ) -> MessageTracker | None:
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    def recv_serialized(
        self,
        deserialize: Callable[[list[bytes]], Any],
        flags: int = 0,
        copy: bool = True,
    ) -> Any:
        frames = self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    def poll(self, timeout: int | None = None, flags: int = POLLIN) -> int:
        p = Poller()
        p.register(self, flags)
        evts = p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0


# ── Shadow socket (sync recv bridge over async handle) ──────────────


class _ShadowSocket(_SocketOptionsBase):
    """Blocking recv bridge over an async socket's native handle.

    Returned by Socket.shadow() when given a pyomq.asyncio.Socket.
    Provides sync recv via the native readiness signal without entering the
    asyncio event loop, matching pyzmq's shadow(underlying) behavior.

    """

    _async_socket: Any  # pyomq.asyncio.Socket
    _native: _native.AsyncSocket
    _context: Context
    _closed: bool
    _recv_waiter_pending: bool
    _send_waiter_pending: bool
    _recv_wakeup_event: threading.Event
    _send_wakeup_event: threading.Event

    def __init__(self, async_socket: Any) -> None:
        self._async_socket = async_socket
        self._native = async_socket._sock
        self._context = async_socket._context
        self._closed = False
        self._last_endpoint = async_socket._last_endpoint
        if _IS_WINDOWS:
            self._recv_waiter_pending = False
            self._send_waiter_pending = False
            self._recv_wakeup_event = async_socket._recv_wakeup_event
            self._send_wakeup_event = async_socket._send_wakeup_event

    @property
    def closed(self) -> bool:
        return self._closed or self._async_socket._closed

    @property
    def context(self) -> Context:
        return self._context

    @property
    def socket_type(self) -> int:
        return self._native.getsockopt(TYPE)

    @property
    def underlying(self) -> _ShadowSocket:
        return self

    def getsockopt(self, option: int) -> Any:
        try:
            return self._native.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def setsockopt(self, option: int, value: Any) -> Any:
        try:
            return self._native.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option: int, value: Any) -> Any:
        return self.setsockopt(option, value)

    def get(self, option: int) -> Any:
        return self.getsockopt(option)

    if _IS_WINDOWS:

        def _register_wakeup_hooks(self) -> None:
            self._async_socket._register_wakeup_hooks()
    else:

        def _register_wakeup_hooks(self) -> None:
            return None

    if _IS_WINDOWS:

        def _blocking_recv(self, try_fn: Callable[[], Any]) -> Any:
            if self._recv_waiter_pending:
                raise RuntimeError(
                    "cannot have more than one pending recv waiter on a shadow socket"
                )
            self._recv_waiter_pending = True
            self._register_wakeup_hooks()
            self._async_socket._set_wakeup_modes(
                recv_mode=_WAKEUP_MODE_SYNC,
            )
            try:
                try:
                    result = try_fn()
                except _native.ZMQError as e:
                    raise error.from_native(e) from None
                if result is not None:
                    return result

                while True:
                    self._recv_wakeup_event.clear()
                    try:
                        result = try_fn()
                    except _native.ZMQError as e:
                        raise error.from_native(e) from None
                    if result is not None:
                        return result
                    self._recv_wakeup_event.wait()
                    try:
                        result = try_fn()
                    except _native.ZMQError as e:
                        raise error.from_native(e) from None
                    if result is not None:
                        return result
            finally:
                self._recv_waiter_pending = False
                self._async_socket._clear_wakeup_modes(
                    recv_mode=_WAKEUP_MODE_SYNC,
                )
    else:

        def _blocking_recv(self, try_fn: Callable[[], Any]) -> Any:
            try:
                result = try_fn()
            except _native.ZMQError as e:
                raise error.from_native(e) from None
            if result is not None:
                return result

            fd = self._native._recv_fd()
            try:
                try:
                    result = try_fn()
                except _native.ZMQError as e:
                    raise error.from_native(e) from None
                if result is not None:
                    return result

                while True:
                    _select.select([fd], [], [])
                    try:
                        os.read(fd, 8)
                    except OSError:
                        pass
                    try:
                        result = try_fn()
                    except _native.ZMQError as e:
                        raise error.from_native(e) from None
                    if result is not None:
                        return result
            finally:
                os.close(fd)

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame:
        if copy:
            return self._blocking_recv(self._native._try_recv)
        return self._blocking_recv(self._native._try_recv_frame)

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes | Frame]:
        if copy:
            return self._blocking_recv(self._native._try_recv_multipart)
        return self._blocking_recv(self._native._try_recv_multipart_frames)

    def send(
        self,
        data: bytes | str,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        self._blocking_send(lambda: self._native.send(data, flags))
        if track:
            return MessageTracker(_pending=True)
        return None

    def send_multipart(
        self,
        parts: list[bytes | str],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        self._blocking_send(lambda: self._native.send_multipart(parts, flags))
        if track:
            return MessageTracker(_pending=True)
        return None

    if _IS_WINDOWS:

        def _blocking_send(self, send_fn: Callable[[], None]) -> None:
            if self._send_waiter_pending:
                raise RuntimeError(
                    "cannot have more than one pending send waiter on a shadow socket"
                )
            self._send_waiter_pending = True
            self._register_wakeup_hooks()
            self._async_socket._set_wakeup_modes(
                send_mode=_WAKEUP_MODE_SYNC,
            )
            try:
                try:
                    send_fn()
                    return
                except _native.ZMQError as e:
                    if getattr(e, "errno", None) != _errno.EAGAIN:
                        raise error.from_native(e) from None

                while True:
                    self._send_wakeup_event.clear()
                    try:
                        send_fn()
                        return
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
                    self._send_wakeup_event.wait()
                    try:
                        send_fn()
                        return
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
            finally:
                self._send_waiter_pending = False
                self._async_socket._clear_wakeup_modes(
                    send_mode=_WAKEUP_MODE_SYNC,
                )
    else:

        def _blocking_send(self, send_fn: Callable[[], None]) -> None:
            try:
                send_fn()
                return
            except _native.ZMQError as e:
                if getattr(e, "errno", None) != _errno.EAGAIN:
                    raise error.from_native(e) from None

            fd = self._native._send_fd()
            try:
                try:
                    send_fn()
                    return
                except _native.ZMQError as e:
                    if getattr(e, "errno", None) != _errno.EAGAIN:
                        raise error.from_native(e) from None

                while True:
                    _select.select([fd], [], [])
                    try:
                        os.read(fd, 8)
                    except OSError:
                        pass
                    try:
                        send_fn()
                        return
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
            finally:
                os.close(fd)

    def close(self, linger: int | None = None) -> None:
        pass


# ── Context wrapper ──────────────────────────────────────────────────

_next_ctx_id: Iterator[int] = itertools.count(1)


class _ContextMeta(type):
    """Context metaclass with per-subclass singleton storage."""

    def __init__(
        cls, name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any]
    ) -> None:
        super().__init__(name, bases, namespace)
        cls._instance_lock = threading.Lock()
        cls._instance = None


class Context(metaclass=_ContextMeta):
    """Synchronous ZMQ context."""

    _instance: Context | None
    _instance_lock: threading.Lock
    _socket_class: type[Socket] | None = None  # set after Socket is defined
    _ctx: _native.Context
    _is_shadow: bool
    _closed: bool
    _sockets: weakref.WeakSet[Socket]
    _ctx_id: int

    def __init__(
        self, io_threads: int = 1, *, _shadow_ctx: Context | None = None
    ) -> None:
        if _shadow_ctx is not None:
            if isinstance(_shadow_ctx._ctx, _native.AsyncContext):
                self._ctx = _native.Context.shadow_async(_shadow_ctx._ctx)
            else:
                self._ctx = _shadow_ctx._ctx
            self._is_shadow = True
        else:
            self._ctx = _native.Context(io_threads)
            self._is_shadow = False
        self._closed = False
        self._sockets = weakref.WeakSet()
        self._ctx_id = (
            _shadow_ctx._ctx_id if _shadow_ctx is not None else next(_next_ctx_id)
        )

    def _namespace_inproc(self, endpoint: str | bytes) -> str | bytes:
        # `inproc://` names are scoped by the native context core. Keep the
        # user endpoint unchanged so LAST_ENDPOINT and errors match input.
        return endpoint

    def __class_getitem__(cls, item: Any) -> type[Context]:
        return cls

    @property
    def closed(self) -> bool:
        return self._closed

    def socket(
        self,
        socket_type: int,
        socket_class: type[Socket] | None = None,
        **kwargs: Any,
    ) -> Socket:
        native = self._ctx.socket(socket_type)
        cls = socket_class or Socket
        s = object.__new__(cls)
        s._sock = native
        s._context = self
        s._closed = False
        s._last_endpoint = None
        s._pid = os.getpid()
        self._sockets.add(s)
        return s

    @classmethod
    def shadow(cls, address: Context | int) -> Self:
        if isinstance(address, Context):
            return cls(_shadow_ctx=address)
        if isinstance(address, int):
            return cls(_shadow_ctx=cls.instance())
        raise TypeError(f"expected Context or int, got {type(address).__name__}")

    def share_key(self) -> int:
        """Return the opaque native context-core key for this process."""
        return int(self._ctx.share_key())

    @classmethod
    def from_share_key(cls, key: int) -> Self:
        """Create a Context wrapper for an existing native context core."""
        obj = object.__new__(cls)
        obj._ctx = _native.Context.from_share_key(key)
        obj._is_shadow = True
        obj._closed = False
        obj._sockets = weakref.WeakSet()
        obj._ctx_id = next(_next_ctx_id)
        return cast(Self, obj)

    @classmethod
    def instance(cls, io_threads: int = 1) -> Self:
        with cls._instance_lock:
            if cls._instance is None or cls._instance._closed:
                cls._instance = cls(io_threads)
            return cast(Self, cls._instance)

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


# ── Poller ───────────────────────────────────────────────────────────


class Poller:
    """Synchronous poller for ZMQ sockets."""

    _sockets: dict[int, tuple[Socket, int]]

    def __init__(self) -> None:
        self._sockets = {}  # native_socket_id -> (Socket, flags)

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

    def poll(self, timeout: int | None = None) -> list[tuple[Socket, int]]:
        if not self._sockets:
            return []
        pollin_socks = [s._sock for k, (s, f) in self._sockets.items() if f & POLLIN]
        if not pollin_socks:
            return []
        t = None if (timeout is None or timeout < 0) else int(timeout)
        ready_ids = _native.wait_any(pollin_socks, t)
        return [
            (self._sockets[rid][0], POLLIN) for rid in ready_ids if rid in self._sockets
        ]


# ── select ──────────────────────────────────────────────────────────


def select(
    rlist: Iterable[Socket],
    wlist: Iterable[Socket],
    xlist: Iterable[Socket],
    timeout: float | None = None,
) -> tuple[list[Socket], list[Socket], list[Socket]]:
    if timeout is not None:
        timeout_ms = int(timeout * 1000)
    else:
        timeout_ms = None
    p = Poller()
    for s in rlist:
        p.register(s, POLLIN)
    for s in wlist:
        p.register(s, POLLOUT)
    evts = p.poll(timeout_ms)
    rready: list[Socket] = []
    wready: list[Socket] = []
    xready: list[Socket] = []
    for sock, mask in evts:
        if mask & POLLIN:
            rready.append(sock)
        if mask & POLLOUT:
            wready.append(sock)
    return rready, wready, xready


# ── proxy ────────────────────────────────────────────────────────────


def proxy(frontend: Socket, backend: Socket, capture: Socket | None = None) -> None:
    _native.native_proxy(
        frontend._sock,
        backend._sock,
        capture._sock if capture is not None else None,
    )


def proxy_steerable(
    frontend: Socket,
    backend: Socket,
    capture: Socket | None = None,
    control: Socket | None = None,
) -> None:
    _native.native_proxy(
        frontend._sock,
        backend._sock,
        capture._sock if capture is not None else None,
        control._sock if control is not None else None,
    )


def device(device_type: int, frontend: Socket, backend: Socket) -> None:
    proxy(frontend, backend)


# ── ZMQStream re-export ─────────────────────────────────────────────

from .zmqstream import ZMQStream  # noqa: E402

__all__ = [
    "Context",
    "Socket",
    "Poller",
    "ZMQStream",
    "ZMQBaseError",
    "ZMQError",
    "ZMQBindError",
    "ZMQVersionError",
    "Again",
    "ContextTerminated",
    "InterruptedSystemCall",
    "backend_name",
    "version",
    "proxy",
    "proxy_steerable",
    "device",
    "strerror",
    "has",
    "select",
    "error",
    # socket types
    "PAIR",
    "PUB",
    "SUB",
    "REQ",
    "REP",
    "DEALER",
    "ROUTER",
    "PULL",
    "PUSH",
    "XPUB",
    "XSUB",
    "STREAM",
    # draft socket types
    "SERVER",
    "CLIENT",
    "RADIO",
    "DISH",
    "GATHER",
    "SCATTER",
    "PEER",
    "CHANNEL",
    # options
    "AFFINITY",
    "IDENTITY",
    "ROUTING_ID",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "RCVMORE",
    "TYPE",
    "LINGER",
    "RECONNECT_IVL",
    "RECONNECT_IVL_MAX",
    "BACKLOG",
    "MAXMSGSIZE",
    "SNDHWM",
    "RCVHWM",
    "RCVTIMEO",
    "SNDTIMEO",
    "ROUTER_MANDATORY",
    "IMMEDIATE",
    "IPV6",
    "HEARTBEAT_IVL",
    "HEARTBEAT_TTL",
    "HEARTBEAT_TIMEOUT",
    "HANDSHAKE_IVL",
    "CONFLATE",
    "TCP_KEEPALIVE",
    "TCP_KEEPALIVE_IDLE",
    "TCP_KEEPALIVE_CNT",
    "TCP_KEEPALIVE_INTVL",
    "SNDMORE",
    "NOBLOCK",
    "DONTWAIT",
    "CURVE_SERVER",
    "CURVE_PUBLICKEY",
    "CURVE_SECRETKEY",
    "CURVE_SERVERKEY",
    "OMQ_ON_MUTE",
    "OMQ_COMPRESSION_LEVEL",
    "OMQ_COMPRESSION_DICT",
    "OMQ_COMPRESSION_AUTO_TRAIN",
    "OMQ_ON_MUTE_BLOCK",
    "OMQ_ON_MUTE_DROP_NEWEST",
    "OMQ_ON_MUTE_DROP_OLDEST",
    # poll / compat constants
    "POLLIN",
    "POLLOUT",
    "POLLERR",
    "POLLPRI",
    "HWM",
    # additional compat constants
    "LAST_ENDPOINT",
    "FD",
    "EVENTS",
    "MECHANISM",
    "SNDBUF",
    "RCVBUF",
    "RATE",
    "CONNECT_TIMEOUT",
    "XPUB_VERBOSE",
    "PROBE_ROUTER",
    "REQ_CORRELATE",
    "REQ_RELAXED",
    "ROUTER_HANDOVER",
    "IPV4ONLY",
    "TCP_ACCEPT_FILTER",
    "TCP_MAXRT",
    "MULTICAST_HOPS",
    "RECOVERY_IVL",
    "RECONNECT_STOP",
    "PLAIN_SERVER",
    "PLAIN_USERNAME",
    "PLAIN_PASSWORD",
    "ZAP_DOMAIN",
    # device types
    "FORWARDER",
    "QUEUE",
    "STREAMER",
    # security mechanism constants
    "NULL",
    "PLAIN",
    "CURVE",
    # version
    "__version__",
    "zmq_version_info",
    "zmq_version",
    "pyomq_version",
    "pyomq_version_info",
    # errno constants
    "EAGAIN",
    "ENOTSUP",
    "EINVAL",
    "EFAULT",
    "ENOMEM",
    "ENODEV",
    "EMSGSIZE",
    "EAFNOSUPPORT",
    "ENETUNREACH",
    "ECONNABORTED",
    "ECONNRESET",
    "ENOTCONN",
    "ETIMEDOUT",
    "EHOSTUNREACH",
    "ENETRESET",
    "EADDRINUSE",
    "EADDRNOTAVAIL",
    # pyzmq compat types
    "NotDone",
    "MessageTracker",
    "Message",
    "Frame",
    # extra constants
    "ETERM",
    "ENOTSOCK",
    "COPY_THRESHOLD",
    # curve
    "curve_keypair",
    "curve_public",
    "PeerInfo",
]


# ── ZMQError errno patch ────────────────────────────────────────────
# pyzmq supports ZMQError(errno, msg) which sets .errno on the instance.
# The native _native.ZMQError doesn't. Patch __init__ so ipykernel's
# mock-based tests and heartbeat code can construct ZMQError(errno, msg).
_orig_zmqerror_init = _native.ZMQError.__init__


def _zmqerror_init(self, *args, **kwargs):
    if args and isinstance(args[0], int):
        _orig_zmqerror_init(self, args[1] if len(args) > 1 else "")
        self.errno = args[0]
        self.strerror = args[1] if len(args) > 1 else ""
    else:
        _orig_zmqerror_init(self, *args, **kwargs)


_native.ZMQError.__init__ = _zmqerror_init
