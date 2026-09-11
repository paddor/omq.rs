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
import types
import weakref
from collections.abc import Callable, Iterable, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Self,
    cast,
    overload,
)

from . import _native
from . import error as error
from ._native import (
    # Option constants
    AFFINITY,
    BACKLOG,
    CHANNEL,
    CLIENT,
    CONFLATE,
    CURVE_PUBLICKEY,
    CURVE_SECRETKEY,
    # CURVE option ids
    CURVE_SERVER,
    CURVE_SERVERKEY,
    DEALER,
    DISH,
    DONTWAIT,
    GATHER,
    HANDSHAKE_IVL,
    HEARTBEAT_IVL,
    HEARTBEAT_TIMEOUT,
    HEARTBEAT_TTL,
    IDENTITY,
    IMMEDIATE,
    IPV6,
    LINGER,
    MAXMSGSIZE,
    NOBLOCK,
    OMQ_COMPRESSION_AUTO_TRAIN,
    OMQ_COMPRESSION_DICT,
    OMQ_COMPRESSION_LEVEL,
    # omq-specific options
    OMQ_ON_MUTE,
    OMQ_ON_MUTE_BLOCK,
    OMQ_ON_MUTE_DROP_NEWEST,
    OMQ_ON_MUTE_DROP_OLDEST,
    # Socket types
    PAIR,
    PEER,
    PUB,
    PULL,
    PUSH,
    RADIO,
    RCVHWM,
    RCVMORE,
    RCVTIMEO,
    RECONNECT_IVL,
    RECONNECT_IVL_MAX,
    REP,
    REQ,
    ROUTER,
    ROUTER_MANDATORY,
    SCATTER,
    # Draft socket types (RFC 41 / 48 / 49 / 51 + PEER)
    SERVER,
    SNDHWM,
    SNDMORE,
    SNDTIMEO,
    STREAM,
    SUB,
    SUBSCRIBE,
    TCP_KEEPALIVE,
    TCP_KEEPALIVE_CNT,
    TCP_KEEPALIVE_IDLE,
    TCP_KEEPALIVE_INTVL,
    TYPE,
    UNSUBSCRIBE,
    XPUB,
    XSUB,
    Frame,
    backend_name,
    version,
)
from .error import (
    Again,
    ContextTerminated,
    InterruptedSystemCall,
    ZMQBaseError,
    ZMQBindError,
    ZMQError,
    ZMQVersionError,
)
from .error import (
    NotImplementedError as ZMQNotImplementedError,
)

if TYPE_CHECKING:
    from .asyncio import Socket as AsyncSocket

# ── Constants ─────────────────────────────────────────────────────────

POLLIN: Final = 1
POLLOUT: Final = 2
POLLERR: Final = 4
POLLPRI: Final = 32
HWM: Final = 1

# Windows specific constants
_IS_WINDOWS: Final[bool] = sys.platform == "win32"
_WAKEUP_MODE_NONE: Final = 0
_WAKEUP_MODE_ASYNC: Final = 1
_WAKEUP_MODE_SYNC: Final = 2

ROUTING_ID: Final = 5
LAST_ENDPOINT: Final = 32
FD: Final = 14
EVENTS: Final = 15
MECHANISM: Final = 43
SNDBUF: Final = 11
RCVBUF: Final = 12
RATE: Final = 8
CONNECT_TIMEOUT: Final = 79
XPUB_VERBOSE: Final = 40
PROBE_ROUTER: Final = 51
REQ_CORRELATE: Final = 52
REQ_RELAXED: Final = 53
ROUTER_HANDOVER: Final = 56
IPV4ONLY: Final = 31
TCP_ACCEPT_FILTER: Final = 38
TCP_MAXRT: Final = 80
MULTICAST_HOPS: Final = 25
RECOVERY_IVL: Final = 9
RECONNECT_STOP: Final = 109
PLAIN_SERVER: Final = 44
PLAIN_USERNAME: Final = 45
PLAIN_PASSWORD: Final = 46
ZAP_DOMAIN: Final = 55

FORWARDER: Final = 2
QUEUE: Final = 3
STREAMER: Final = 1

NULL: Final = 0
PLAIN: Final = 1
CURVE: Final = 2

ETERM: Final = 156384765
ENOTSOCK: Final = 108
COPY_THRESHOLD: Final = 65536

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

from ._tracker import MessageTracker, NotDone
from ._typing import (
    AuthCallback,
    ConnectionInfo,
    CurveAuth,
    FutureResult,
    MonitorEvent,
    PlainAuth,
    Sendable,
    _BytesOption,
    _IntOption,
)

SENDABLE_TYPES = Sendable

# ── Top-level functions ──────────────────────────────────────────────


def strerror(errnum: int) -> str:
    return os.strerror(errnum)


def zmq_version() -> str:
    return "{:d}.{:d}.{:d}".format(*zmq_version_info)


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


Message = Frame


# ── Socket wrapper ───────────────────────────────────────────────────


# Socket option descriptor for IDE autocomplete support
class _SocketOptionDescriptor[T]:
    """Descriptor for socket options providing IDE autocomplete."""

    def __init__(self, option_code: int) -> None:
        self.option_code = option_code

    @overload
    def __get__(self, obj: None, objtype: type[object] | None = None) -> Self: ...

    @overload
    def __get__(
        self, obj: _SocketOptionsBase, objtype: type[object] | None = None
    ) -> T: ...

    def __get__(
        self, obj: _SocketOptionsBase | None, objtype: type[object] | None = None
    ) -> T | Self:
        if obj is None:
            return self
        if self.option_code == LAST_ENDPOINT:
            return cast(T, obj._last_endpoint)
        return cast(T, obj.getsockopt(self.option_code))

    def __set__(self, obj: _SocketOptionsBase, value: T) -> None:
        obj.setsockopt(self.option_code, cast(int | bytes, value))


class _SocketOptionsBase:
    """Base class with socket option descriptors and shared methods."""

    # Attributes (subclasses must define these)
    # Concrete subclasses initialize and narrow these private handles. Shared
    # methods never replace them with a different socket or context kind.
    _sock: _native.Socket | _native.AsyncSocket
    _context: Context

    _closed: bool
    _last_endpoint: bytes | None

    # Socket options
    affinity = _SocketOptionDescriptor[int](AFFINITY)
    identity = _SocketOptionDescriptor[bytes](IDENTITY)
    routing_id = _SocketOptionDescriptor[bytes](ROUTING_ID)
    rcvmore = _SocketOptionDescriptor[int](RCVMORE)
    sndhwm = _SocketOptionDescriptor[int](SNDHWM)
    rcvhwm = _SocketOptionDescriptor[int](RCVHWM)
    linger = _SocketOptionDescriptor[int](LINGER)
    reconnect_ivl = _SocketOptionDescriptor[int](RECONNECT_IVL)
    reconnect_ivl_max = _SocketOptionDescriptor[int](RECONNECT_IVL_MAX)
    backlog = _SocketOptionDescriptor[int](BACKLOG)
    maxmsgsize = _SocketOptionDescriptor[int](MAXMSGSIZE)
    rcvtimeo = _SocketOptionDescriptor[int](RCVTIMEO)
    sndtimeo = _SocketOptionDescriptor[int](SNDTIMEO)
    ipv6 = _SocketOptionDescriptor[int](IPV6)
    immediate = _SocketOptionDescriptor[int](IMMEDIATE)
    router_mandatory = _SocketOptionDescriptor[int](ROUTER_MANDATORY)
    tcp_keepalive = _SocketOptionDescriptor[int](TCP_KEEPALIVE)
    tcp_keepalive_idle = _SocketOptionDescriptor[int](TCP_KEEPALIVE_IDLE)
    tcp_keepalive_cnt = _SocketOptionDescriptor[int](TCP_KEEPALIVE_CNT)
    tcp_keepalive_intvl = _SocketOptionDescriptor[int](TCP_KEEPALIVE_INTVL)
    heartbeat_ivl = _SocketOptionDescriptor[int](HEARTBEAT_IVL)
    heartbeat_ttl = _SocketOptionDescriptor[int](HEARTBEAT_TTL)
    heartbeat_timeout = _SocketOptionDescriptor[int](HEARTBEAT_TIMEOUT)
    handshake_ivl = _SocketOptionDescriptor[int](HANDSHAKE_IVL)
    conflate = _SocketOptionDescriptor[int](CONFLATE)
    curve_server = _SocketOptionDescriptor[int](CURVE_SERVER)
    curve_publickey = _SocketOptionDescriptor[bytes](CURVE_PUBLICKEY)
    curve_secretkey = _SocketOptionDescriptor[bytes](CURVE_SECRETKEY)
    curve_serverkey = _SocketOptionDescriptor[bytes](CURVE_SERVERKEY)
    on_mute = _SocketOptionDescriptor[int](OMQ_ON_MUTE)
    compression_level = _SocketOptionDescriptor[int](OMQ_COMPRESSION_LEVEL)
    compression_dict = _SocketOptionDescriptor[bytes](OMQ_COMPRESSION_DICT)
    compression_auto_train = _SocketOptionDescriptor[int](OMQ_COMPRESSION_AUTO_TRAIN)
    sndbuf = _SocketOptionDescriptor[int](SNDBUF)
    rcvbuf = _SocketOptionDescriptor[int](RCVBUF)
    mechanism = _SocketOptionDescriptor[int](MECHANISM)
    plain_server = _SocketOptionDescriptor[int](PLAIN_SERVER)
    plain_username = _SocketOptionDescriptor[bytes](PLAIN_USERNAME)
    plain_password = _SocketOptionDescriptor[bytes](PLAIN_PASSWORD)

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
    def last_endpoint(self) -> bytes | None:
        return self._last_endpoint

    @property
    def underlying(self) -> Self:
        return self

    # ── Options ──────────────────────────────────────────────────────

    def setsockopt(self, option: int, value: int | bytes) -> None:
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    @overload
    def getsockopt(self, option: _BytesOption) -> bytes: ...

    @overload
    def getsockopt(self, option: Literal[32]) -> bytes | None: ...

    @overload
    def getsockopt(
        self,
        option: _IntOption,
    ) -> int: ...

    @overload
    def getsockopt(self, option: int) -> int | bytes | None: ...

    def getsockopt(self, option: int) -> int | bytes | None:
        if option == LAST_ENDPOINT:
            return self._last_endpoint
        try:
            return self._sock.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option: int, value: int | bytes) -> None:
        return self.setsockopt(option, value)

    @overload
    def get(self, option: _BytesOption) -> bytes: ...

    @overload
    def get(self, option: Literal[32]) -> bytes | None: ...

    @overload
    def get(
        self,
        option: _IntOption,
    ) -> int: ...

    @overload
    def get(self, option: int) -> int | bytes | None: ...

    def get(self, option: int) -> int | bytes | None:
        return self.getsockopt(option)

    def setsockopt_string(
        self, option: int, value: str, encoding: str = "utf-8"
    ) -> None:
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

    _closed: bool
    _pid: int
    _binds: list[str | bytes]
    _connects: list[str | bytes]

    def set_curve_auth(self, auth: CurveAuth) -> None:
        try:
            return self._sock.set_curve_auth(auth)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        except AttributeError:
            raise ZMQNotImplementedError("curve feature not compiled")

    def set_plain_auth(self, auth: PlainAuth) -> None:
        """Configure PLAIN server admission before first socket use.

        Pass an iterable of exact ``(username, password)`` pairs or a callable
        receiving ``PeerInfo``. Each string must contain at most 255 ASCII
        VCHAR bytes. An empty iterable rejects every client. PLAIN
        authenticates clients but does not encrypt traffic.
        """
        try:
            return self._sock.set_plain_auth(auth)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        except AttributeError:
            raise ZMQNotImplementedError("plain feature not compiled")

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

    def bind(self, endpoint: str | bytes) -> str | bytes:
        as_bytes = isinstance(endpoint, bytes)
        if isinstance(endpoint, bytes):
            endpoint = endpoint.decode("utf-8")
        try:
            ep = self._sock.bind(self._context._namespace_inproc(endpoint))
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            return ep.encode("utf-8") if as_bytes else ep
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
        if isinstance(endpoint, bytes):
            endpoint = endpoint.decode("utf-8")
        try:
            return self._sock.unbind(self._context._namespace_inproc(endpoint))
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def disconnect(self, endpoint: str | bytes) -> None:
        if isinstance(endpoint, bytes):
            endpoint = endpoint.decode("utf-8")
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

    def monitor(self) -> _native.Monitor:
        return self._sock.monitor()

    def connections(self) -> list[ConnectionInfo]:
        return self._sock.connections()

    def connection_info(self, connection_id: int) -> ConnectionInfo | None:
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

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool:
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

    def __init__(self, _sock: _native.Socket, _context: Context) -> None:
        self._sock = _sock  # pyright: ignore[reportIncompatibleVariableOverride]
        self._context = _context
        self._closed = False
        self._last_endpoint = None
        self._pid = os.getpid()
        self._binds = []
        self._connects = []

    def __class_getitem__(cls, item: Any) -> type[Socket]:
        return cls

    @classmethod
    @overload
    def shadow[S: Socket](cls, socket: S) -> S: ...

    @classmethod
    @overload
    def shadow(cls, socket: AsyncSocket) -> _ShadowSocket: ...

    @classmethod
    def shadow(cls, socket: Socket | AsyncSocket) -> Socket | _ShadowSocket:
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
        data: SENDABLE_TYPES,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        try:
            return self._sock.send(data, flags, copy, track)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    @overload
    def recv(
        self, flags: int = 0, copy: Literal[True] = True, track: bool = False
    ) -> bytes: ...

    @overload
    def recv(
        self, flags: int = 0, *, copy: Literal[False], track: bool = False
    ) -> Frame: ...

    @overload
    def recv(self, flags: int, copy: Literal[False], track: bool = False) -> Frame: ...

    @overload
    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame: ...

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame:
        try:
            if copy:
                return self._sock.recv(flags)
            frame = self._sock.recv_frame(flags)
            if track:
                frame._track_received()
            return frame
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def send_multipart(
        self,
        parts: Iterable[SENDABLE_TYPES],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        try:
            return self._sock.send_multipart(parts, flags, copy, track)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    @overload
    def recv_multipart(
        self, flags: int = 0, copy: Literal[True] = True, track: bool = False
    ) -> list[bytes]: ...

    @overload
    def recv_multipart(
        self, flags: int = 0, *, copy: Literal[False], track: bool = False
    ) -> list[Frame]: ...

    @overload
    def recv_multipart(
        self, flags: int, copy: Literal[False], track: bool = False
    ) -> list[Frame]: ...

    @overload
    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes] | list[Frame]: ...

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes] | list[Frame]:
        try:
            if copy:
                return self._sock.recv_multipart(flags)
            frames = self._sock.recv_multipart_frames(flags)
            if track:
                for frame in frames:
                    frame._track_received()
            return frames
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

    def send_serialized[T](
        self,
        msg: T,
        serialize: Callable[[T], Iterable[SENDABLE_TYPES]],
        flags: int = 0,
        copy: bool = True,
        **kwargs: Any,
    ) -> MessageTracker | None:
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    @overload
    def recv_serialized[T](
        self,
        deserialize: Callable[[list[bytes]], T],
        flags: int = 0,
        copy: Literal[True] = True,
    ) -> T: ...

    @overload
    def recv_serialized[T](
        self,
        deserialize: Callable[[list[Frame]], T],
        flags: int = 0,
        *,
        copy: Literal[False],
    ) -> T: ...

    @overload
    def recv_serialized[T](
        self, deserialize: Callable[[list[Frame]], T], flags: int, copy: Literal[False]
    ) -> T: ...

    @overload
    def recv_serialized[T](
        self,
        deserialize: Callable[[list[bytes] | list[Frame]], T],
        flags: int = 0,
        copy: bool = True,
    ) -> T: ...

    def recv_serialized[T](
        self, deserialize: Callable[[Any], T], flags: int = 0, copy: bool = True
    ) -> T:
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

    _async_socket: AsyncSocket
    _native: _native.AsyncSocket
    _context: Context
    _closed: bool
    _recv_waiter_pending: bool
    _send_waiter_pending: bool
    _recv_wakeup_event: threading.Event
    _send_wakeup_event: threading.Event

    def __init__(self, async_socket: AsyncSocket) -> None:
        self._async_socket = async_socket
        self._native = async_socket._sock
        self._context = async_socket._context
        self._closed = False
        self._last_endpoint = async_socket._last_endpoint
        if sys.platform == "win32":
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
    def underlying(self) -> Self:
        return self

    @overload
    def getsockopt(self, option: _BytesOption) -> bytes: ...

    @overload
    def getsockopt(self, option: Literal[32]) -> bytes | None: ...

    @overload
    def getsockopt(
        self,
        option: _IntOption,
    ) -> int: ...

    @overload
    def getsockopt(self, option: int) -> int | bytes | None: ...

    def getsockopt(self, option: int) -> int | bytes | None:
        try:
            return self._native.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def setsockopt(self, option: int, value: int | bytes) -> None:
        try:
            return self._native.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option: int, value: int | bytes) -> None:
        return self.setsockopt(option, value)

    @overload
    def get(self, option: _BytesOption) -> bytes: ...

    @overload
    def get(self, option: Literal[32]) -> bytes | None: ...

    @overload
    def get(
        self,
        option: _IntOption,
    ) -> int: ...

    @overload
    def get(self, option: int) -> int | bytes | None: ...

    def get(self, option: int) -> int | bytes | None:
        return self.getsockopt(option)

    if sys.platform == "win32":

        def _register_wakeup_hooks(self) -> None:
            self._async_socket._register_wakeup_hooks()
    else:

        def _register_wakeup_hooks(self) -> None:
            return None

    if sys.platform == "win32":

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

    @overload
    def recv(
        self, flags: int = 0, copy: Literal[True] = True, track: bool = False
    ) -> bytes: ...

    @overload
    def recv(
        self, flags: int = 0, *, copy: Literal[False], track: bool = False
    ) -> Frame: ...

    @overload
    def recv(self, flags: int, copy: Literal[False], track: bool = False) -> Frame: ...

    @overload
    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame: ...

    def recv(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> bytes | Frame:
        if copy:
            return self._blocking_recv(self._native._try_recv)
        frame = self._blocking_recv(self._native._try_recv_frame)
        if track:
            frame._track_received()
        return frame

    @overload
    def recv_multipart(
        self, flags: int = 0, copy: Literal[True] = True, track: bool = False
    ) -> list[bytes]: ...

    @overload
    def recv_multipart(
        self, flags: int = 0, *, copy: Literal[False], track: bool = False
    ) -> list[Frame]: ...

    @overload
    def recv_multipart(
        self, flags: int, copy: Literal[False], track: bool = False
    ) -> list[Frame]: ...

    @overload
    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes] | list[Frame]: ...

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[bytes] | list[Frame]:
        if copy:
            return self._blocking_recv(self._native._try_recv_multipart)
        frames = self._blocking_recv(self._native._try_recv_multipart_frames)
        if track:
            for frame in frames:
                frame._track_received()
        return frames

    def send(
        self,
        data: SENDABLE_TYPES,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        return self._blocking_send(lambda: self._native.send(data, flags, copy, track))

    def send_multipart(
        self,
        parts: Iterable[SENDABLE_TYPES],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None:
        return self._blocking_send(
            lambda: self._native.send_multipart(parts, flags, copy, track)
        )

    if sys.platform == "win32":

        def _blocking_send(
            self, send_fn: Callable[[], MessageTracker | None]
        ) -> MessageTracker | None:
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
                    return send_fn()
                except _native.ZMQError as e:
                    if getattr(e, "errno", None) != _errno.EAGAIN:
                        raise error.from_native(e) from None
                    if hasattr(e, "_pending_send"):
                        send_fn = e._pending_send.retry

                while True:
                    self._send_wakeup_event.clear()
                    try:
                        return send_fn()
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
                        if hasattr(e, "_pending_send"):
                            send_fn = e._pending_send.retry
                    self._send_wakeup_event.wait()
                    try:
                        return send_fn()
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
                        if hasattr(e, "_pending_send"):
                            send_fn = e._pending_send.retry
            finally:
                self._send_waiter_pending = False
                self._async_socket._clear_wakeup_modes(
                    send_mode=_WAKEUP_MODE_SYNC,
                )
    else:

        def _blocking_send(
            self, send_fn: Callable[[], MessageTracker | None]
        ) -> MessageTracker | None:
            try:
                return send_fn()
            except _native.ZMQError as e:
                if getattr(e, "errno", None) != _errno.EAGAIN:
                    raise error.from_native(e) from None
                if hasattr(e, "_pending_send"):
                    send_fn = e._pending_send.retry

            fd = self._native._send_fd()
            try:
                try:
                    return send_fn()
                except _native.ZMQError as e:
                    if getattr(e, "errno", None) != _errno.EAGAIN:
                        raise error.from_native(e) from None
                    if hasattr(e, "_pending_send"):
                        send_fn = e._pending_send.retry

                while True:
                    _select.select([fd], [], [])
                    try:
                        os.read(fd, 8)
                    except OSError:
                        pass
                    try:
                        return send_fn()
                    except _native.ZMQError as e:
                        if getattr(e, "errno", None) != _errno.EAGAIN:
                            raise error.from_native(e) from None
                        if hasattr(e, "_pending_send"):
                            send_fn = e._pending_send.retry
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

    def _namespace_inproc[T: (str, bytes)](self, endpoint: T) -> T:
        # `inproc://` names are scoped by the native context core. Keep the
        # user endpoint unchanged so LAST_ENDPOINT and errors match input.
        return endpoint

    def __class_getitem__(cls, item: Any) -> type[Context]:
        return cls

    @property
    def closed(self) -> bool:
        return self._closed

    @overload
    def socket(
        self, socket_type: int, socket_class: None = None, **kwargs: Any
    ) -> Socket: ...

    @overload
    def socket[S: Socket](
        self, socket_type: int, socket_class: type[S], **kwargs: Any
    ) -> S: ...

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
        return obj

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

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool:
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
        ready: dict[int, int] = {
            k: POLLOUT for k, (_, f) in self._sockets.items() if f & POLLOUT
        }
        pollin_socks = [s._sock for k, (s, f) in self._sockets.items() if f & POLLIN]
        if not pollin_socks:
            return [(s, ready[k]) for k, (s, _) in self._sockets.items() if k in ready]
        t = None if (timeout is None or timeout < 0) else int(timeout)
        if ready:
            t = 0
        ready_ids = _native.wait_any(pollin_socks, t)
        for rid in ready_ids:
            ready[rid] = ready.get(rid, 0) | POLLIN
        return [(s, ready[k]) for k, (s, _) in self._sockets.items() if k in ready]


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


from .zmqstream import ZMQStream

__all__ = [  # noqa: RUF022
    "AuthCallback",
    "CurveAuth",
    "PlainAuth",
    "FutureResult",
    "Sendable",
    "ConnectionInfo",
    "MonitorEvent",
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
_orig_zmqerror_init = cast(Callable[..., None], _native.ZMQError.__init__)


def _zmqerror_init(
    self: ZMQError, errno: int | str | None = None, msg: str | None = None
) -> None:
    if isinstance(errno, str):
        if msg is not None:
            raise TypeError("message specified twice")
        msg, errno = errno, None
    elif errno is not None and not isinstance(errno, int):
        raise TypeError("errno must be an integer or None")
    if msg is not None and not isinstance(msg, str):
        raise TypeError("msg must be a string or None")
    self.errno = errno
    self.strerror = (
        msg if msg is not None else (strerror(errno) if errno is not None else "")
    )
    _orig_zmqerror_init(self, self.strerror)


_native.ZMQError.__init__ = _zmqerror_init
