"""pyomq - Python binding for omq.rs.

Drop-in pyzmq replacement on the common path. Use as::

    import pyomq as zmq

The Socket / Context API mirrors pyzmq's surface; constants
(``zmq.PUSH``, ``zmq.SUBSCRIBE``, ``zmq.LINGER`` ...) match libzmq's
integer values, so existing pyzmq code typically just works.

For asynchronous code::

    import pyomq.asyncio as zmq_async
"""

import errno as _errno
import itertools
import json
import os
import pickle
import select as _select
import sys
import threading
import weakref

from . import _native  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]
from . import error as error  # noqa: F401

from ._native import (  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]
    backend_name,
    version,
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

POLLIN = 1
POLLOUT = 2
POLLERR = 4
POLLPRI = 32
HWM = 1
_WAKEUP_MODE_NONE = 0
_WAKEUP_MODE_ASYNC = 1
_WAKEUP_MODE_SYNC = 2
_IS_WINDOWS = sys.platform == "win32"

ROUTING_ID = 5
LAST_ENDPOINT = 32
FD = 14
EVENTS = 15
MECHANISM = 43
SNDBUF = 11
RCVBUF = 12
RATE = 8
CONNECT_TIMEOUT = 79
XPUB_VERBOSE = 40
PROBE_ROUTER = 51
REQ_CORRELATE = 52
REQ_RELAXED = 53
ROUTER_HANDOVER = 56
IPV4ONLY = 31
TCP_ACCEPT_FILTER = 38
TCP_MAXRT = 80
MULTICAST_HOPS = 25
RECOVERY_IVL = 9
RECONNECT_STOP = 109
PLAIN_SERVER = 44
PLAIN_USERNAME = 45
PLAIN_PASSWORD = 46
ZAP_DOMAIN = 55

FORWARDER = 2
QUEUE = 3
STREAMER = 1

NULL = 0
PLAIN = 1
CURVE = 2

ETERM = 156384765
ENOTSOCK = 108
COPY_THRESHOLD = 65536

# errno constants (pyzmq exposes these at top level)
EAGAIN = _errno.EAGAIN
ENOTSUP = _errno.ENOTSUP
EINVAL = _errno.EINVAL
EFAULT = _errno.EFAULT
ENOMEM = _errno.ENOMEM
ENODEV = _errno.ENODEV
EMSGSIZE = _errno.EMSGSIZE
EAFNOSUPPORT = _errno.EAFNOSUPPORT
ENETUNREACH = _errno.ENETUNREACH
ECONNABORTED = _errno.ECONNABORTED
ECONNRESET = _errno.ECONNRESET
ENOTCONN = _errno.ENOTCONN
ETIMEDOUT = _errno.ETIMEDOUT
EHOSTUNREACH = _errno.EHOSTUNREACH
ENETRESET = _errno.ENETRESET
EADDRINUSE = _errno.EADDRINUSE
EADDRNOTAVAIL = _errno.EADDRNOTAVAIL

__version__ = version()
zmq_version_info = (4, 3, 4)


# ── Top-level functions ──────────────────────────────────────────────


def strerror(errnum):
    return os.strerror(errnum)


def zmq_version():
    return "%d.%d.%d" % zmq_version_info


def pyomq_version():
    return __version__


def pyomq_version_info():
    parts = __version__.split(".")
    return tuple(int(p) for p in parts[:3])


def has(capability):
    cap = capability.lower()
    if cap in ("ipc", "inproc"):
        return True
    if hasattr(_native, "has_feature"):
        return _native.has_feature(cap)
    return False


def curve_keypair():
    if not hasattr(_native, "curve_keypair"):
        raise ZMQNotImplementedError("curve feature not compiled")
    return _native.curve_keypair()


def curve_public(secret):
    if not hasattr(_native, "curve_public"):
        raise ZMQNotImplementedError("curve feature not compiled")
    if isinstance(secret, str):
        secret = secret.encode("ascii")
    return _native.curve_public(secret)


if hasattr(_native, "PeerInfo"):
    PeerInfo = _native.PeerInfo


# ── Socket option attribute map ──────────────────────────────────────

_TYPE_NAMES = {
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

_SOCKOPT_NAMES = {
    "affinity": AFFINITY,
    "identity": IDENTITY,
    "routing_id": ROUTING_ID,
    "subscribe": SUBSCRIBE,
    "unsubscribe": UNSUBSCRIBE,
    "rcvmore": RCVMORE,
    "sndhwm": SNDHWM,
    "rcvhwm": RCVHWM,
    "linger": LINGER,
    "reconnect_ivl": RECONNECT_IVL,
    "reconnect_ivl_max": RECONNECT_IVL_MAX,
    "backlog": BACKLOG,
    "maxmsgsize": MAXMSGSIZE,
    "rcvtimeo": RCVTIMEO,
    "sndtimeo": SNDTIMEO,
    "ipv6": IPV6,
    "immediate": IMMEDIATE,
    "router_mandatory": ROUTER_MANDATORY,
    "tcp_keepalive": TCP_KEEPALIVE,
    "tcp_keepalive_idle": TCP_KEEPALIVE_IDLE,
    "tcp_keepalive_cnt": TCP_KEEPALIVE_CNT,
    "tcp_keepalive_intvl": TCP_KEEPALIVE_INTVL,
    "heartbeat_ivl": HEARTBEAT_IVL,
    "heartbeat_ttl": HEARTBEAT_TTL,
    "heartbeat_timeout": HEARTBEAT_TIMEOUT,
    "handshake_ivl": HANDSHAKE_IVL,
    "conflate": CONFLATE,
    "curve_server": CURVE_SERVER,
    "curve_publickey": CURVE_PUBLICKEY,
    "curve_secretkey": CURVE_SECRETKEY,
    "curve_serverkey": CURVE_SERVERKEY,
    "on_mute": OMQ_ON_MUTE,
    "compression_dict": OMQ_COMPRESSION_DICT,
    "compression_auto_train": OMQ_COMPRESSION_AUTO_TRAIN,
    "sndbuf": SNDBUF,
    "rcvbuf": RCVBUF,
    "mechanism": MECHANISM,
    "plain_server": PLAIN_SERVER,
    "plain_username": PLAIN_USERNAME,
    "plain_password": PLAIN_PASSWORD,
}


# ── MessageTracker / Message / Frame (pyzmq compat) ─────────────────


class NotDone(ZMQBaseError):
    pass


class MessageTracker:
    def __init__(self, *args, _pending=False, **kwargs):
        self.done = not _pending

    def wait(self, timeout=None):
        if not self.done:
            raise NotDone


class Message(bytes):
    tracker = None

    def __new__(cls, data=b"", track=False):
        return super().__new__(cls, data)

    @property
    def bytes(self):
        return bytes(self)

    @property
    def buffer(self):
        return memoryview(bytes(self))


Frame = Message


# ── Socket wrapper ───────────────────────────────────────────────────


class _SocketMeta(type):
    def __instancecheck__(cls, instance):
        if type.__instancecheck__(cls, instance):
            return True
        if cls is Socket:
            amod = sys.modules.get("pyomq.asyncio")
            if amod is not None and type.__instancecheck__(amod.Socket, instance):
                return True
        return False


class Socket(metaclass=_SocketMeta):
    _sock: _native.Socket
    _context: "Context"
    _closed: bool

    def __init__(self, _sock, _context):
        self._sock = _sock
        self._context = _context
        self._closed = False
        self._last_endpoint = None
        self._pid = os.getpid()
        self._binds = []
        self._connects = []

    def __class_getitem__(cls, item):
        return cls

    @classmethod
    def shadow(cls, socket):
        from . import asyncio as _zmq_async

        if isinstance(socket, _zmq_async.Socket):
            return _ShadowSocket(socket)
        return socket

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
        return f"<pyomq.Socket(pyomq.{st}) at {id(self):#x}>"

    @property
    def closed(self):
        return self._closed

    @property
    def context(self):
        return self._context

    @property
    def socket_type(self):
        return self._sock.getsockopt(TYPE)

    @property
    def underlying(self):
        return self

    # ── I/O ──────────────────────────────────────────────────────────

    def fileno(self):
        return self.getsockopt(FD)

    @property
    def last_endpoint(self):
        return self._last_endpoint

    def _check_fork(self):
        pid = os.getpid()
        if pid == self._pid:
            return
        self._pid = pid
        for ep in self._binds:
            try:
                self._sock.bind(self._context._namespace_inproc(ep))
            except _native.ZMQError:
                pass
        for ep in self._connects:
            try:
                self._sock.connect(self._context._namespace_inproc(ep))
            except _native.ZMQError:
                pass

    def bind(self, endpoint):
        try:
            ep = self._sock.bind(self._context._namespace_inproc(endpoint))
            self._last_endpoint = ep.encode() if isinstance(ep, str) else ep
            self._binds.append(endpoint)
            return ep
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def connect(self, endpoint):
        if isinstance(endpoint, bytes):
            endpoint = endpoint.decode("utf-8")
        try:
            self._sock.connect(self._context._namespace_inproc(endpoint))
            self._last_endpoint = (
                endpoint.encode() if isinstance(endpoint, str) else endpoint
            )
            self._connects.append(endpoint)
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
            raise error.from_native(e) from None
        if track:
            return MessageTracker(_pending=True)

    def recv(self, flags=0, copy=True, track=False):
        try:
            data = self._sock.recv(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        if not copy:
            return Frame(data)
        return data

    def send_multipart(self, parts, flags=0, copy=True, track=False):
        parts = [p.encode("utf-8") if isinstance(p, str) else p for p in parts]
        try:
            self._sock.send_multipart(parts, flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        if track:
            return MessageTracker(_pending=True)

    def recv_multipart(self, flags=0, copy=True, track=False):
        try:
            parts = self._sock.recv_multipart(flags)
        except _native.ZMQError as e:
            raise error.from_native(e) from None
        if not copy:
            return [Frame(p) for p in parts]
        return parts

    # ── Serialization helpers ────────────────────────────────────────

    def send_string(self, u, flags=0, encoding="utf-8"):
        return self.send(u.encode(encoding), flags)

    def recv_string(self, flags=0, encoding="utf-8"):
        return self.recv(flags).decode(encoding)

    def send_json(self, obj, flags=0, **kwargs):
        return self.send(json.dumps(obj, **kwargs).encode("utf-8"), flags)

    def recv_json(self, flags=0, **kwargs):
        return json.loads(self.recv(flags), **kwargs)

    def send_pyobj(self, obj, flags=0, protocol=-1):
        return self.send(pickle.dumps(obj, protocol), flags)

    def recv_pyobj(self, flags=0):
        return pickle.loads(self.recv(flags))

    def send_serialized(self, msg, serialize, flags=0, copy=True, **kwargs):
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    def recv_serialized(self, deserialize, flags=0, copy=True):
        frames = self.recv_multipart(flags=flags, copy=copy)
        return deserialize(frames)

    # ── Options ──────────────────────────────────────────────────────

    def setsockopt(self, option, value):
        try:
            return self._sock.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def getsockopt(self, option):
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

    # ── Monitoring ───────────────────────────────────────────────────

    def monitor(self):
        return self._sock.monitor()

    def connections(self):
        return self._sock.connections()

    def connection_info(self, connection_id):
        return self._sock.connection_info(connection_id)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self, linger=None):
        if not self._closed:
            self._closed = True
            self._sock.close(linger)

    def __del__(self):
        self.close()

    def bind_to_random_port(self, addr, min_port=49152, max_port=65536, max_tries=100):
        ep = self.bind(f"{addr}:0")
        if isinstance(ep, bytes):
            ep = ep.decode()
        return int(ep.rsplit(":", 1)[1])

    def poll(self, timeout=None, flags=POLLIN):
        p = Poller()
        p.register(self, flags)
        evts = p.poll(timeout)
        for sock, mask in evts:
            if sock is self:
                return mask
        return 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


# ── Shadow socket (sync recv bridge over async handle) ──────────────


class _ShadowSocket:
    """Blocking recv bridge over an async socket's native handle.

    Returned by Socket.shadow() when given a pyomq.asyncio.Socket.
    Provides sync recv via the native readiness signal without entering the
    asyncio event loop, matching pyzmq's shadow(underlying) behavior.

    """

    def __init__(self, async_socket):
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

    @property
    def closed(self):
        return self._closed or self._async_socket._closed

    @property
    def context(self):
        return self._context

    @property
    def socket_type(self):
        return self._native.getsockopt(TYPE)

    @property
    def underlying(self):
        return self

    def getsockopt(self, option):
        try:
            return self._native.getsockopt(option)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def setsockopt(self, option, value):
        try:
            return self._native.setsockopt(option, value)
        except _native.ZMQError as e:
            raise error.from_native(e) from None

    def set(self, option, value):
        return self.setsockopt(option, value)

    def get(self, option):
        return self.getsockopt(option)

    if _IS_WINDOWS:

        def _register_wakeup_hooks(self):
            self._async_socket._register_wakeup_hooks()
    else:

        def _register_wakeup_hooks(self):
            return None

    if _IS_WINDOWS:

        def _blocking_recv(self, try_fn):
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

        def _blocking_recv(self, try_fn):
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

    def recv(self, flags=0, copy=True, track=False):
        data = self._blocking_recv(self._native._try_recv)
        if not copy:
            return Frame(data)
        return data

    def recv_multipart(self, flags=0, copy=True, track=False):
        parts = self._blocking_recv(self._native._try_recv_multipart)
        if not copy:
            return [Frame(p) for p in parts]
        return parts

    def send(self, data, flags=0, copy=True, track=False):
        self._blocking_send(lambda: self._native.send(data, flags))
        if track:
            return MessageTracker(_pending=True)

    def send_multipart(self, parts, flags=0, copy=True, track=False):
        self._blocking_send(lambda: self._native.send_multipart(parts, flags))
        if track:
            return MessageTracker(_pending=True)

    if _IS_WINDOWS:

        def _blocking_send(self, send_fn):
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

        def _blocking_send(self, send_fn):
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

    def close(self, linger=None):
        pass


# ── Context wrapper ──────────────────────────────────────────────────

_next_ctx_id = itertools.count(1)

_INPROC_PREFIX = "inproc://"


class Context:
    _instance = None
    _instance_lock = threading.Lock()
    _socket_class = None  # set after Socket is defined

    def __init__(self, io_threads=1, *, _shadow_ctx=None):
        if _shadow_ctx is not None:
            self._ctx = _shadow_ctx._ctx
            self._is_shadow = True
        else:
            self._ctx = _native.Context(io_threads)
            self._is_shadow = False
        self._closed = False
        self._sockets = weakref.WeakSet()
        self._ctx_id = next(_next_ctx_id)

    def _namespace_inproc(self, endpoint):
        # libzmq scopes inproc per-context; omq's registry is global. pytest
        # holds frame references to locals (traceback capture), so __del__
        # never fires and the old socket's registry entry stays alive. Next
        # test's new Context tries bind("inproc://test") and gets "already
        # bound". The entry is cleaned up eventually (Socket.close() ->
        # SocketCommand::Close -> InprocListener::drop removes it), but not
        # before the next test's bind(). Prefixing with context ID gives each
        # Context its own namespace, matching libzmq's per-context scoping.
        ns_str = f"pyomq-ctx-{self._ctx_id}/"
        ns_bytes = ns_str.encode()
        if isinstance(endpoint, bytes):
            pfx = b"inproc://"
            if endpoint.startswith(pfx) and not endpoint[len(pfx) :].startswith(
                ns_bytes
            ):
                return pfx + ns_bytes + endpoint[len(pfx) :]
        elif isinstance(endpoint, str):
            if endpoint.startswith(_INPROC_PREFIX) and not endpoint[
                len(_INPROC_PREFIX) :
            ].startswith(ns_str):
                return f"inproc://{ns_str}{endpoint[len(_INPROC_PREFIX) :]}"
        return endpoint

    def __class_getitem__(cls, item):
        return cls

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
        self._sockets.add(s)
        return s

    @classmethod
    def shadow(cls, address):
        if isinstance(address, Context):
            return cls(_shadow_ctx=address)
        if isinstance(address, int):
            return cls(_shadow_ctx=Context.instance())
        raise TypeError(f"expected Context or int, got {type(address).__name__}")

    @classmethod
    def instance(cls, io_threads=1):
        with cls._instance_lock:
            if cls._instance is None or cls._instance._closed:
                cls._instance = cls(io_threads)
            return cls._instance

    def term(self):
        self._closed = True
        for s in list(self._sockets):
            if not s.closed:
                s.close()
        self._sockets.clear()
        if not self._is_shadow:
            self._ctx.term()

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


# ── Poller ───────────────────────────────────────────────────────────


class Poller:
    def __init__(self):
        self._sockets = {}  # native_socket_id -> (Socket, flags)

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

    def poll(self, timeout=None):
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


def select(rlist, wlist, xlist, timeout=None):
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
    rready, wready, xready = [], [], []
    for sock, mask in evts:
        if mask & POLLIN:
            rready.append(sock)
        if mask & POLLOUT:
            wready.append(sock)
    return rready, wready, xready


# ── proxy ────────────────────────────────────────────────────────────


def proxy(frontend, backend, capture=None):
    _native.native_proxy(
        frontend._sock,
        backend._sock,
        capture._sock if capture is not None else None,
    )


def proxy_steerable(frontend, backend, capture=None, control=None):
    _native.native_proxy(
        frontend._sock,
        backend._sock,
        capture._sock if capture is not None else None,
        control._sock if control is not None else None,
    )


def device(device_type, frontend, backend):
    proxy(frontend, backend)


# ── builtins reference (for copy/track errors) ──────────────────────

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
