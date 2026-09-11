"""Interface of the PyO3 library.

This file is deliberately maintained by hand because PyO3's generated
stubs are intentionally limited and do not cover the full public
Python/native API contract of pyomq.

"""

import builtins
import sys
import types
from collections.abc import Buffer, Callable, Iterable, Sequence
from threading import Event
from typing import Final, Literal, Self, final, overload

from ._tracker import MessageTracker
from ._typing import (
    ConnectionInfo,
    CurveAuth,
    MonitorEvent,
    PlainAuth,
    Sendable,
    _BytesOption,
    _IntOption,
)

class ZMQBaseError(Exception): ...

class ZMQError(ZMQBaseError):
    errno: int | None
    strerror: str
    _pending_send: PendingSend
    @overload
    def __init__(self, errno: int | None = None, msg: str | None = None) -> None: ...
    @overload
    def __init__(self, msg: str, /) -> None: ...

# Socket type constants (libzmq-compatible)
PAIR: Final = 0
PUB: Final = 1
SUB: Final = 2
REQ: Final = 3
REP: Final = 4
DEALER: Final = 5
ROUTER: Final = 6
PULL: Final = 7
PUSH: Final = 8
XPUB: Final = 9
XSUB: Final = 10
STREAM: Final = 11
SERVER: Final = 12
CLIENT: Final = 13
RADIO: Final = 14
DISH: Final = 15
GATHER: Final = 16
SCATTER: Final = 17
PEER: Final = 19
CHANNEL: Final = 20

# Socket option constants
AFFINITY: Final = 4
IDENTITY: Final = 5
SUBSCRIBE: Final = 6
UNSUBSCRIBE: Final = 7
RCVMORE: Final = 13
TYPE: Final = 16
LINGER: Final = 17
RECONNECT_IVL: Final = 18
BACKLOG: Final = 19
RECONNECT_IVL_MAX: Final = 21
MAXMSGSIZE: Final = 22
SNDHWM: Final = 23
RCVHWM: Final = 24
RCVTIMEO: Final = 27
SNDTIMEO: Final = 28
ROUTER_MANDATORY: Final = 33
TCP_KEEPALIVE: Final = 34
TCP_KEEPALIVE_CNT: Final = 35
TCP_KEEPALIVE_IDLE: Final = 36
TCP_KEEPALIVE_INTVL: Final = 37
IMMEDIATE: Final = 39
IPV6: Final = 42
HEARTBEAT_IVL: Final = 75
HEARTBEAT_TTL: Final = 76
HEARTBEAT_TIMEOUT: Final = 77
HANDSHAKE_IVL: Final = 66
CONFLATE: Final = 54
CURVE_SERVER: Final = 47
CURVE_PUBLICKEY: Final = 48
CURVE_SECRETKEY: Final = 49
CURVE_SERVERKEY: Final = 50
OMQ_ON_MUTE: Final = 1004
OMQ_ON_MUTE_BLOCK: Final = 0
OMQ_ON_MUTE_DROP_NEWEST: Final = 1
OMQ_ON_MUTE_DROP_OLDEST: Final = 2
OMQ_COMPRESSION_LEVEL: Final = 1005
OMQ_COMPRESSION_DICT: Final = 1006
OMQ_COMPRESSION_AUTO_TRAIN: Final = 1007

# Compatibility constants
NOBLOCK: Final = 1
DONTWAIT: Final = 1
SNDMORE: Final = 2

# In-process connection metadata
@final
class PeerInfo:
    @property
    def public_key(self) -> bytes: ...
    @property
    def identity(self) -> bytes | None: ...
    @property
    def peer_address(self) -> str | None: ...
    @property
    def username(self) -> str | None: ...
    @property
    def password(self) -> str | None: ...

# Module-level helpers

def backend_name() -> str: ...
def version() -> str: ...
def has_feature(name: str) -> bool: ...
def wait_any(
    sockets: Sequence[Socket | AsyncSocket], timeout_ms: int | None = ...
) -> list[int]: ...
def rust_thread_send_via_share_key(
    share_key: int, endpoint: str, payload: bytes
) -> None: ...
def native_proxy(
    frontend: Socket,
    backend: Socket,
    capture: Socket | None = ...,
    control: Socket | None = ...,
) -> None: ...
def curve_keypair() -> tuple[bytes, bytes]: ...
def curve_public(secret_z85: bytes) -> bytes: ...

@final
class ReleaseToken:
    @property
    def done(self) -> bool: ...
    def wait(self, timeout: float | None = None) -> bool: ...

@final
class PendingSend:
    def retry(self) -> MessageTracker | None: ...
    def cancel(self) -> None: ...

@final
class Context:
    def __new__(cls, io_threads: int = 1) -> Self: ...
    @staticmethod
    def shadow_async(context: AsyncContext) -> Context: ...
    def share_key(self) -> int: ...
    @staticmethod
    def from_share_key(share_key: int) -> Context: ...
    def socket(self, socket_type: int, /) -> Socket: ...
    def term(self) -> None: ...
    def destroy(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

@final
class AsyncContext:
    def __new__(cls, io_threads: int = 1) -> Self: ...
    @staticmethod
    def shadow_sync(context: Context) -> AsyncContext: ...
    def share_key(self) -> int: ...
    @staticmethod
    def from_share_key(share_key: int) -> AsyncContext: ...
    def socket(self, socket_type: int, /) -> AsyncSocket: ...
    def term(self) -> None: ...
    def destroy(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

@final
class Frame(Buffer):
    def __new__(
        cls,
        data: Sendable | None = None,
        track: bool = False,
        copy: bool | None = ...,
        copy_threshold: int | None = ...,
    ) -> Self: ...
    @property
    def bytes(self) -> builtins.bytes: ...
    @property
    def buffer(self) -> memoryview: ...
    @property
    def more(self) -> bool: ...
    @property
    def routing_id(self) -> int: ...
    @routing_id.setter
    def routing_id(self, routing_id: int) -> None: ...
    @property
    def tracker(self) -> MessageTracker | None: ...
    def _track_received(self) -> None: ...
    def __buffer__(self, flags: int, /) -> memoryview: ...
    def __bytes__(self) -> builtins.bytes: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __eq__(self, other: object, /) -> bool: ...
    def __ne__(self, other: object, /) -> bool: ...

@final
class Monitor:
    def recv(self, timeout_ms: int = ...) -> MonitorEvent: ...
    def recv_nowait(self) -> MonitorEvent: ...

@final
class Socket:
    def socket_id(self) -> int: ...
    def bind(self, endpoint: str) -> str: ...
    def connect(self, endpoint: str) -> None: ...
    def unbind(self, endpoint: str) -> None: ...
    def disconnect(self, endpoint: str) -> None: ...
    def send(
        self,
        payload: Sendable,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None: ...
    def send_multipart(
        self,
        parts: Iterable[Sendable],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None: ...
    def recv(self, flags: int = 0) -> bytes: ...
    def recv_frame(self, flags: int = 0) -> Frame: ...
    def recv_multipart(self, flags: int = 0) -> list[bytes]: ...
    def recv_multipart_frames(self, flags: int = 0) -> list[Frame]: ...
    def subscribe(self, prefix: bytes) -> None: ...
    def unsubscribe(self, prefix: bytes) -> None: ...
    def join(self, group: bytes) -> None: ...
    def leave(self, group: bytes) -> None: ...
    def connections(self) -> list[ConnectionInfo]: ...
    def connection_info(self, connection_id: int) -> ConnectionInfo | None: ...
    def monitor(self) -> Monitor: ...
    def setsockopt(self, option: int, value: int | bytes) -> None: ...
    @overload
    def getsockopt(self, option: _BytesOption | Literal[32]) -> bytes: ...
    @overload
    def getsockopt(
        self,
        option: _IntOption,
    ) -> int: ...
    @overload
    def getsockopt(self, option: int) -> int | bytes: ...
    def set_curve_auth(self, auth: CurveAuth) -> None: ...
    def set_plain_auth(self, auth: PlainAuth) -> None: ...
    def close(self, linger: int | None = None) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

@final
class AsyncSocket:
    def socket_id(self) -> int: ...
    def _try_recv(self) -> bytes | None: ...
    def _try_recv_frame(self) -> Frame | None: ...
    def _try_recv_multipart(self) -> list[bytes] | None: ...
    def _try_recv_multipart_frames(self) -> list[Frame] | None: ...
    def _recv_fd(self) -> int: ...
    def _send_fd(self) -> int: ...
    if sys.platform == "win32":
        def _set_wakeup_hooks(
            self,
            recv_async: Callable[[], object] | None = ...,
            recv_event: Event | None = ...,
            send_async: Callable[[], object] | None = ...,
            send_event: Event | None = ...,
        ) -> None: ...
        def _set_wakeup_modes(
            self,
            recv_mode: int | None = ...,
            send_mode: int | None = ...,
        ) -> None: ...
        def _clear_wakeup_modes(
            self,
            recv_mode: int | None = ...,
            send_mode: int | None = ...,
        ) -> None: ...
        def _mark_recv_drain_complete(self) -> None: ...
        def _mark_send_drain_complete(self) -> None: ...
    def bind(self, endpoint: str) -> str: ...
    def connect(self, endpoint: str) -> None: ...
    def unbind(self, endpoint: str) -> None: ...
    def disconnect(self, endpoint: str) -> None: ...
    def send(
        self,
        payload: Sendable,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None: ...
    def send_multipart(
        self,
        parts: Iterable[Sendable],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> MessageTracker | None: ...
    def subscribe(self, prefix: bytes) -> None: ...
    def unsubscribe(self, prefix: bytes) -> None: ...
    def join(self, group: bytes) -> None: ...
    def leave(self, group: bytes) -> None: ...
    def connections(self) -> list[ConnectionInfo]: ...
    def connection_info(self, connection_id: int) -> ConnectionInfo | None: ...
    def monitor(self) -> Monitor: ...
    def setsockopt(self, option: int, value: int | bytes) -> None: ...
    @overload
    def getsockopt(self, option: _BytesOption | Literal[32]) -> bytes: ...
    @overload
    def getsockopt(
        self,
        option: _IntOption,
    ) -> int: ...
    @overload
    def getsockopt(self, option: int) -> int | bytes: ...
    def set_curve_auth(self, auth: CurveAuth) -> None: ...
    def set_plain_auth(self, auth: PlainAuth) -> None: ...
    def close(self, linger: int | None = None) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...
    def __aenter__(self) -> Self: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None = ...,
        exc_val: BaseException | None = ...,
        exc_tb: types.TracebackType | None = ...,
    ) -> bool: ...

__all__ = [
    "AFFINITY",
    "BACKLOG",
    "CHANNEL",
    "CLIENT",
    "CONFLATE",
    "CURVE_PUBLICKEY",
    "CURVE_SECRETKEY",
    "CURVE_SERVER",
    "CURVE_SERVERKEY",
    "DEALER",
    "DISH",
    "DONTWAIT",
    "GATHER",
    "HANDSHAKE_IVL",
    "HEARTBEAT_IVL",
    "HEARTBEAT_TIMEOUT",
    "HEARTBEAT_TTL",
    "IDENTITY",
    "IMMEDIATE",
    "IPV6",
    "LINGER",
    "MAXMSGSIZE",
    "NOBLOCK",
    "OMQ_COMPRESSION_AUTO_TRAIN",
    "OMQ_COMPRESSION_DICT",
    "OMQ_COMPRESSION_LEVEL",
    "OMQ_ON_MUTE",
    "OMQ_ON_MUTE_BLOCK",
    "OMQ_ON_MUTE_DROP_NEWEST",
    "OMQ_ON_MUTE_DROP_OLDEST",
    "PAIR",
    "PEER",
    "PUB",
    "PULL",
    "PUSH",
    "RADIO",
    "RCVHWM",
    "RCVMORE",
    "RCVTIMEO",
    "RECONNECT_IVL",
    "RECONNECT_IVL_MAX",
    "REP",
    "REQ",
    "ROUTER",
    "ROUTER_MANDATORY",
    "SCATTER",
    "SERVER",
    "SNDHWM",
    "SNDMORE",
    "SNDTIMEO",
    "STREAM",
    "SUB",
    "SUBSCRIBE",
    "TCP_KEEPALIVE",
    "TCP_KEEPALIVE_CNT",
    "TCP_KEEPALIVE_IDLE",
    "TCP_KEEPALIVE_INTVL",
    "TYPE",
    "UNSUBSCRIBE",
    "XPUB",
    "XSUB",
    "AsyncContext",
    "AsyncSocket",
    "Context",
    "Frame",
    "Monitor",
    "PeerInfo",
    "PendingSend",
    "ReleaseToken",
    "Socket",
    "ZMQBaseError",
    "ZMQError",
    "backend_name",
    "curve_keypair",
    "curve_public",
    "has_feature",
    "native_proxy",
    "rust_thread_send_via_share_key",
    "version",
    "wait_any",
]
