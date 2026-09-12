"""Shared public value types. No socket imports or runtime validation."""

from collections.abc import Buffer, Callable, Generator, Iterable
from typing import Any, Literal, NotRequired, Protocol, TypedDict

from ._native import Frame, PeerInfo

type Sendable = Buffer | Frame
type _BytesOption = Literal[5, 38, 45, 46, 48, 49, 50, 55, 1006]
type _IntOption = Literal[
    4,
    8,
    9,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    21,
    22,
    23,
    24,
    25,
    27,
    28,
    31,
    33,
    34,
    35,
    36,
    37,
    39,
    40,
    42,
    43,
    44,
    47,
    51,
    52,
    53,
    54,
    56,
    66,
    75,
    76,
    77,
    79,
    80,
    109,
    1004,
    1005,
    1007,
]
type Multipart = list[bytes] | list[Frame]
type AuthCallback = Callable[[PeerInfo], bool]
type CurveAuth = Iterable[bytes] | AuthCallback | None
type PlainAuth = Iterable[tuple[str, str]] | AuthCallback


class ConnectionInfo(TypedDict):
    connection_id: int
    endpoint: str
    identity: bytes


class ListeningEvent(TypedDict):
    event: Literal["listening"]
    endpoint: str


class ConnectionEvent(TypedDict):
    event: Literal["accepted", "connected", "disconnected", "peer_command"]
    endpoint: str
    connection_id: int


class HandshakeEvent(TypedDict):
    event: Literal["handshake_succeeded"]
    endpoint: str
    connection_id: int
    peer_identity: NotRequired[bytes]


class HandshakeFailedEvent(TypedDict):
    event: Literal["handshake_failed"]
    endpoint: str
    reason: str


class ConnectDelayedEvent(TypedDict):
    event: Literal["connect_delayed"]
    endpoint: str
    attempt: int


class LaggedEvent(TypedDict):
    event: Literal["lagged"]
    count: int


class TerminalEvent(TypedDict):
    event: Literal["closed", "unknown"]


type MonitorEvent = (
    ListeningEvent
    | ConnectionEvent
    | HandshakeEvent
    | HandshakeFailedEvent
    | ConnectDelayedEvent
    | LaggedEvent
    | TerminalEvent
)


class FutureResult[T](Protocol):
    """Common interface of native-ready results and asyncio futures."""

    def __await__(self) -> Generator[Any, None, T]: ...
    def result(self) -> T: ...
    def done(self) -> bool: ...
