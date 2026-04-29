"""pyomq - Python binding for omq.rs.

Drop-in pyzmq replacement on the common path. Use as::

    import pyomq as zmq

The Socket / Context API mirrors pyzmq's surface; constants
(``zmq.PUSH``, ``zmq.SUBSCRIBE``, ``zmq.LINGER`` ...) match libzmq's
integer values, so existing pyzmq code typically just works.

For asynchronous code::

    import pyomq.asyncio as zmq_async
"""

from ._native import (  # type: ignore[attr-defined]
    Context,
    Socket,
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
    # CURVE option ids (the option works only when the wheel was built
    # with the `curve` feature; setsockopt raises NotImplementedError
    # otherwise).
    CURVE_SERVER,
    CURVE_PUBLICKEY,
    CURVE_SECRETKEY,
    CURVE_SERVERKEY,
)

from .error import (  # noqa: F401  re-exports
    ZMQError,
    Again,
    ContextTerminated,
    NotImplementedError as ZMQNotImplementedError,
)

# pyzmq ships these on the top-level zmq module; mirror them so
# `zmq.error.ZMQError` style imports work.
from . import error as error  # noqa: F401

__all__ = [
    "Context",
    "Socket",
    "ZMQError",
    "Again",
    "ContextTerminated",
    "backend_name",
    "version",
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
    "CURVE_SERVER",
    "CURVE_PUBLICKEY",
    "CURVE_SECRETKEY",
    "CURVE_SERVERKEY",
]
