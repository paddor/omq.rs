"""Context registry behind ``pyomq.auth``.

pyzmq authenticators answer ZAP requests (RFC 27) for every socket of a
context. pyomq has no ZAP socket. Instead, a socket created while an
authenticator runs, or first used by ``bind()``, ``connect()`` or
``monitor()``, gets a native callback that asks the authenticator running
for its context at handshake time. Socket options are fixed at first use,
so the callback must be in place before it.
"""

from __future__ import annotations

import errno
import os
import sys
import threading
import weakref
from collections.abc import Callable
from typing import Any, Protocol

from ._native import PeerInfo, ZMQError


class Policy(Protocol):
    def _zap_admit(self, mechanism: str, domain: bytes, peer: PeerInfo) -> bool: ...


_lock = threading.Lock()
# Running authenticators, keyed by the native context core.
_active: dict[int, Policy] = {}
# Sockets an authenticator should cover, seen without one running.
_unguarded: dict[int, weakref.WeakSet[Any]] = {}


def _reset_after_fork() -> None:
    global _lock
    _lock = threading.Lock()


if sys.platform != "win32":
    os.register_at_fork(after_in_child=_reset_after_fork)


def context_key(context: Any) -> int:
    """Key shared by every wrapper and shadow of one native context."""
    return int(context._ctx.share_key())


def attach(socket: Any, *, first_use: bool = True) -> None:
    """Cover a socket before its first use if its context has an authenticator.

    At creation (``first_use=False``) the options are not known yet, so the
    callback is installed on every socket; native code applies it only to
    covered mechanisms. At first use it goes only to covered sockets. With no
    authenticator running, a covered socket is remembered for ``register``.
    """
    if first_use and not _covered(socket):
        return
    try:
        key = context_key(socket._context)
    except Exception:
        return  # A terminated context: the socket call itself raises.

    with _lock:
        if key not in _active:
            if first_use:
                _unguarded.setdefault(key, weakref.WeakSet()).add(socket)
            return

    # Raises once the socket is in use. The flag stays unset, so every
    # later bind(), connect() or monitor() raises too.
    socket._sock._set_zap_auth(_dispatcher(key))
    socket._zap_checked = True


def register(key: int, policy: Policy) -> None:
    """Make ``policy`` the authenticator for the context ``key``.

    Covers the remembered sockets that are not in use yet. Raises
    ``ZMQError`` (``EADDRINUSE``) when the context already has an
    authenticator, as binding a second ZAP handler does. Raises
    ``RuntimeError`` when a covered socket is already in use without one; it
    would admit every peer. Sockets covered before that error reject peers
    until an authenticator starts.
    """
    with _lock:
        if key in _active:
            raise ZMQError(
                errno.EADDRINUSE, "an authenticator is already running for this context"
            )

        exposed: weakref.WeakSet[Any] = weakref.WeakSet()
        for socket in list(_unguarded.pop(key, ())):
            if socket.closed or not _covered(socket):
                continue
            try:
                socket._sock._set_zap_auth(_dispatcher(key))
                socket._zap_checked = True
            except ValueError:
                exposed.add(socket)

        if exposed:
            _unguarded[key] = exposed
            raise RuntimeError(
                f"{len(exposed)} socket(s) of this context were bound or "
                "connected before the authenticator started and would admit "
                "every peer; start it before binding CURVE servers or sockets "
                "with a ZAP domain"
            )

        _active[key] = policy


def unregister(key: int, policy: Policy) -> None:
    with _lock:
        if _active.get(key) is policy:
            del _active[key]


def is_running(key: int | None, policy: Policy) -> bool:
    return key is not None and _active.get(key) is policy


def forget(context: Any) -> None:
    """Drop a terminating context's authenticator and remembered sockets."""
    if not (_active or _unguarded):
        return
    try:
        key = context_key(context)
    except Exception:
        return
    with _lock:
        _active.pop(key, None)
        _unguarded.pop(key, None)


def _dispatcher(key: int) -> Callable[[str, bytes, PeerInfo], bool]:
    # Holds the key, not the socket or the policy, so sockets see a restart
    # and a stopped authenticator admits nobody.
    def admit(mechanism: str, domain: bytes, peer: PeerInfo) -> bool:
        policy = _active.get(key)
        return policy is not None and policy._zap_admit(mechanism, domain, peer)

    return admit


def _covered(socket: Any) -> bool:
    """Would a libzmq ZAP handler see this socket's handshakes?

    An explicit ``set_curve_auth()`` or ``set_plain_auth()`` policy wins.
    """
    if socket._explicit_auth:
        return False

    from . import CURVE_SERVER, PLAIN_SERVER, ZAP_DOMAIN

    try:
        return bool(
            socket.getsockopt(CURVE_SERVER)
            or socket.getsockopt(PLAIN_SERVER)
            or socket.getsockopt(ZAP_DOMAIN)
        )
    except Exception:
        return False
