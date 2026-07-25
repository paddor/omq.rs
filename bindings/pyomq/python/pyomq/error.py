"""Error hierarchy mirroring pyzmq's ``zmq.error``.

``ZMQBaseError`` is the root exception (above ``ZMQError``), matching
pyzmq's hierarchy. ``ZMQBindError`` is a sibling of ``ZMQError`` under
``ZMQBaseError``, not a subclass of ``ZMQError``.
"""

from __future__ import annotations

import builtins
import errno as _errno
from typing import Final

from ._native import ZMQBaseError as ZMQBaseError  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]
from ._native import ZMQError as ZMQError  # type: ignore[attr-defined]  # ty:ignore[unresolved-import]


class Again(ZMQError):
    """Non-blocking call would block (``EAGAIN`` / timeout elapsed)."""


class ContextTerminated(ZMQError):
    """Operation issued against a terminated Context (``ETERM`` ≈ 156)."""


class NotImplementedError(ZMQError):  # noqa: A001  shadow OK; matches pyzmq
    """The requested option / feature is not implemented in pyomq."""


class ZMQBindError(ZMQBaseError):
    """Binding failed (e.g. ``bind_to_random_port`` exhausted all tries)."""


class InterruptedSystemCall(ZMQError):
    """Interrupted system call (``EINTR``). Never raised by pyomq."""


class ZMQVersionError(builtins.NotImplementedError, ZMQBaseError):
    """Feature requires a newer libzmq than the emulated version."""


_BY_ERRNO: Final[dict[int, type[ZMQError]]] = {
    _errno.EAGAIN: Again,
    _errno.ETIMEDOUT: Again,
    156: ContextTerminated,
    _errno.ENOSYS: NotImplementedError,
}


def from_native(exc: ZMQError) -> ZMQError:
    """Promote a native ZMQError to the most specific subclass."""
    cls = _BY_ERRNO.get(getattr(exc, "errno", None) or -1, ZMQError)
    new = cls(str(exc))
    new.errno = getattr(exc, "errno", None)  # type: ignore[attr-defined]
    return new
