"""Error hierarchy mirroring pyzmq's ``zmq.error``.

`ZMQError` is the native exception class itself (a subclass of
``OSError``). `Again` / `ContextTerminated` / `NotImplementedError` are
plain Python subclasses; the binding raises the base class with an
errno attribute set, and helper code (or user code) can promote to the
specific subclass when desired.
"""

import errno as _errno

from ._native import ZMQError as ZMQError  # type: ignore[attr-defined]


class Again(ZMQError):
    """Non-blocking call would block (``EAGAIN`` / timeout elapsed)."""


class ContextTerminated(ZMQError):
    """Operation issued against a terminated Context (``ETERM`` ≈ 156)."""


class NotImplementedError(ZMQError):  # noqa: A001  shadow OK; matches pyzmq
    """The requested option / feature is not implemented in pyomq v0.1."""


_BY_ERRNO = {
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
