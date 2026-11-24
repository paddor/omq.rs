"""pyzmq-compatible ``AsyncioAuthenticator``."""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Awaitable
from typing import Any

import pyomq

from . import base
from .base import Authenticator, _wait

__all__ = ["AsyncioAuthenticator"]


class AsyncioAuthenticator(Authenticator):
    """Authenticator with pyzmq's ``AsyncioAuthenticator`` API.

    Awaitable results from a CURVE credentials provider run on the event
    loop that called ``start()``. The handshake waits for them on a worker
    thread, not on the loop.
    """

    def __init__(
        self,
        context: pyomq.Context[Any] | None = None,
        loop: Any = None,
        encoding: str = "utf-8",
        log: Any = None,
    ) -> None:
        super().__init__(context, encoding, log)
        if loop is not None:
            warnings.warn(
                f"{type(self).__name__}(loop) is deprecated and ignored",
                DeprecationWarning,
                stacklevel=2,
            )
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self) -> None:
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        super().start()

    def stop(self) -> None:
        super().stop()
        self._loop = None

    def _run_awaitable(self, awaitable: Awaitable[Any]) -> Any:
        loop = self._loop
        if loop is None or loop.is_closed() or not loop.is_running():
            return super()._run_awaitable(awaitable)
        future = asyncio.run_coroutine_threadsafe(_wait(awaitable), loop)
        try:
            return future.result(base._AWAIT_TIMEOUT)
        except TimeoutError:
            future.cancel()
            raise
