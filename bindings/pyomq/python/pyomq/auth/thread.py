"""pyzmq-compatible ``ThreadAuthenticator``."""

from __future__ import annotations

from typing import Any

import pyomq

from .base import Authenticator

__all__ = ["ThreadAuthenticator"]


class ThreadAuthenticator(Authenticator):
    """Authenticator with pyzmq's ``ThreadAuthenticator`` API.

    pyomq runs the policy on handshake worker threads, so ``start()`` starts
    no thread. ``thread`` and ``pipe`` stay ``None``.
    """

    pipe: None
    pipe_endpoint: str
    thread: None

    def __init__(
        self,
        context: pyomq.Context[Any] | None = None,
        encoding: str = "utf-8",
        log: Any = None,
    ) -> None:
        super().__init__(context=context, encoding=encoding, log=log)
        self.pipe = None
        self.pipe_endpoint = ""
        self.thread = None

    def is_alive(self) -> bool:
        """Is the authenticator running?"""
        return self._is_running()
