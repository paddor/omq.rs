"""Track the lifetime of buffers borrowed by native messages."""

from __future__ import annotations

import math
import time
from threading import Event

from . import _native
from .error import ZMQBaseError


class NotDone(ZMQBaseError):
    """A tracked buffer is still in use."""


class MessageTracker:
    """Completion means buffer reuse is safe, not that a peer received data."""

    _sources: tuple[Event | MessageTracker | _native.ReleaseToken, ...]

    def __init__(
        self, *towatch: Event | MessageTracker | _native.Frame | _native.ReleaseToken
    ) -> None:
        sources: list[Event | MessageTracker | _native.ReleaseToken] = []
        for source in towatch:
            if isinstance(source, _native.Frame):
                if source.tracker is None:
                    raise ValueError("Not a tracked message")
                source = source.tracker
            if not isinstance(source, (Event, MessageTracker, _native.ReleaseToken)):
                raise TypeError("expected an Event, MessageTracker, or tracked Frame")
            sources.append(source)
        self._sources = tuple(sources)

    @property
    def done(self) -> bool:
        return all(
            s.is_set() if isinstance(s, Event) else s.done for s in self._sources
        )

    def wait(self, timeout: float | None = -1) -> None:
        if timeout is not None and not math.isfinite(timeout):
            raise ValueError("timeout must be finite")
        if len(self._sources) == 1 and isinstance(
            self._sources[0], _native.ReleaseToken
        ):
            # One native wait already owns its timeout. Avoid computing a
            # deadline and remaining time when there are no other sources.
            if not self._sources[0].wait(
                None if timeout is None or timeout < 0 else timeout
            ):
                raise NotDone
            return
        deadline = (
            None if timeout is None or timeout < 0 else time.monotonic() + timeout
        )
        for source in self._sources:
            remaining = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            if isinstance(source, MessageTracker):
                source.wait(remaining)
            elif not source.wait(remaining):
                raise NotDone


def _from_sources(
    *sources: MessageTracker | _native.ReleaseToken,
) -> MessageTracker:
    """Native callers already validated sources and resolved tracked Frames."""
    tracker = MessageTracker.__new__(MessageTracker)
    tracker._sources = sources
    return tracker


_FINISHED_TRACKER = MessageTracker()
