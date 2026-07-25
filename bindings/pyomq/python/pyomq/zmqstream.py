"""ZMQStream: tornado IOLoop integration for pyomq sockets.

Registers the socket's FD with the tornado IOLoop. When the fd
signals readability, drains available messages and invokes the
on_recv callback.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Callable, Optional, Union, List

import pyomq


def _get_IOLoop() -> type:
    from tornado.ioloop import IOLoop

    return IOLoop


class ZMQStream:
    """Integration layer for pyomq sockets with Tornado IOLoop."""

    socket: pyomq.Socket
    io_loop: Any  # tornado.ioloop.IOLoop
    _recv_callback: Optional[Callable[[Any], Any]]
    _recv_copy: bool
    _send_callback: Optional[Callable[[Any, Optional[Any]], Any]]
    _closed: bool
    _fd: int
    _watching: bool

    def __init__(self, socket: pyomq.Socket, io_loop: Optional[Any] = None) -> None:
        IOLoop = _get_IOLoop()
        self.socket = socket
        self.io_loop = io_loop or IOLoop.current()  # type: ignore[ty:unresolved-attribute]
        self._recv_callback = None
        self._recv_copy = True
        self._send_callback = None
        self._closed = False
        self._fd = socket.getsockopt(pyomq.FD)
        self._watching = False

    def on_recv(
        self, callback: Optional[Callable[[Any], Any]], copy: bool = True
    ) -> None:
        """Set a callback to be invoked when messages are received."""
        self._recv_callback = callback
        self._recv_copy = copy
        if callback is not None:
            self._start_watching()
        else:
            self._stop_watching()

    def on_send(self, callback: Optional[Callable[[Any, Optional[Any]], Any]]) -> None:
        """Set a callback to be invoked when sends complete."""
        self._send_callback = callback

    def stop_on_recv(self) -> None:
        """Stop receiving messages."""
        self.on_recv(None)

    def stop_on_send(self) -> None:
        """Stop receiving send completion callbacks."""
        self._send_callback = None

    def send(
        self,
        msg: Union[bytes, str],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        callback: Optional[Callable[[Any, Any], Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """Send a message."""
        result = self.socket.send(msg, flags=flags, copy=copy, track=track)
        if self._send_callback:
            self._send_callback(msg, None)
        return result

    def send_multipart(
        self,
        msg_list: List[Union[bytes, str]],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        callback: Optional[Callable[[Any, Any], Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """Send a multipart message."""
        result = self.socket.send_multipart(
            msg_list,
            flags=flags,
            copy=copy,
            track=track,
        )
        if self._send_callback:
            self._send_callback(msg_list, None)
        return result

    def flush(self, flag: int = 3, limit: Optional[int] = None) -> None:
        """Flush pending messages."""
        if flag & 1 and self._recv_callback:
            self._handle_recv()

    def _handle_events(
        self, fd: Optional[int] = None, events: Optional[int] = None
    ) -> None:
        """Internal handler for IOLoop events."""
        if self._closed:
            return
        try:
            os.read(self._fd, 8)
        except OSError:
            pass
        self._handle_recv()

    def _handle_recv(self) -> None:
        """Drain and invoke callback for all available messages."""
        if self._recv_callback is None:
            return
        while True:
            try:
                parts = self.socket.recv_multipart(
                    pyomq.NOBLOCK,
                    copy=self._recv_copy,
                )
            except pyomq.Again:
                break
            except Exception:
                break
            result = self._recv_callback(parts)
            if asyncio.iscoroutine(result):
                asyncio.ensure_future(result)

    def _start_watching(self) -> None:
        """Register the socket FD with the IOLoop."""
        if self._closed or self._watching:
            return
        fd = self._fd
        handler = self._handle_events
        io_loop = self.io_loop

        def _do_add() -> None:
            if self._closed or self._watching:
                return
            try:
                io_loop.add_handler(fd, handler, _get_IOLoop().READ)  # type: ignore[ty:unresolved-attribute]
                self._watching = True
            except Exception:
                pass

        try:
            io_loop.add_callback(_do_add)
        except RuntimeError:
            _do_add()

    def _stop_watching(self) -> None:
        """Unregister the socket FD from the IOLoop."""
        if not self._watching:
            return
        self._watching = False
        try:
            self.io_loop.remove_handler(self._fd)
        except Exception:
            pass

    def close(self, linger: Optional[int] = None) -> None:
        """Close the stream and unregister from IOLoop."""
        if self._closed:
            return
        self._closed = True
        self._stop_watching()

    def setsockopt(self, opt: int, value: Any) -> Any:
        """Set a socket option."""
        return self.socket.setsockopt(opt, value)

    def getsockopt(self, opt: int) -> Any:
        """Get a socket option."""
        return self.socket.getsockopt(opt)

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._closed
