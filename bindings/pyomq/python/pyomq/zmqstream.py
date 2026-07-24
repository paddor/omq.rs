"""ZMQStream: tornado IOLoop integration for pyomq sockets.

Registers the socket's FD with the tornado IOLoop. When the fd
signals readability, drains available messages and invokes the
on_recv callback.
"""

import asyncio
import os

import pyomq


def _get_IOLoop():
    from tornado.ioloop import IOLoop

    return IOLoop


class ZMQStream:
    def __init__(self, socket, io_loop=None):
        IOLoop = _get_IOLoop()
        self.socket = socket
        self.io_loop = io_loop or IOLoop.current()
        self._recv_callback = None
        self._recv_copy = True
        self._send_callback = None
        self._closed = False
        self._fd = socket.getsockopt(pyomq.FD)
        self._watching = False

    def on_recv(self, callback, copy=True):
        self._recv_callback = callback
        self._recv_copy = copy
        if callback is not None:
            self._start_watching()
        else:
            self._stop_watching()

    def on_send(self, callback):
        self._send_callback = callback

    def stop_on_recv(self):
        self.on_recv(None)

    def stop_on_send(self):
        self._send_callback = None

    def send(self, msg, flags=0, copy=True, track=False, callback=None, **kwargs):
        result = self.socket.send(msg, flags=flags, copy=copy, track=track)
        if self._send_callback:
            self._send_callback(msg, None)
        return result

    def send_multipart(
        self, msg_list, flags=0, copy=True, track=False, callback=None, **kwargs
    ):
        result = self.socket.send_multipart(
            msg_list,
            flags=flags,
            copy=copy,
            track=track,
        )
        if self._send_callback:
            self._send_callback(msg_list, None)
        return result

    def flush(self, flag=3, limit=None):
        if flag & 1 and self._recv_callback:
            self._handle_recv()

    def _handle_events(self, fd=None, events=None):
        if self._closed:
            return
        try:
            os.read(self._fd, 8)
        except OSError:
            pass
        self._handle_recv()

    def _handle_recv(self):
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

    def _start_watching(self):
        if self._closed or self._watching:
            return
        fd = self._fd
        handler = self._handle_events
        io_loop = self.io_loop

        def _do_add():
            if self._closed or self._watching:
                return
            try:
                io_loop.add_handler(fd, handler, _get_IOLoop().READ)
                self._watching = True
            except Exception:
                pass

        try:
            io_loop.add_callback(_do_add)
        except RuntimeError:
            _do_add()

    def _stop_watching(self):
        if not self._watching:
            return
        self._watching = False
        try:
            self.io_loop.remove_handler(self._fd)
        except Exception:
            pass

    def close(self, linger=None):
        if self._closed:
            return
        self._closed = True
        self._stop_watching()

    def setsockopt(self, opt, value):
        self.socket.setsockopt(opt, value)

    def getsockopt(self, opt):
        return self.socket.getsockopt(opt)

    @property
    def closed(self):
        return self._closed
