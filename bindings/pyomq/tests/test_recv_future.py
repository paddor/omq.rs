"""_RecvFuture: both await and blocking .result() paths."""

import asyncio
import os
import select
import sys
import threading
import types
from typing import Any

import pytest

import pyomq
import pyomq.asyncio as zmq_async


async def _await(value):
    return await value


@pytest.mark.asyncio
async def test_recv_future_await(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"await-test")
        msg = await pull.recv()
        assert msg == b"await-test"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_recv_future_fast_path(tcp_endpoint):
    """Message already available returns a resolved future."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"fast")
        await asyncio.sleep(0.1)
        msg = await pull.recv()
        assert msg == b"fast"
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_recv_future_done_transitions(tcp_endpoint):
    """_RecvFuture.done() transitions from False to True."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        fut = pull.recv()

        push.send(b"done-test")
        msg = await fut
        assert msg == b"done-test"
    finally:
        push.close()
        pull.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Unix fd readiness path only")
@pytest.mark.asyncio
async def test_cancelled_read_does_not_close_reused_fd(tcp_endpoint, monkeypatch):
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        pending: Any = pull.recv_multipart()
        old_fd = pending._fd
        pending_task = asyncio.create_task(_await(pending))
        await asyncio.sleep(0)

        push.send_multipart([b"raced"])
        ready, _, _ = select.select([old_fd], [], [], 5.0)
        assert ready

        replacement: list[Any] = []
        replacement_created = asyncio.Event()
        real_os = zmq_async.os
        os_proxy = types.ModuleType("os")
        os_proxy.__dict__.update(vars(real_os))

        def close_and_reuse(fd: int) -> None:
            os.close(fd)
            if fd == old_fd and not replacement:
                replacement.append(pull.recv_multipart())
                replacement_created.set()

        setattr(os_proxy, "close", close_and_reuse)
        monkeypatch.setattr(zmq_async, "os", os_proxy)

        asyncio.get_running_loop().call_soon(pending_task.cancel)
        await replacement_created.wait()
        assert replacement[0]._fd == old_fd

        replacement_task = asyncio.create_task(_await(replacement[0]))
        await asyncio.sleep(0)
        push.send_multipart([b"replacement"])
        assert await asyncio.wait_for(replacement_task, timeout=5.0) == [b"replacement"]

        with pytest.raises(asyncio.CancelledError):
            await pending_task
    finally:
        push.close()
        pull.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows callback path only")
@pytest.mark.event_loop("selector", "proactor")
@pytest.mark.asyncio
async def test_windows_cancelled_recv_does_not_stall_replacement(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        for index in range(200):
            pending_task = asyncio.create_task(_await(pull.recv_multipart()))
            await asyncio.sleep(0)
            pending_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending_task
            await asyncio.sleep(0)
            assert not pull._recv_waiters

            replacement_task = asyncio.create_task(_await(pull.recv_multipart()))
            push.send_multipart([str(index).encode()])
            assert await asyncio.wait_for(replacement_task, timeout=5.0) == [
                str(index).encode()
            ]
    finally:
        push.close()
        pull.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows callback path only")
def test_windows_socket_rejects_cross_loop_use(inproc_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    first_loop = asyncio.new_event_loop()
    second_loop = asyncio.new_event_loop()

    async def bind_to_first_loop():
        pending = asyncio.ensure_future(pull.recv())
        pending.cancel()
        await asyncio.sleep(0)

    async def use_from_second_loop():
        with pytest.raises(RuntimeError, match="different event loop"):
            pull.recv()

    try:
        pull.bind(inproc_endpoint)
        first_loop.run_until_complete(bind_to_first_loop())
        second_loop.run_until_complete(use_from_second_loop())
    finally:
        pull.close()
        first_loop.close()
        second_loop.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows callback path only")
def test_windows_schedule_failure_releases_native_claim():
    class ClosingLoop:
        def is_closed(self):
            return False

        def call_soon_threadsafe(self, callback):
            raise RuntimeError("event loop closed during scheduling")

    class NativeSpy:
        def __init__(self):
            self.clear_calls = 0
            self.complete_calls = 0

        def _clear_wakeup_modes(self, **kwargs):
            self.clear_calls += 1

        def _mark_recv_drain_complete(self):
            self.complete_calls += 1

    native = NativeSpy()
    sock = object.__new__(zmq_async.Socket)
    sock._sock = native
    sock._loop = ClosingLoop()
    sock._closed = True

    sock._schedule_recv_drain()

    assert native.clear_calls == 1
    assert native.complete_calls == 1
