"""Buffer ownership and multipart retry regressions."""

import asyncio
import gc
import subprocess
import sys
import threading
import time
from array import array
from typing import Any, cast

import pyomq as zmq
import pyomq.asyncio as azmq
import pytest


async def _fill_send_ring(socket):
    deadline = time.monotonic() + 3
    full_rounds = 0
    while time.monotonic() < deadline:
        sent = 0
        for _ in range(10_000):
            try:
                socket._sock.send(b"fill")
                sent += 1
            except zmq._native.ZMQError as exc:
                assert exc.errno == zmq.EAGAIN
                break
        full_rounds = full_rounds + 1 if sent == 0 else 0
        if full_rounds == 3:
            return
        # Let the native pump finish any batch already taken from the ring.
        await asyncio.sleep(0.01)
    pytest.fail("native ring did not remain backpressured")


def test_frame_tracker_follows_exported_views():
    data = bytearray(100_000)
    frame = zmq.Frame(data, copy=False, track=True)
    tracker = frame.tracker
    assert tracker is not None and not tracker.done
    view = frame.buffer
    del frame
    with pytest.raises(zmq.NotDone):
        tracker.wait(0)
    del view
    gc.collect()
    tracker.wait(2)
    data.extend(b"safe")


def test_tracker_aggregation_and_timeout():
    first, last = threading.Event(), threading.Event()
    tracker = zmq.MessageTracker(zmq.MessageTracker(first), last)
    last.set()
    with pytest.raises(zmq.NotDone):
        tracker.wait(0.001)
    first.set()
    tracker.wait(0)
    assert tracker.done
    with pytest.raises(ValueError):
        zmq.MessageTracker(zmq.Frame(b"untracked"))


@pytest.mark.parametrize("timeout", [None, -1, 2])
def test_single_native_tracker_wait_releases_gil(timeout):
    frames = [zmq.Frame(bytearray(100_000), copy=False, track=True)]
    tracker = frames[0].tracker
    assert type(tracker) is zmq.MessageTracker

    def release():
        time.sleep(0.02)
        frames.clear()

    thread = threading.Thread(target=release)
    thread.start()
    try:
        tracker.wait(timeout)
    finally:
        thread.join(2)
    assert tracker.done
    tracker.wait(0)


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("released", [False, True])
def test_single_native_tracker_rejects_nonfinite_timeout(timeout, released):
    frame = zmq.Frame(bytearray(100_000), copy=False, track=True)
    tracker = frame.tracker
    assert tracker is not None
    if released:
        del frame
    with pytest.raises(ValueError, match="finite"):
        tracker.wait(timeout)


def test_multipart_preserves_existing_frame_tracker_and_raw_buffer_ownership():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://mixed-completion-sources")
        push.connect("inproc://mixed-completion-sources")
        data = bytearray(100_000)
        frame = zmq.Frame(data, copy=False, track=True)
        original = frame.tracker
        assert original is not None
        assert push.send_multipart([frame], copy=False, track=True) is original
        assert pull.recv() == data
        tracker = push.send_multipart(
            [bytearray(100_000), frame], copy=False, track=True
        )
        assert type(tracker) is zmq.MessageTracker
        received = pull.recv_multipart(copy=False)
        del frame
        # Keep only the raw-buffer part. Both kinds of source must be watched.
        received.pop()
        original.wait(2)
        assert not tracker.done
        received.clear()
        tracker.wait(2)
        data.extend(b"released")


@pytest.mark.parametrize("multipart", [False, True])
@pytest.mark.parametrize("size", [128, 1024])
def test_tcp_tracker_allows_buffer_reuse_across_processes(multipart, size):
    code = """
import sys
import pyomq as zmq
multipart = sys.argv[2] == "True"
size = int(sys.argv[3])
with zmq.Context() as ctx, ctx.socket(zmq.PULL) as pull:
    pull.rcvtimeo = 3000
    pull.connect(sys.argv[1])
    for index in range(16):
        expected = bytes([index]) * size
        if multipart:
            assert pull.recv_multipart() == [expected, expected]
        else:
            assert pull.recv() == expected
"""
    with zmq.Context() as ctx, ctx.socket(zmq.PUSH) as push:
        push.linger = 0
        push.sndtimeo = 3000
        push.bind("tcp://127.0.0.1:0")
        endpoint = push.last_endpoint
        assert endpoint is not None
        receiver = subprocess.Popen(
            [sys.executable, "-c", code, endpoint.decode(), str(multipart), str(size)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            data = bytearray(size)
            for index in range(16):
                data[:] = bytes([index]) * len(data)
                tracker = (
                    push.send_multipart([data, data], copy=False, track=True)
                    if multipart
                    else push.send(data, copy=False, track=True)
                )
                assert tracker is not None
                tracker.wait(3)
                # Completion must release every export, not just allow reads.
                data.append(0)
                data.pop()
            stdout, stderr = receiver.communicate(timeout=5)
            assert receiver.returncode == 0, stdout + stderr
        finally:
            if receiver.poll() is None:
                receiver.kill()
                receiver.wait()


@pytest.mark.parametrize("copy", [True, False])
def test_multibyte_buffers(copy):
    data = array("I", [1, 2, 3])
    assert bytes(zmq.Frame(data, copy=copy)) == data.tobytes()
    with pytest.raises(BufferError):
        zmq.Frame(memoryview(b"abcdef")[::2], copy=copy)


@pytest.mark.parametrize("retained_index", [0, 1])
def test_multipart_tracker_waits_for_every_inproc_part(retained_index):
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://tracked-multipart")
        push.connect("inproc://tracked-multipart")
        tracker = push.send_multipart(
            (bytearray(100_000) for _ in range(2)), copy=False, track=True
        )
        assert tracker is not None
        frames = pull.recv_multipart(copy=False, track=True)
        assert all(f.tracker is not None and f.tracker.done for f in frames)
        retained = frames.pop(retained_index)
        frames.clear()
        assert not tracker.done
        del retained
        gc.collect()
        tracker.wait(2)


def test_copied_send_and_tracked_frame_contract():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://tracker-copy")
        push.connect("inproc://tracker-copy")
        assert push.send(bytearray(b"copy"), copy=True, track=True) is None
        assert pull.recv() == b"copy"
        with pytest.raises(ValueError):
            push.send(zmq.Frame(b"untracked"), track=True)
        frame = zmq.Frame(b"tracked", copy=True, track=True)
        assert push.send(frame) is frame.tracker
        assert pull.recv() == b"tracked"


@pytest.mark.asyncio
@pytest.mark.parametrize("shadow", [False, True])
async def test_async_and_shadow_send_results(shadow):
    with (
        azmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://tracker-async")
        push.connect("inproc://tracker-async")
        if shadow:
            result = zmq.Socket.shadow(push).send_multipart(
                (part for part in [b"a", b"b"]), copy=False, track=True
            )
        else:
            result = await push.send_multipart(
                (part for part in [b"a", b"b"]), copy=False, track=True
            )
        assert isinstance(result, zmq.MessageTracker)
        assert await pull.recv_multipart() == [b"a", b"b"]
        result.wait(2)


@pytest.mark.asyncio
@pytest.mark.parametrize("sndmore", [False, True])
async def test_generator_survives_native_backpressure(sndmore):
    with (
        azmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        push.sndhwm = 1
        push.bind("inproc://generator-retry")
        await _fill_send_ring(push)
        iterations = []

        def parts():
            for value in (b"first", b"last"):
                iterations.append(value)
                yield value

        if sndmore:
            await push.send(b"first", zmq.SNDMORE)
            sent = asyncio.ensure_future(push.send(b"last"))
        else:
            sent = asyncio.ensure_future(push.send_multipart(parts()))
        pull.connect("inproc://generator-retry")
        while True:
            received = await asyncio.wait_for(pull.recv_multipart(), 3)
            if received != [b"fill"]:
                break
        assert received == [b"first", b"last"]
        assert await asyncio.wait_for(sent, 3) is None
        assert iterations == ([] if sndmore else [b"first", b"last"])


@pytest.mark.parametrize("async_socket", [False, True])
def test_close_releases_incomplete_multipart(async_socket):
    context = azmq.Context if async_socket else zmq.Context
    with context() as ctx:
        socket = ctx.socket(zmq.PUSH)
        data = bytearray(100_000)
        result: Any = socket.send(data, zmq.SNDMORE, copy=False, track=True)
        tracker = result.result() if async_socket else result
        assert isinstance(tracker, zmq.MessageTracker)
        assert not tracker.done
        socket.close(0)
        tracker.wait(2)
        data.extend(b"released")


def test_close_releases_unreceived_multipart_tail():
    with zmq.Context() as ctx, ctx.socket(zmq.PUSH) as push:
        pull = ctx.socket(zmq.PULL)
        pull.bind("inproc://tracked-tail")
        push.connect("inproc://tracked-tail")
        data = bytearray(100_000)
        tracker = push.send_multipart([b"header", data], copy=False, track=True)
        assert tracker is not None
        assert pull.recv() == b"header"
        assert not tracker.done
        pull.close(0)
        tracker.wait(2)
        data.extend(b"released")


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_pending_send_releases_export_on_cancel_or_close(cancel):
    with azmq.Context() as ctx, ctx.socket(zmq.PUSH) as push:
        push.sndhwm = 1
        push.bind("inproc://pending-release")
        await _fill_send_ring(push)
        data = bytearray(100_000)
        pending = push.send_multipart((part for part in [data]), copy=False, track=True)
        task = asyncio.ensure_future(pending)
        await asyncio.sleep(0)
        assert not task.done()
        with pytest.raises(BufferError):
            data.extend(b"still borrowed")
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            push.close(0)
            with pytest.raises(zmq.ZMQError):
                await asyncio.wait_for(task, 2)
        # Keep the awaitable/task alive: cancellation and close must release
        # the exporter without relying on their garbage collection.
        data.extend(b"released")
        assert pending.done()


def test_wait_releases_gil_and_uses_one_deadline():
    data = bytearray(100_000)
    frames = [zmq.Frame(data, copy=False, track=True)]
    tracker = zmq.MessageTracker(frames[0])

    def release():
        time.sleep(0.02)
        frames.clear()

    thread = threading.Thread(target=release)
    thread.start()
    try:
        tracker.wait(2)
    finally:
        thread.join(2)
    data.extend(b"released")
    first, last = threading.Event(), threading.Event()
    timer = threading.Timer(0.03, first.set)
    timer.start()
    try:
        start = time.monotonic()
        with pytest.raises(zmq.NotDone):
            zmq.MessageTracker(first, last).wait(0.05)
        # Broad upper bound tolerates scheduler noise; tests never assert an
        # exact wall-clock interval.
        assert time.monotonic() - start >= 0.045
    finally:
        timer.join()


@pytest.mark.asyncio
async def test_close_releases_all_pending_exports_after_registry_growth():
    with azmq.Context() as ctx, ctx.socket(zmq.PUSH) as socket:
        socket.sndhwm = 1
        socket.bind("inproc://pending-registry")
        await _fill_send_ring(socket)
        buffers = [bytearray(100_000) for _ in range(64)]
        pending = [socket.send(data, copy=False, track=True) for data in buffers]
        assert all(not future.done() for future in pending)
        socket.close(0)
        # Keep every awaitable alive: closing must visit all registry entries.
        for data in buffers:
            data.extend(b"released")
        assert all(future.done() for future in pending)


def test_failed_multipart_conversion_releases_previous_exports():
    with zmq.Context() as ctx, ctx.socket(zmq.PUSH) as push:
        data = bytearray(100_000)
        with pytest.raises(TypeError):
            push.send_multipart(cast(Any, [data, "invalid"]), copy=False, track=True)
        data.extend(b"released")


class BytesOnly:
    def __bytes__(self):
        return b"not a buffer"


@pytest.mark.parametrize("value", ["text", BytesOnly()])
@pytest.mark.parametrize("kind", ["sync", "async", "shadow"])
def test_send_rejects_non_buffers(value, kind):
    context = zmq.Context if kind == "sync" else azmq.Context
    with context() as ctx, ctx.socket(zmq.PUSH) as original:
        socket = zmq.Socket.shadow(original) if kind == "shadow" else original
        with pytest.raises(TypeError):
            socket.send(value)
        with pytest.raises(TypeError):
            socket.send_multipart([value])


def test_close_allows_reentrant_python_buffer_release():
    # A Python 3.12 buffer exporter can execute user code on release. Keep a
    # deadlock regression isolated so pytest can terminate it deterministically.
    code = """
import pyomq as zmq
released = []
with zmq.Context() as ctx:
    socket = ctx.socket(zmq.PUSH)
    class Exporter:
        def __buffer__(self, flags):
            return memoryview(bytearray(100_000))
        def __release_buffer__(self, view):
            try:
                socket.send(b"closed")
            except zmq.ZMQError:
                released.append(True)
    tracker = socket.send(Exporter(), zmq.SNDMORE, copy=False, track=True)
    socket.close(0)
    tracker.wait(2)
    assert released == [True]
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=5)


@pytest.mark.asyncio
async def test_shadow_retry_keeps_one_shot_parts():
    with (
        azmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        push.sndhwm = 1
        push.bind("inproc://shadow-retry")
        await _fill_send_ring(push)
        shadow = zmq.Socket.shadow(push)
        parts = iter([b"first", bytearray(100_000)])
        blocked = threading.Event()

        def submit():
            try:
                return push._sock.send_multipart(parts, copy=False, track=True)
            except zmq._native.ZMQError as exc:
                assert exc.errno == zmq.EAGAIN
                blocked.set()
                raise

        # Exercise the actual blocking retry path; observe its first native
        # EAGAIN before allowing a receiver to drain the backpressured socket.
        task = asyncio.create_task(asyncio.to_thread(shadow._blocking_send, submit))
        try:
            assert await asyncio.to_thread(blocked.wait, 3)
            pull.connect("inproc://shadow-retry")
            while True:
                received = await asyncio.wait_for(pull.recv_multipart(), 3)
                if received != [b"fill"]:
                    break
            assert received == [b"first", bytes(100_000)]
            tracker = await asyncio.wait_for(task, 3)
            assert tracker is not None
            tracker.wait(2)
        finally:
            push.close(0)
            await asyncio.gather(task, return_exceptions=True)


def test_cancellation_allows_reentrant_python_buffer_release():
    code = """
import asyncio
import runpy
import sys
import pyomq as zmq
import pyomq.asyncio as azmq
fill = runpy.run_path(sys.argv[1])["_fill_send_ring"]
async def run():
    with azmq.Context() as ctx, ctx.socket(zmq.PUSH) as socket:
        socket.sndhwm = 1
        socket.bind("inproc://release-cancel")
        await fill(socket)
        released = []
        class Exporter:
            def __buffer__(self, flags):
                return memoryview(bytearray(100_000))
            def __release_buffer__(self, view):
                socket.close(0)
                released.append(True)
        pending = socket.send(Exporter(), copy=False, track=True)
        task = asyncio.ensure_future(pending)
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert released == [True]
        assert socket.closed
asyncio.run(run())
"""
    subprocess.run([sys.executable, "-c", code, __file__], check=True, timeout=5)


@pytest.mark.asyncio
async def test_cleanup_future_marks_cancelled_before_cleanup():
    states = []
    future = None
    waiter = None

    def cleanup():
        assert future is not None
        assert waiter is not None
        states.append(future.cancelled())
        waiter.fail(RuntimeError("reentrant close"))

    future = azmq._CleanupFuture(
        loop=asyncio.get_running_loop(),
        cleanup=cleanup,
    )
    waiter = azmq._WindowsWaiter(future, lambda: None, cleanup)
    future.set_cleanup(waiter.release)
    assert future.cancel()
    assert states == [True]
