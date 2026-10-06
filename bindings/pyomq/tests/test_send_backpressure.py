"""Send backpressure: async send waits (not spins) when HWM full."""

import asyncio
import time

import pyomq
import pyomq.asyncio as zmq_async
import pytest


@pytest.mark.asyncio
async def test_async_send_completes_after_drain(tcp_endpoint):
    """Fill the send HWM, start an async send, drain consumer, verify it completes."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        push.setsockopt(pyomq.SNDHWM, 2)
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        await asyncio.sleep(0.1)

        await push.send(b"1")
        await push.send(b"2")

        send_task = asyncio.ensure_future(push.send(b"3"))

        await asyncio.sleep(0.05)

        msg1 = await pull.recv()
        assert msg1 == b"1"

        msg3 = await asyncio.wait_for(send_task, timeout=5.0)
    finally:
        push.close()
        pull.close()


@pytest.mark.asyncio
async def test_async_send_does_not_block_event_loop(tcp_endpoint):
    """Other coroutines run while send waits for HWM space."""
    ctx = zmq_async.Context()
    push = ctx.socket(pyomq.PUSH)
    pull = ctx.socket(pyomq.PULL)
    try:
        push.setsockopt(pyomq.SNDHWM, 1)
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        await asyncio.sleep(0.1)

        await push.send(b"fill")

        canary = []

        async def background():
            canary.append(True)

        send_task = asyncio.ensure_future(push.send(b"blocked"))
        bg_task = asyncio.ensure_future(background())

        await asyncio.sleep(0.05)
        assert len(canary) == 1

        await pull.recv()
        await asyncio.wait_for(send_task, timeout=5.0)
        await bg_task
    finally:
        push.close()
        pull.close()


def test_sync_sndtimeo_raises_again(tcp_endpoint):
    """SNDTIMEO causes Again when send pipeline is full and timeout elapses."""
    ctx = pyomq.Context()
    push = ctx.socket(pyomq.PUSH)
    try:
        push.setsockopt(pyomq.SNDHWM, 1)
        push.setsockopt(pyomq.SNDTIMEO, 200)
        push.bind(tcp_endpoint)

        with pytest.raises(pyomq.Again):
            for _ in range(1000):
                push.send(b"x")
    finally:
        push.close()
        ctx.term()


@pytest.mark.parametrize("mandatory", [False, True])
def test_router_full_destination_policy(inproc_endpoint, mandatory):
    ctx = pyomq.Context()
    router = ctx.socket(pyomq.ROUTER)
    slow = ctx.socket(pyomq.DEALER)
    fast = ctx.socket(pyomq.DEALER)
    try:
        router.sndhwm = 1
        router.sndtimeo = 2000
        router.rcvtimeo = 2000
        router.router_mandatory = int(mandatory)
        slow.identity = b"slow"
        slow.rcvhwm = 1
        slow.rcvtimeo = 2000
        fast.identity = b"fast"
        fast.rcvtimeo = 2000
        router.bind(inproc_endpoint)
        for peer in (slow, fast):
            peer.connect(inproc_endpoint)
            peer.send(b"ready")
            assert router.recv_multipart() == [peer.identity, b"ready"]

        message = [b"slow", b"header", b"body"]
        accepted = 0
        started = time.monotonic()
        for _ in range(64):
            try:
                router.send_multipart(message, pyomq.DONTWAIT)
            except pyomq.Again:
                assert mandatory
                break
            accepted += 1
        assert time.monotonic() - started < 1, "DONTWAIT must ignore SNDTIMEO"
        router.sndtimeo = 50
        if mandatory:
            assert 0 < accepted < 64
            with pytest.raises(pyomq.Again):
                router.send_multipart(message)
        else:
            assert accepted == 64
            router.send_multipart(message)

        router.send_multipart([b"fast", b"available"])
        assert fast.recv() == b"available"
        drained = 0
        while True:
            try:
                assert slow.recv_multipart(pyomq.DONTWAIT) == [b"header", b"body"]
                drained += 1
            except pyomq.Again:
                break
        assert 0 < drained < 64
        if mandatory:
            assert drained == accepted
        router.send_multipart([b"slow", b"after"])
        assert slow.recv_multipart() == [b"after"]
    finally:
        router.close()
        slow.close()
        fast.close()
        ctx.term()
