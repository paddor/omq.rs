"""Send backpressure: async send waits (not spins) when HWM full."""

import asyncio

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
        ep = pull.bind(tcp_endpoint)
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
        ep = pull.bind(tcp_endpoint)
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
