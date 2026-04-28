"""asyncio facade: pyomq.asyncio.Context / Socket roundtrips."""

import asyncio

import pytest

import pyomq
import pyomq.asyncio as zmq_async


@pytest.mark.asyncio
async def test_async_push_pull_inproc(inproc_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        await pull.bind(inproc_endpoint)
        await push.connect(inproc_endpoint)
        await push.send(b"hello")
        assert await pull.recv() == b"hello"
    finally:
        await push.close()
        await pull.close()


@pytest.mark.asyncio
async def test_async_push_pull_tcp(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send(b"tcp-hello")
        assert await pull.recv() == b"tcp-hello"
    finally:
        await push.close()
        await pull.close()


@pytest.mark.asyncio
async def test_async_send_multipart(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send_multipart([b"a", b"b", b"c"])
        assert await pull.recv_multipart() == [b"a", b"b", b"c"]
    finally:
        await push.close()
        await pull.close()


@pytest.mark.asyncio
async def test_async_pubsub(tcp_endpoint):
    ctx = zmq_async.Context()
    pub = ctx.socket(pyomq.PUB)
    sub = ctx.socket(pyomq.SUB)
    try:
        await pub.bind(tcp_endpoint)
        await sub.connect(tcp_endpoint)
        sub.setsockopt(pyomq.SUBSCRIBE, b"hot/")
        await asyncio.sleep(0.2)  # let SUBSCRIBE propagate
        await pub.send(b"cold/skip")
        await pub.send(b"hot/take")
        sub.setsockopt(pyomq.RCVTIMEO, 1000)
        assert await sub.recv() == b"hot/take"
    finally:
        await pub.close()
        await sub.close()


@pytest.mark.asyncio
async def test_async_concurrent_recvs(tcp_endpoint):
    """Many concurrent awaits on different Python tasks all wake up."""
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        await pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)

        # Fire off N concurrent recvs. AsyncSocket.recv returns an
        # asyncio.Future directly (not a coroutine), so wrap in
        # ensure_future so asyncio.gather is happy.
        N = 32
        recv_futs = [asyncio.ensure_future(pull.recv()) for _ in range(N)]
        await asyncio.sleep(0.05)  # let them register
        for i in range(N):
            await push.send(f"msg-{i}".encode())
        results = sorted(await asyncio.gather(*recv_futs))
        assert results == sorted(f"msg-{i}".encode() for i in range(N))
    finally:
        await push.close()
        await pull.close()


@pytest.mark.asyncio
async def test_async_mixed_with_sync(tcp_endpoint):
    """Async sender, sync receiver. Both share the wire."""
    ctx_async = zmq_async.Context()
    ctx_sync = pyomq.Context()
    pull = ctx_sync.socket(pyomq.PULL)
    push = ctx_async.socket(pyomq.PUSH)
    try:
        pull.bind(tcp_endpoint)
        await push.connect(tcp_endpoint)
        await push.send(b"mixed")
        assert pull.recv() == b"mixed"
    finally:
        await push.close()
        pull.close()
