"""asyncio facade interop with pyzmq (sync). Both directions, all
patterns we currently support."""

import asyncio
import time

import pytest

zmq_pyzmq = pytest.importorskip("zmq")  # pyzmq

import pyomq
import pyomq.asyncio as zmq_async


@pytest.mark.asyncio
async def test_async_pyomq_push_to_pyzmq_pull(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    pull = py_ctx.socket(zmq_pyzmq.PULL)
    pull.bind(tcp_endpoint)
    try:
        ctx = zmq_async.Context()
        push = ctx.socket(pyomq.PUSH)
        await push.connect(tcp_endpoint)
        await push.send(b"async-pyomq-to-pyzmq")
        # Drop GIL so the in-flight send actually flushes.
        pull.setsockopt(zmq_pyzmq.RCVTIMEO, 1000)
        assert pull.recv() == b"async-pyomq-to-pyzmq"
        await push.close()
    finally:
        pull.close()


@pytest.mark.asyncio
async def test_async_pyzmq_push_to_pyomq_pull(tcp_endpoint):
    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    await pull.bind(tcp_endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        push = py_ctx.socket(zmq_pyzmq.PUSH)
        push.connect(tcp_endpoint)
        push.send(b"pyzmq-to-async-pyomq")
        pull.setsockopt(pyomq.RCVTIMEO, 1000)
        assert await pull.recv() == b"pyzmq-to-async-pyomq"
        push.close()
    finally:
        await pull.close()


@pytest.mark.asyncio
async def test_async_pyomq_pub_to_pyzmq_sub(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    sub = py_ctx.socket(zmq_pyzmq.SUB)
    sub.setsockopt(zmq_pyzmq.SUBSCRIBE, b"hot/")
    sub.connect(tcp_endpoint)
    try:
        ctx = zmq_async.Context()
        pub = ctx.socket(pyomq.PUB)
        await pub.bind(tcp_endpoint)
        await asyncio.sleep(0.2)
        await pub.send(b"cold/skip")
        await pub.send(b"hot/take")
        sub.setsockopt(zmq_pyzmq.RCVTIMEO, 1000)
        assert sub.recv() == b"hot/take"
        await pub.close()
    finally:
        sub.close()


@pytest.mark.asyncio
async def test_async_dealer_router_identity(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    router = py_ctx.socket(zmq_pyzmq.ROUTER)
    router.bind(tcp_endpoint)
    try:
        ctx = zmq_async.Context()
        dealer = ctx.socket(pyomq.DEALER)
        dealer.setsockopt(pyomq.IDENTITY, b"D-async")
        await dealer.connect(tcp_endpoint)
        await dealer.send(b"hi")
        parts = router.recv_multipart()
        assert parts[0] == b"D-async"
        assert parts[-1] == b"hi"
        router.send_multipart([b"D-async", b"reply"])
        assert await dealer.recv() == b"reply"
        await dealer.close()
    finally:
        router.close()
