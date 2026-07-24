"""Async wrapper parity tests."""

import pytest

import pyomq as zmq
import pyomq.asyncio as zmq_async


async def test_async_again_exception(tcp_endpoint):
    import asyncio

    ctx = zmq_async.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.close()
        with pytest.raises(zmq.ContextTerminated):
            await pull.recv()
    finally:
        pass


async def test_async_send_recv_string(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_string("hello")
        assert await pull.recv_string() == "hello"
    finally:
        push.close()
        pull.close()


async def test_async_send_recv_json(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_json({"k": 1})
        assert await pull.recv_json() == {"k": 1}
    finally:
        push.close()
        pull.close()


async def test_async_send_recv_pyobj(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_pyobj([1, 2, 3])
        assert await pull.recv_pyobj() == [1, 2, 3]
    finally:
        push.close()
        pull.close()


async def test_async_closed_property(tcp_endpoint):
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.closed is False
    sock.close()
    assert sock.closed is True


async def test_async_context_property():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.context is ctx
    sock.close()


async def test_async_socket_type():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.socket_type == zmq.PUSH
    sock.close()


# ── New Phase 2/3 parity tests ─────────────────────────────────────


async def test_async_setsockopt_string():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.setsockopt_string(zmq.IDENTITY, "foo")
        assert sock.getsockopt_string(zmq.IDENTITY) == "foo"
    finally:
        sock.close()


async def test_async_set_string_get_string():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.set_string(zmq.IDENTITY, "bar")
        assert sock.get_string(zmq.IDENTITY) == "bar"
    finally:
        sock.close()


async def test_async_set_hwm_get_hwm():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.set_hwm(300)
        assert sock.get_hwm() == 300
        assert sock.getsockopt(zmq.SNDHWM) == 300
        assert sock.getsockopt(zmq.RCVHWM) == 300
    finally:
        sock.close()


async def test_async_hwm_property():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.hwm = 150
        assert sock.hwm == 150
    finally:
        sock.close()


async def test_async_send_recv_serialized(tcp_endpoint):
    ctx = zmq_async.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)

        def ser(msg):
            return [b"hdr", msg.encode("utf-8")]

        def deser(frames):
            assert frames[0] == b"hdr"
            return frames[1].decode("utf-8")

        push.send_serialized("async-hello", ser)
        assert await pull.recv_serialized(deser) == "async-hello"
    finally:
        push.close()
        pull.close()


async def test_async_repr():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        r = repr(sock)
        assert "pyomq.asyncio.Socket" in r
        assert "PULL" in r
    finally:
        sock.close()


async def test_async_underlying():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        assert sock.underlying is sock
    finally:
        sock.close()


async def test_async_attr_linger():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.linger = 0
        assert sock.linger == 0
        sock.linger = 500
        assert sock.linger == 500
    finally:
        sock.close()


async def test_async_attr_identity():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.identity = b"async-id"
        assert sock.identity == b"async-id"
    finally:
        sock.close()


async def test_async_attr_unknown_raises():
    ctx = zmq_async.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        with pytest.raises(AttributeError):
            _ = sock.nonexistent_attribute
    finally:
        sock.close()


async def test_async_context_closed():
    ctx = zmq_async.Context()
    assert ctx.closed is False
    ctx.term()
    assert ctx.closed is True


async def test_async_context_destroy():
    ctx = zmq_async.Context()
    s1 = ctx.socket(zmq.PUSH)
    s2 = ctx.socket(zmq.PULL)
    assert not s1.closed
    ctx.destroy(linger=0)
    assert ctx.closed
