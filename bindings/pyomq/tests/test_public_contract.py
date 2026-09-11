"""Runtime checks for public contracts not exercised by native stubtest."""

import errno
from typing import Any, cast

import pyomq as zmq
import pyomq.asyncio as azmq
import pytest


@pytest.mark.parametrize("context", [zmq.Context, azmq.Context])
def test_bytes_endpoints_and_metadata(context):
    with context() as ctx, ctx.socket(zmq.PUSH) as socket:
        scope = socket.bind(b"inproc://bytes-contract")
        assert scope.addr == "inproc://bytes-contract"
        assert socket.last_endpoint == b"inproc://bytes-contract"
        assert socket.getsockopt(zmq.LAST_ENDPOINT) == b"inproc://bytes-contract"
        socket.unbind(b"inproc://bytes-contract")
        socket.connect(b"inproc://bytes-contract")
        socket.disconnect(b"inproc://bytes-contract")
        assert socket.connection_info(999_999) is None


@pytest.mark.parametrize("context", [zmq.Context, azmq.Context])
def test_auth_accepts_one_shot_iterables(context):
    with context() as ctx, ctx.socket(zmq.PULL) as socket:
        socket.set_plain_auth(pair for pair in [("user", "password")])
        public, _ = zmq.curve_keypair()
        socket.set_curve_auth(key for key in [public])


def test_error_constructor_contract():
    error = zmq.ZMQError(errno.EAGAIN)
    assert error.errno == errno.EAGAIN
    assert error.strerror == zmq.strerror(errno.EAGAIN)
    assert str(error) == error.strerror
    error = zmq.ZMQError(errno=errno.EINVAL, msg="invalid")
    assert error.errno == errno.EINVAL
    assert str(error) == error.strerror == "invalid"
    assert zmq.ZMQError("message").strerror == "message"
    assert zmq.ZMQError(msg="message").strerror == "message"
    assert zmq.ZMQError().errno is None


def test_pyzmq_keyword_and_scope_contracts():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        bind_scope = pull.bind("inproc://scope-contract")
        assert bind_scope.socket is pull
        assert bind_scope.kind == "bind"
        assert bind_scope.addr == "inproc://scope-contract"
        assert not isinstance(bind_scope, str)
        assert str(bind_scope) == "<SocketContext(bind='inproc://scope-contract')>"
        with pytest.raises(TypeError):
            push.connect(cast(Any, bind_scope))
        push.connect(pull.last_endpoint)
        push.send_multipart(msg_parts=[b"one", b"two"])
        assert pull.recv_multipart() == [b"one", b"two"]
        bind_scope.__exit__()

        with pull.bind("inproc://scope-contract-with") as bound:
            assert bound is pull
            with push.connect("inproc://scope-contract-with") as connected:
                assert connected is push

        sub = ctx.socket(zmq.SUB)
        sub.subscribe(topic=b"topic")
        sub.unsubscribe(topic=b"topic")
        sub.close()

        dealer = ctx.socket(zmq.DEALER)
        dealer.setsockopt_string(zmq.IDENTITY, optval="identity")
        assert dealer.getsockopt(zmq.IDENTITY) == b"identity"
        dealer.close()

        _, secret_key = zmq.curve_keypair()
        assert zmq.curve_public(secret_key=secret_key)
        assert zmq.strerror(errno=zmq.EAGAIN)


def test_recv_into_matches_pyzmq_copy_contract():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        endpoint = "inproc://recv-into-contract"
        pull.bind(endpoint)
        push.connect(endpoint)
        target = bytearray(3)
        push.send(b"hello")
        assert pull.recv_into(target) == 5
        assert target == b"hel"
        target = bytearray(10)
        push.send(b"hello")
        assert pull.recv_into(target, nbytes=2) == 5
        assert target[:2] == b"he"


def test_context_shadow_constructor_contract():
    with zmq.Context() as context:
        with zmq.Context(shadow=context) as shadow:
            assert shadow._ctx is context._ctx
        with zmq.Context(context) as shadow:
            assert shadow._ctx is context._ctx
    with azmq.Context() as context, azmq.Context(shadow=context) as shadow:
        assert shadow._ctx is context._ctx


def test_routing_id_keyword_contract():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.SERVER) as server,
        ctx.socket(zmq.CLIENT) as client,
    ):
        endpoint = "inproc://routing-id-contract"
        server.bind(endpoint)
        client.connect(endpoint)
        client.send(b"ping")
        request = server.recv(copy=False)
        server.send_multipart(msg_parts=[b"pong"], routing_id=request.routing_id)
        assert client.recv() == b"pong"


@pytest.mark.asyncio
async def test_async_recv_into_contract():
    with (
        azmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        endpoint = "inproc://async-recv-into-contract"
        pull.bind(endpoint)
        push.connect(endpoint)
        target = bytearray(3)
        await push.send(b"hello")
        assert await pull.recv_into(target) == 5
        assert target == b"hel"


@pytest.mark.asyncio
async def test_stream_callbacks_receive_parts_and_tracker():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://stream-callbacks")
        push.connect("inproc://stream-callbacks")
        stream = zmq.ZMQStream(push)
        called = []
        stream.on_send(
            lambda parts, tracker: called.append(("default", parts, tracker))
        )
        tracker = stream.send(b"first", copy=False, track=True)
        assert called == [("default", [b"first"], tracker)]
        assert pull.recv() == b"first"
        tracker = stream.send_multipart(
            (part for part in [b"second", b"third"]),
            copy=False,
            track=True,
            callback=lambda parts, tracker: called.append(("override", parts, tracker)),
        )
        assert called[-1] == ("override", [b"second", b"third"], tracker)
        assert len(called) == 2
        assert pull.recv_multipart() == [b"second", b"third"]

        class FalseyCallback:
            def __bool__(self):
                return False

            def __call__(self, parts, tracker):
                called.append(("falsey", parts, tracker))

        stream.send(b"fourth", callback=FalseyCallback())
        assert called[-1] == ("falsey", [b"fourth"], None)
        assert pull.recv() == b"fourth"
        stream.send_multipart(iter([b"fifth"]), callback=FalseyCallback())
        assert called[-1] == ("falsey", [b"fifth"], None)
        assert pull.recv_multipart() == [b"fifth"]
        stream.close()
