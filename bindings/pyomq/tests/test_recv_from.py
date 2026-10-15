"""Owning received identities across binding queues and peer churn."""

import asyncio

import pyomq as zmq
import pyomq.asyncio as azmq
import pytest
from conftest import wait_for


@pytest.mark.parametrize("transport", ["tcp://127.0.0.1:0", "inproc://recv-from"])
@pytest.mark.parametrize("peer", [False, True])
@pytest.mark.parametrize("copy", [True, False])
def test_recv_from_roundtrip_and_lifetime(transport, peer, copy):
    ctx = zmq.Context()
    server = ctx.socket(zmq.PEER if peer else zmq.ROUTER)
    client = ctx.socket(zmq.PEER if peer else zmq.DEALER)
    try:
        server.identity = b"server"
        client.identity = b"client"
        server.rcvtimeo = client.rcvtimeo = 5000
        server.bind(transport)
        client.connect(server.last_endpoint)
        if peer:
            assert wait_for(
                lambda: any(p["identity"] == b"server" for p in client.connections())
            )
        parts = [b"", b"payload" * 100]
        client.send_multipart(([b"server"] if peer else []) + parts)
        identity, body = server.recv_from(copy=copy)
        assert isinstance(identity, zmq.Identity)
        assert bytes(identity) == b"client"
        assert [bytes(part) for part in body] == parts
        assert isinstance(body[0], bytes if copy else zmq.Frame)
        info = identity.peer_info
        assert isinstance(info, zmq.PeerInfo)
        assert info.authentication.public_key is None
        assert info.password is None
        assert info.diagnostics.endpoint == server.last_endpoint.decode()
        metadata = info.authentication.metadata
        metadata["injected"] = b"value"
        assert "injected" not in info.authentication.metadata
        with pytest.raises(AttributeError):
            info.diagnostics.endpoint = "changed"
        server.send_to(identity, [b"reply"])
        if peer:
            sender, reply = client.recv_from()
            assert bytes(sender) == b"server"
            assert reply == [b"reply"]
        else:
            assert client.recv() == b"reply"
        client.send_multipart(([b"server"] if peer else []) + [b"again"])
        again, _ = server.recv_from()
        assert again == identity
        assert hash(again) == hash(identity)
        with pytest.raises(zmq.Again):
            server.recv_from(zmq.NOBLOCK)
    finally:
        client.close(0)
        server.close(0)
        ctx.term()
    assert bytes(identity) == b"client"
    assert identity.peer_info.diagnostics.endpoint == info.diagnostics.endpoint
    assert [bytes(part) for part in body] == parts


def test_partial_receive_and_unsupported_socket_do_not_consume():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.ROUTER) as server,
        ctx.socket(zmq.DEALER) as client,
    ):
        server.bind("inproc://partial")
        client.connect("inproc://partial")
        server.rcvtimeo = 5000
        client.send_multipart([b"one", b"two"])
        server.recv()  # consume just the routing prefix
        with pytest.raises(ValueError, match="finish"):
            server.recv_from()
        assert server.recv_multipart() == [b"one", b"two"]
        with pytest.raises(ValueError, match="ROUTER or PEER"):
            client.recv_from(zmq.NOBLOCK)


@pytest.mark.skipif(not zmq.has("curve"), reason="CURVE not built")
def test_curve_snapshot_survives_same_route_handover():
    server_public, server_secret = zmq.curve_keypair()
    with zmq.Context() as ctx, ctx.socket(zmq.ROUTER) as server:
        server.curve_server = 1
        server.curve_publickey = server_public
        server.curve_secretkey = server_secret
        server.setsockopt(zmq.ROUTER_HANDOVER, 1)
        server.rcvtimeo = 5000
        server.bind("tcp://127.0.0.1:0")
        identities = []
        keys = []
        for n in range(2):
            with ctx.socket(zmq.DEALER) as client:
                public, secret = zmq.curve_keypair()
                keys.append(public)
                client.identity = b"same-route"
                client.curve_publickey = public
                client.curve_secretkey = secret
                client.curve_serverkey = server_public
                client.connect(server.last_endpoint)
                client.send(str(n).encode())
                identity, body = server.recv_from()
                identities.append(identity)
                assert body == [str(n).encode()]
                auth = identity.peer_info.authentication
                assert auth.mechanism == "CURVE"
                assert auth.public_key_type == "CURVE"
                assert len(auth.public_key) == 32
                assert identity.peer_info.public_key == public
                assert identity.peer_info.password is None
        assert identities[0] != identities[1]
        assert bytes(identities[0]) == bytes(identities[1])
    for identity, key in zip(identities, keys):
        assert identity.peer_info.public_key == key


@pytest.mark.asyncio
@pytest.mark.parametrize("copy", [True, False])
@pytest.mark.parametrize("peer", [False, True])
@pytest.mark.parametrize("transport", ["inproc://async-from", "tcp://127.0.0.1:0"])
async def test_async_recv_from(copy, peer, transport):
    ctx = azmq.Context()
    server = ctx.socket(zmq.PEER if peer else zmq.ROUTER)
    client = ctx.socket(zmq.PEER if peer else zmq.DEALER)
    try:
        server.identity = b"server"
        server.bind(transport)
        endpoint = server.last_endpoint
        client.identity = b"async-client"
        client.connect(endpoint)
        if peer:

            async def connected():
                while not any(p["identity"] == b"server" for p in client.connections()):
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(connected(), 5)
        pending = asyncio.ensure_future(server.recv_from(copy=copy))
        await client.send_multipart(([b"server"] if peer else []) + [b"one", b"two"])
        identity, body = await asyncio.wait_for(pending, 5)
        assert [bytes(part) for part in body] == [b"one", b"two"]
        assert isinstance(body[0], bytes if copy else zmq.Frame)
        assert bytes(identity) == b"async-client"
        assert identity.peer_info.authentication.public_key is None
        await server.send_to(identity, [b"reply"])
        if peer:
            _, reply = await asyncio.wait_for(client.recv_from(), 5)
            assert reply == [b"reply"]
        else:
            assert await asyncio.wait_for(client.recv(), 5) == b"reply"
        with pytest.raises(zmq.Again):
            await server.recv_from(zmq.NOBLOCK)
        pending = asyncio.ensure_future(server.recv_from())
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await client.send_multipart(([b"server"] if peer else []) + [b"after-cancel"])
        _, body = await asyncio.wait_for(server.recv_from(), 5)
        assert body == [b"after-cancel"]
    finally:
        client.close(0)
        server.close(0)
        ctx.term()
    assert identity.peer_info.diagnostics.endpoint == endpoint.decode()


def test_shadow_recv_from():
    ctx = azmq.Context()
    server = ctx.socket(zmq.ROUTER)
    client = ctx.socket(zmq.DEALER)
    try:
        server.bind("inproc://shadow-from")
        client.identity = b"shadow"
        client.connect("inproc://shadow-from")
        shadow_server = zmq.Socket.shadow(server)
        shadow_client = zmq.Socket.shadow(client)
        shadow_client.send(b"request")
        identity, body = shadow_server.recv_from(copy=False)
        assert bytes(identity) == b"shadow"
        assert [bytes(part) for part in body] == [b"request"]
        shadow_server.send_to(identity, [b"reply"])
        assert shadow_client.recv() == b"reply"
        with pytest.raises(zmq.Again):
            shadow_server.recv_from(zmq.NOBLOCK)
    finally:
        client.close(0)
        server.close(0)
        ctx.term()
