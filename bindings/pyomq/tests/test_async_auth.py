"""Async API authentication tests for CURVE."""

import asyncio

import pytest

import pyomq
import pyomq.asyncio as zmq_async


# ── CURVE ────────────────────────────────────────────────────────────


@pytest.mark.skipif(not pyomq.has("curve"), reason="curve feature not compiled")
@pytest.mark.asyncio
async def test_async_curve_auth_allowed_keys(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.curve_server = 1
        pull.curve_publickey = server_pub
        pull.curve_secretkey = server_sec
        pull.set_curve_auth([client_pub])

        push.curve_serverkey = server_pub
        push.curve_publickey = client_pub
        push.curve_secretkey = client_sec

        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"async-curve-ok")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-curve-ok"
    finally:
        push.close()
        pull.close()


@pytest.mark.skipif(not pyomq.has("curve"), reason="curve feature not compiled")
@pytest.mark.asyncio
async def test_async_curve_auth_callback(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    ctx = zmq_async.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    try:
        pull.curve_server = 1
        pull.curve_publickey = server_pub
        pull.curve_secretkey = server_sec
        pull.set_curve_auth(lambda peer: peer.public_key == client_pub)

        push.curve_serverkey = server_pub
        push.curve_publickey = client_pub
        push.curve_secretkey = client_sec

        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"async-curve-cb")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert await pull.recv() == b"async-curve-cb"
    finally:
        push.close()
        pull.close()


@pytest.mark.skipif(not pyomq.has("curve"), reason="curve feature not compiled")
@pytest.mark.asyncio
async def test_async_curve_auth_callback_receives_identity(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()
    captured = []

    ctx = zmq_async.Context()
    router = ctx.socket(pyomq.ROUTER)
    dealer = ctx.socket(pyomq.DEALER)
    try:
        router.curve_server = 1
        router.curve_publickey = server_pub
        router.curve_secretkey = server_sec
        router.set_curve_auth(
            lambda peer: (
                captured.append(peer.identity) is None
                and peer.public_key == client_pub
                and peer.identity == b"async-client"
            )
        )

        dealer.identity = b"async-client"
        dealer.curve_serverkey = server_pub
        dealer.curve_publickey = client_pub
        dealer.curve_secretkey = client_sec

        ep = router.bind(tcp_endpoint)
        dealer.connect(ep)
        dealer.send(b"async-probe")
        msg = await asyncio.wait_for(router.recv_multipart(), timeout=5.0)
        assert msg == [b"async-client", b"async-probe"]
        assert captured == [b"async-client"]
    finally:
        dealer.close()
        router.close()
