"""CURVE interop tests against pyzmq as the reference peer.

Both directions: pyomq CURVE server <-> pyzmq CURVE client and vice versa.
pyzmq's CURVE server requires a ZAP authenticator; we use CURVE_ALLOW_ANY
to accept any client with a valid handshake.
"""

import asyncio
import sys

import pytest

zmq_pyzmq = pytest.importorskip("zmq")

# PyZMQ requires to use the selector event loop on Windows,
# so we skip the proactor event loop for these tests.
pytestmark = pytest.mark.event_loop("selector")


from zmq.auth.thread import ThreadAuthenticator

import pyomq

_skip_no_curve = pytest.mark.skipif(
    not pyomq.has("curve"), reason="curve feature not compiled"
)


# ── pyomq server, pyzmq client ──────────────────────────────────────


@_skip_no_curve
def test_pyomq_curve_server_pyzmq_curve_client_push_pull(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = zmq_pyzmq.curve_keypair()

    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    pull.curve_server = 1
    pull.curve_publickey = server_pub
    pull.curve_secretkey = server_sec
    ep = pull.bind(tcp_endpoint)

    py_ctx = zmq_pyzmq.Context.instance()
    push = py_ctx.socket(zmq_pyzmq.PUSH)
    push.curve_serverkey = server_pub
    push.curve_publickey = client_pub
    push.curve_secretkey = client_sec
    push.connect(ep)
    try:
        push.send(b"from-pyzmq-curve")
        pull.setsockopt(pyomq.RCVTIMEO, 5000)
        assert pull.recv() == b"from-pyzmq-curve"
    finally:
        push.close()
        pull.close()
        ctx.term()


@_skip_no_curve
def test_pyomq_curve_server_pyzmq_curve_client_req_rep(tcp_endpoint):
    server_pub, server_sec = pyomq.curve_keypair()
    client_pub, client_sec = zmq_pyzmq.curve_keypair()

    ctx = pyomq.Context()
    rep = ctx.socket(pyomq.REP)
    rep.curve_server = 1
    rep.curve_publickey = server_pub
    rep.curve_secretkey = server_sec
    ep = rep.bind(tcp_endpoint)

    py_ctx = zmq_pyzmq.Context.instance()
    req = py_ctx.socket(zmq_pyzmq.REQ)
    req.curve_serverkey = server_pub
    req.curve_publickey = client_pub
    req.curve_secretkey = client_sec
    req.connect(ep)
    try:
        req.send(b"ping")
        rep.setsockopt(pyomq.RCVTIMEO, 5000)
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        req.setsockopt(zmq_pyzmq.RCVTIMEO, 5000)
        assert req.recv() == b"pong"
    finally:
        req.close()
        rep.close()
        ctx.term()


# ── pyzmq server, pyomq client ──────────────────────────────────────


@_skip_no_curve
def test_pyzmq_curve_server_pyomq_curve_client_push_pull(tcp_endpoint):
    if sys.platform == "win32":
        with pytest.warns(DeprecationWarning):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    server_pub, server_sec = zmq_pyzmq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    py_ctx = zmq_pyzmq.Context()
    auth = ThreadAuthenticator(py_ctx)
    auth.start()
    auth.configure_curve(domain="*", location=zmq_pyzmq.auth.CURVE_ALLOW_ANY)

    pull = py_ctx.socket(zmq_pyzmq.PULL)
    pull.curve_server = True
    pull.curve_publickey = server_pub
    pull.curve_secretkey = server_sec
    pull.bind(tcp_endpoint)
    ep = pull.last_endpoint.decode()

    ctx = pyomq.Context()
    push = ctx.socket(pyomq.PUSH)
    push.curve_serverkey = server_pub
    push.curve_publickey = client_pub
    push.curve_secretkey = client_sec
    push.connect(ep)
    try:
        push.send(b"from-pyomq-curve")
        pull.setsockopt(zmq_pyzmq.RCVTIMEO, 5000)
        assert pull.recv() == b"from-pyomq-curve"
    finally:
        push.close()
        ctx.term()
        pull.close()
        auth.stop()
        py_ctx.term()


@_skip_no_curve
def test_pyzmq_curve_server_pyomq_curve_client_req_rep(tcp_endpoint):
    if sys.platform == "win32":
        with pytest.warns(DeprecationWarning):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    server_pub, server_sec = zmq_pyzmq.curve_keypair()
    client_pub, client_sec = pyomq.curve_keypair()

    py_ctx = zmq_pyzmq.Context()
    auth = ThreadAuthenticator(py_ctx)
    auth.start()
    auth.configure_curve(domain="*", location=zmq_pyzmq.auth.CURVE_ALLOW_ANY)

    rep = py_ctx.socket(zmq_pyzmq.REP)
    rep.curve_server = True
    rep.curve_publickey = server_pub
    rep.curve_secretkey = server_sec
    rep.bind(tcp_endpoint)
    ep = rep.last_endpoint.decode()

    ctx = pyomq.Context()
    req = ctx.socket(pyomq.REQ)
    req.curve_serverkey = server_pub
    req.curve_publickey = client_pub
    req.curve_secretkey = client_sec
    req.connect(ep)
    try:
        req.send(b"ping")
        req.setsockopt(pyomq.RCVTIMEO, 5000)
        rep.setsockopt(zmq_pyzmq.RCVTIMEO, 5000)
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
    finally:
        req.close()
        ctx.term()
        rep.close()
        auth.stop()
        py_ctx.term()
