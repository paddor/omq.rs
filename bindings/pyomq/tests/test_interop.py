"""Wire-compat tests against pyzmq as the reference peer.

Each pattern runs both directions: pyomq listener <-> pyzmq dialer and
pyzmq listener <-> pyomq dialer. Cells run over TCP and IPC. Inproc is
intentionally absent: pyzmq's inproc and pyomq's inproc each maintain
their own process-local registry, so they can never see each other.
"""

import sys
import time

import pytest

zmq_pyzmq = pytest.importorskip("zmq")  # pyzmq

# PyZMQ requires to use the selector event loop on Windows,
# so we skip the proactor event loop for these tests.
pytestmark = pytest.mark.event_loop("selector")

import pyomq


def _settle():
    # PUB/SUB has no handshake; brief settle for prefix-filter tests.
    time.sleep(0.2)


# Each pattern test takes an `endpoint` string. Parametrising with two
# fixtures via indirect=True keeps the body transport-agnostic.
@pytest.fixture
def endpoint(request):
    return request.getfixturevalue(request.param)


_TRANSPORTS = pytest.mark.parametrize(
    "endpoint",
    # pyzmq does not support IPC on Windows, so skip that transport there.
    ["tcp_endpoint"] if sys.platform == "win32" else ["tcp_endpoint", "ipc_endpoint"],
    indirect=True,
)


# ---------- PUSH / PULL ----------


@_TRANSPORTS
def test_pyomq_push_pyzmq_pull(endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    pull = py_ctx.socket(zmq_pyzmq.PULL)
    pull.bind(endpoint)
    ep = (
        pull.last_endpoint.decode()
        if isinstance(pull.last_endpoint, bytes)
        else pull.last_endpoint
    )
    try:
        ctx = pyomq.Context()
        push = ctx.socket(pyomq.PUSH)
        push.connect(ep)
        push.send(b"from-pyomq")
        assert pull.recv() == b"from-pyomq"
        push.close()
        ctx.term()
    finally:
        pull.close()


@_TRANSPORTS
def test_pyzmq_push_pyomq_pull(endpoint):
    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    ep = pull.bind(endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        push = py_ctx.socket(zmq_pyzmq.PUSH)
        push.connect(ep)
        push.send(b"from-pyzmq")
        assert pull.recv() == b"from-pyzmq"
        push.close()
    finally:
        pull.close()
        ctx.term()


# ---------- PUB / SUB ----------


@_TRANSPORTS
def test_pyomq_pub_pyzmq_sub(endpoint):
    ctx = pyomq.Context()
    pub = ctx.socket(pyomq.PUB)
    ep = pub.bind(endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        sub = py_ctx.socket(zmq_pyzmq.SUB)
        sub.setsockopt(zmq_pyzmq.SUBSCRIBE, b"hot/")
        sub.connect(ep)
        _settle()
        pub.send(b"cold/skip")
        pub.send(b"hot/take")
        sub.setsockopt(zmq_pyzmq.RCVTIMEO, 1000)
        assert sub.recv() == b"hot/take"
        pub.close()
        ctx.term()
    finally:
        sub.close()


@_TRANSPORTS
def test_pyzmq_pub_pyomq_sub(endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    pub = py_ctx.socket(zmq_pyzmq.PUB)
    pub.bind(endpoint)
    ep = (
        pub.last_endpoint.decode()
        if isinstance(pub.last_endpoint, bytes)
        else pub.last_endpoint
    )
    try:
        ctx = pyomq.Context()
        sub = ctx.socket(pyomq.SUB)
        sub.setsockopt(pyomq.SUBSCRIBE, b"hot/")
        sub.connect(ep)
        _settle()
        pub.send(b"cold/skip")
        pub.send(b"hot/take")
        sub.setsockopt(pyomq.RCVTIMEO, 1000)
        assert sub.recv() == b"hot/take"
        pub.close()
    finally:
        sub.close()
        ctx.term()


# ---------- REQ / REP ----------


@_TRANSPORTS
def test_pyomq_req_pyzmq_rep(endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    rep = py_ctx.socket(zmq_pyzmq.REP)
    rep.bind(endpoint)
    ep = (
        rep.last_endpoint.decode()
        if isinstance(rep.last_endpoint, bytes)
        else rep.last_endpoint
    )
    try:
        ctx = pyomq.Context()
        req = ctx.socket(pyomq.REQ)
        req.connect(ep)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
        req.close()
        ctx.term()
    finally:
        rep.close()


@_TRANSPORTS
def test_pyzmq_req_pyomq_rep(endpoint):
    ctx = pyomq.Context()
    rep = ctx.socket(pyomq.REP)
    ep = rep.bind(endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        req = py_ctx.socket(zmq_pyzmq.REQ)
        req.connect(ep)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
        req.close()
    finally:
        rep.close()
        ctx.term()


# ---------- DEALER / ROUTER ----------


@_TRANSPORTS
def test_pyomq_dealer_pyzmq_router(endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    router = py_ctx.socket(zmq_pyzmq.ROUTER)
    router.bind(endpoint)
    ep = (
        router.last_endpoint.decode()
        if isinstance(router.last_endpoint, bytes)
        else router.last_endpoint
    )
    try:
        ctx = pyomq.Context()
        dealer = ctx.socket(pyomq.DEALER)
        dealer.setsockopt(pyomq.IDENTITY, b"D")
        dealer.connect(ep)
        dealer.send(b"hi")
        parts = router.recv_multipart()
        assert parts[0] == b"D"
        assert parts[-1] == b"hi"
        router.send_multipart([b"D", b"back"])
        assert dealer.recv() == b"back"
        dealer.close()
        ctx.term()
    finally:
        router.close()


@_TRANSPORTS
def test_pyzmq_dealer_pyomq_router(endpoint):
    ctx = pyomq.Context()
    router = ctx.socket(pyomq.ROUTER)
    ep = router.bind(endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        dealer = py_ctx.socket(zmq_pyzmq.DEALER)
        dealer.setsockopt(zmq_pyzmq.IDENTITY, b"D")
        dealer.connect(ep)
        dealer.send(b"hi")
        parts = router.recv_multipart()
        assert parts[0] == b"D"
        assert parts[-1] == b"hi"
        router.send_multipart([b"D", b"back"])
        assert dealer.recv() == b"back"
        dealer.close()
    finally:
        router.close()
        ctx.term()


# ---------- PAIR ----------


@_TRANSPORTS
def test_pair_both_directions(endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    a = py_ctx.socket(zmq_pyzmq.PAIR)
    a.bind(endpoint)
    ep = (
        a.last_endpoint.decode()
        if isinstance(a.last_endpoint, bytes)
        else a.last_endpoint
    )
    try:
        ctx = pyomq.Context()
        b = ctx.socket(pyomq.PAIR)
        b.connect(ep)
        a.send(b"hi-from-pyzmq")
        assert b.recv() == b"hi-from-pyzmq"
        b.send(b"hi-from-pyomq")
        assert a.recv() == b"hi-from-pyomq"
        b.close()
        ctx.term()
    finally:
        a.close()


# ---------- XPUB / XSUB ----------


@_TRANSPORTS
def test_pyomq_xpub_pyzmq_xsub(endpoint):
    """XPUB receives subscribes from XSUB and filters its publishes
    accordingly. Exercises both ZMTP 3.1 SUBSCRIBE commands and the
    legacy 3.0 0x01-prefix message form pyzmq XSUB emits."""
    ctx = pyomq.Context()
    xpub = ctx.socket(pyomq.XPUB)
    ep = xpub.bind(endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        xsub = py_ctx.socket(zmq_pyzmq.XSUB)
        xsub.connect(ep)
        xsub.send(b"\x01hot/")  # legacy ZMTP 3.0 subscribe
        # XPUB surfaces the subscribe as a 0x01-prefixed message.
        xpub.setsockopt(pyomq.RCVTIMEO, 1000)
        sub_msg = xpub.recv()
        assert sub_msg == b"\x01hot/"
        xsub.setsockopt(zmq_pyzmq.RCVTIMEO, 1000)
        for _ in range(20):
            xpub.send(b"cold/skip")
            xpub.send(b"hot/take")
            try:
                assert xsub.recv() == b"hot/take"
                break
            except zmq_pyzmq.Again:
                time.sleep(0.05)
        else:
            pytest.fail("XSUB never received hot/take")
        xsub.close()
    finally:
        xpub.close()
        ctx.term()
