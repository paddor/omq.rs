"""Wire-compat tests against pyzmq as the reference peer.

Each pattern runs both directions: pyomq listener <-> pyzmq dialer and
pyzmq listener <-> pyomq dialer. The test module is gated on pyzmq
being importable so the rest of the suite still runs without it.
"""

import time

import pytest

zmq_pyzmq = pytest.importorskip("zmq")  # pyzmq

import pyomq


def _settle():
    # PUB/SUB has no handshake; brief settle for prefix-filter tests.
    time.sleep(0.2)


# ---------- PUSH / PULL ----------

def test_pyomq_push_pyzmq_pull(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    pull = py_ctx.socket(zmq_pyzmq.PULL)
    pull.bind(tcp_endpoint)
    try:
        ctx = pyomq.Context()
        push = ctx.socket(pyomq.PUSH)
        push.connect(tcp_endpoint)
        push.send(b"from-pyomq")
        assert pull.recv() == b"from-pyomq"
        push.close()
        ctx.term()
    finally:
        pull.close()


def test_pyzmq_push_pyomq_pull(tcp_endpoint):
    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    pull.bind(tcp_endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        push = py_ctx.socket(zmq_pyzmq.PUSH)
        push.connect(tcp_endpoint)
        push.send(b"from-pyzmq")
        assert pull.recv() == b"from-pyzmq"
        push.close()
    finally:
        pull.close()
        ctx.term()


# ---------- PUB / SUB ----------

def test_pyomq_pub_pyzmq_sub(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    sub = py_ctx.socket(zmq_pyzmq.SUB)
    sub.setsockopt(zmq_pyzmq.SUBSCRIBE, b"hot/")
    sub.connect(tcp_endpoint)
    try:
        ctx = pyomq.Context()
        pub = ctx.socket(pyomq.PUB)
        pub.bind(tcp_endpoint)
        _settle()
        pub.send(b"cold/skip")
        pub.send(b"hot/take")
        sub.setsockopt(zmq_pyzmq.RCVTIMEO, 1000)
        assert sub.recv() == b"hot/take"
        pub.close()
        ctx.term()
    finally:
        sub.close()


def test_pyzmq_pub_pyomq_sub(tcp_endpoint):
    ctx = pyomq.Context()
    sub = ctx.socket(pyomq.SUB)
    sub.setsockopt(pyomq.SUBSCRIBE, b"hot/")
    sub.connect(tcp_endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        pub = py_ctx.socket(zmq_pyzmq.PUB)
        pub.bind(tcp_endpoint)
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

def test_pyomq_req_pyzmq_rep(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    rep = py_ctx.socket(zmq_pyzmq.REP)
    rep.bind(tcp_endpoint)
    try:
        ctx = pyomq.Context()
        req = ctx.socket(pyomq.REQ)
        req.connect(tcp_endpoint)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
        req.close()
        ctx.term()
    finally:
        rep.close()


def test_pyzmq_req_pyomq_rep(tcp_endpoint):
    ctx = pyomq.Context()
    rep = ctx.socket(pyomq.REP)
    rep.bind(tcp_endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        req = py_ctx.socket(zmq_pyzmq.REQ)
        req.connect(tcp_endpoint)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
        req.close()
    finally:
        rep.close()
        ctx.term()


# ---------- DEALER / ROUTER ----------

def test_pyomq_dealer_pyzmq_router(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    router = py_ctx.socket(zmq_pyzmq.ROUTER)
    router.bind(tcp_endpoint)
    try:
        ctx = pyomq.Context()
        dealer = ctx.socket(pyomq.DEALER)
        dealer.setsockopt(pyomq.IDENTITY, b"D")
        dealer.connect(tcp_endpoint)
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


def test_pyzmq_dealer_pyomq_router(tcp_endpoint):
    ctx = pyomq.Context()
    router = ctx.socket(pyomq.ROUTER)
    router.bind(tcp_endpoint)
    try:
        py_ctx = zmq_pyzmq.Context.instance()
        dealer = py_ctx.socket(zmq_pyzmq.DEALER)
        dealer.setsockopt(zmq_pyzmq.IDENTITY, b"D")
        dealer.connect(tcp_endpoint)
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

def test_pair_both_directions(tcp_endpoint):
    py_ctx = zmq_pyzmq.Context.instance()
    a = py_ctx.socket(zmq_pyzmq.PAIR)
    a.bind(tcp_endpoint)
    try:
        ctx = pyomq.Context()
        b = ctx.socket(pyomq.PAIR)
        b.connect(tcp_endpoint)
        a.send(b"hi-from-pyzmq")
        assert b.recv() == b"hi-from-pyzmq"
        b.send(b"hi-from-pyomq")
        assert a.recv() == b"hi-from-pyomq"
        b.close()
        ctx.term()
    finally:
        a.close()
