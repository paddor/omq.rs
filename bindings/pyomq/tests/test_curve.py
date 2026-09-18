"""CURVE keypair generation and encrypted socket tests."""

import time

import pyomq as zmq
import pytest


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_keypair_returns_two_z85_bytes():
    pub, sec = zmq.curve_keypair()
    assert isinstance(pub, bytes)
    assert isinstance(sec, bytes)
    assert len(pub) == 40
    assert len(sec) == 40


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_keypair_unique():
    pub1, sec1 = zmq.curve_keypair()
    pub2, sec2 = zmq.curve_keypair()
    assert pub1 != pub2
    assert sec1 != sec2


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_derives_from_secret():
    pub_orig, sec = zmq.curve_keypair()
    pub_derived = zmq.curve_public(sec)
    assert pub_derived == pub_orig


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_accepts_str():
    pub_orig, sec = zmq.curve_keypair()
    pub_derived = zmq.curve_public(sec.decode("ascii"))
    assert pub_derived == pub_orig


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_bad_z85_raises():
    with pytest.raises(ValueError):
        zmq.curve_public(b"not-valid-z85-key")


# ── Encrypted socket tests ──────────────────────────────────────────


def _curve_server_client(server_sock, client_sock):
    server_pub, server_sec = zmq.curve_keypair()
    client_pub, client_sec = zmq.curve_keypair()

    server_sock.curve_server = 1
    server_sock.curve_publickey = server_pub
    server_sock.curve_secretkey = server_sec

    client_sock.curve_serverkey = server_pub
    client_sock.curve_publickey = client_pub
    client_sock.curve_secretkey = client_sec


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_option_round_trip():
    pub, sec = zmq.curve_keypair()
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    try:
        s.curve_server = 1
        assert s.getsockopt(zmq.CURVE_SERVER) == 1
        s.curve_publickey = pub
        assert s.getsockopt(zmq.CURVE_PUBLICKEY) == pub
        s.curve_secretkey = sec
        assert s.getsockopt(zmq.CURVE_SECRETKEY) == sec
        s.curve_serverkey = pub
        assert s.getsockopt(zmq.CURVE_SERVERKEY) == pub
    finally:
        s.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_push_pull_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _curve_server_client(pull, push)
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        push.send(b"hello over curve")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"hello over curve"
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_req_rep_tcp(tcp_endpoint):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        _curve_server_client(rep, req)
        rep.bind(tcp_endpoint)
        ep = rep.last_endpoint
        req.connect(ep)
        req.setsockopt(zmq.SNDTIMEO, 5000)
        rep.setsockopt(zmq.RCVTIMEO, 5000)
        req.setsockopt(zmq.RCVTIMEO, 5000)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
    finally:
        req.close()
        rep.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_pub_sub_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    sub = ctx.socket(zmq.SUB)
    try:
        _curve_server_client(pub, sub)
        pub.bind(tcp_endpoint)
        ep = pub.last_endpoint
        sub.setsockopt(zmq.SUBSCRIBE, b"hot/")
        sub.connect(ep)
        sub.setsockopt(zmq.RCVTIMEO, 5000)
        time.sleep(0.3)
        pub.send(b"cold/skip")
        pub.send(b"hot/take")
        assert sub.recv() == b"hot/take"
    finally:
        sub.close()
        pub.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_multipart_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _curve_server_client(pull, push)
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)
        push.send_multipart([b"a", b"bb", b"ccc"])
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv_multipart() == [b"a", b"bb", b"ccc"]
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_bad_serverkey_rejects(tcp_endpoint):
    server_pub, server_sec = zmq.curve_keypair()
    wrong_pub, _ = zmq.curve_keypair()
    client_pub, client_sec = zmq.curve_keypair()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.curve_server = 1
        pull.curve_publickey = server_pub
        pull.curve_secretkey = server_sec
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint

        push.curve_serverkey = wrong_pub
        push.curve_publickey = client_pub
        push.curve_secretkey = client_sec
        push.connect(ep)

        push.send(b"should not arrive")
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        push.close()
        pull.close()
        ctx.term()
