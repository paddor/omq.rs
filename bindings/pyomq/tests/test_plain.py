"""PLAIN mechanism socket tests."""

import time

import pytest

import pyomq as zmq


def _plain_server_client(server_sock, client_sock):
    server_sock.plain_server = 1
    server_sock.set_plain_auth([("alice", "secret"), ("bob", "hunter2")])
    client_sock.plain_username = b"alice"
    client_sock.plain_password = b"secret"


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_push_pull_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _plain_server_client(pull, push)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"hello over plain")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"hello over plain"
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_fixed_policy_accepts_each_allowlist_entry(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.plain_server = 1
        pull.set_plain_auth([("alice", "secret"), ("bob", "hunter2")])
        push.plain_username = b"bob"
        push.plain_password = b"hunter2"
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"second credential")
        pull.rcvtimeo = 5000
        assert pull.recv() == b"second credential"
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
@pytest.mark.parametrize(
    "credentials",
    [
        [("missing-password",)],
        [("has space", "secret")],
        [("alice", "line\nbreak")],
        [("alice", "\N{LATIN SMALL LETTER E WITH ACUTE}")],
        [("x" * 256, "secret")],
    ],
)
def test_plain_fixed_policy_validates_credentials(credentials):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        pull.plain_server = 1
        with pytest.raises((TypeError, ValueError)):
            pull.set_plain_auth(credentials)
    finally:
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_fixed_policy_rejects_wrong_password(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.plain_server = 1
        pull.set_plain_auth([("alice", "secret")])
        push.plain_username = b"alice"
        push.plain_password = b"wrong"
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"blocked")
        pull.rcvtimeo = 300
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_server_requires_auth_policy(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        pull.plain_server = 1
        with pytest.raises(ValueError, match="explicit authentication"):
            pull.bind(tcp_endpoint)
    finally:
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_policy_cannot_change_after_socket_use(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        pull.plain_server = 1
        pull.set_plain_auth([("alice", "secret")])
        pull.bind(tcp_endpoint)
        with pytest.raises(ValueError, match="before socket use"):
            pull.set_plain_auth([("mallory", "wrong")])
    finally:
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_server_callback_receives_credentials(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    seen = []

    def authenticate(peer):
        seen.append((peer.username, peer.password, peer.peer_address))
        return peer.username == "alice" and peer.password == "secret"

    try:
        pull.plain_server = 1
        pull.set_plain_auth(authenticate)
        push.plain_username = b"alice"
        push.plain_password = b"secret"
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"authenticated")
        pull.rcvtimeo = 5000
        assert pull.recv() == b"authenticated"
        assert seen == [("alice", "secret", "127.0.0.1")]
    finally:
        push.close()
        pull.close()
        ctx.term()


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_req_rep_tcp(tcp_endpoint):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        _plain_server_client(rep, req)
        ep = rep.bind(tcp_endpoint)
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


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_pub_sub_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    sub = ctx.socket(zmq.SUB)
    try:
        _plain_server_client(pub, sub)
        ep = pub.bind(tcp_endpoint)
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


@pytest.mark.skipif(not zmq.has("plain"), reason="plain feature not compiled")
def test_plain_multipart_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _plain_server_client(pull, push)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_multipart([b"a", b"bb", b"ccc"])
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv_multipart() == [b"a", b"bb", b"ccc"]
    finally:
        push.close()
        pull.close()
        ctx.term()
