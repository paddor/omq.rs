"""PUSH/PULL across inproc and tcp; multipart shape."""

import pyomq as zmq


def _push_pull(endpoint: str) -> None:
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.bind(endpoint)
        push.connect(endpoint)
        push.send(b"hello")
        assert pull.recv() == b"hello"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_push_pull_inproc(inproc_endpoint):
    _push_pull(inproc_endpoint)


def test_push_pull_tcp(tcp_endpoint):
    _push_pull(tcp_endpoint)


def test_push_pull_multipart(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_multipart([b"meta", b"trailer"])
        assert pull.recv_multipart() == [b"meta", b"trailer"]
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_sndmore_flag_aggregates(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send(b"a", flags=zmq.SNDMORE)
        push.send(b"b", flags=zmq.SNDMORE)
        push.send(b"c")
        assert pull.recv_multipart() == [b"a", b"b", b"c"]
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_rcvmore_iterates_frames(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_multipart([b"x", b"y", b"z"])
        assert pull.recv() == b"x"
        assert pull.getsockopt(zmq.RCVMORE) == 1
        assert pull.recv() == b"y"
        assert pull.getsockopt(zmq.RCVMORE) == 1
        assert pull.recv() == b"z"
        assert pull.getsockopt(zmq.RCVMORE) == 0
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_context_manager_protocol(tcp_endpoint):
    with zmq.Context() as ctx, ctx.socket(zmq.PAIR) as a, ctx.socket(zmq.PAIR) as b:
        a.bind(tcp_endpoint)
        b.connect(tcp_endpoint)
        a.send(b"ping")
        assert b.recv() == b"ping"
        b.send(b"pong")
        assert a.recv() == b"pong"
