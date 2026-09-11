"""ZMQStream (tornado IOLoop integration) tests."""

import asyncio
import time

import pytest

tornado = pytest.importorskip("tornado")

import pyomq as zmq
from pyomq.zmqstream import ZMQStream


def test_zmqstream_on_recv_callback(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)

        stream = ZMQStream(pull)
        received = []
        stream.on_recv(lambda msg: received.append(msg))

        push.send(b"hello")
        time.sleep(0.2)
        stream.flush()

        assert len(received) >= 1
        assert received[0] == [b"hello"]
    finally:
        stream.close()
        push.close()
        pull.close()
        ctx.term()


def test_zmqstream_stop_on_recv(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)

        stream = ZMQStream(pull)
        received = []
        stream.on_recv(lambda msg: received.append(msg))
        stream.stop_on_recv()

        push.send(b"ignored")
        time.sleep(0.1)
        stream.flush()

        assert len(received) == 0
    finally:
        stream.close()
        push.close()
        pull.close()
        ctx.term()


def test_zmqstream_send_forwards(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)

        stream = ZMQStream(push)
        stream.send(b"via-stream")

        msg = pull.recv()
        assert msg == b"via-stream"
    finally:
        stream.close()
        push.close()
        pull.close()
        ctx.term()


def test_zmqstream_send_multipart_forwards(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        ep = pull.last_endpoint
        push.connect(ep)

        stream = ZMQStream(push)
        stream.send_multipart([b"a", b"b", b"c"])

        parts = pull.recv_multipart()
        assert parts == [b"a", b"b", b"c"]
    finally:
        stream.close()
        push.close()
        pull.close()
        ctx.term()


def test_zmqstream_closed_is_property():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        stream = ZMQStream(sock)
        assert stream.closed is False
        stream.close()
        assert stream.closed is True
    finally:
        sock.close()
        ctx.term()


def test_zmqstream_setsockopt_getsockopt():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        stream = ZMQStream(sock)
        stream.setsockopt(zmq.LINGER, 500)
        assert stream.getsockopt(zmq.LINGER) == 500
    finally:
        stream.close()
        sock.close()
        ctx.term()
