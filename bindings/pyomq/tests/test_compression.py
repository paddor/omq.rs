"""Compression transport smoke tests."""

import pytest

import pyomq as zmq

pytestmark = pytest.mark.skipif(not zmq.has("zstd"), reason="zstd feature not compiled")

ZSTD_DICT = bytes.fromhex(
    "37a430ecbeaadd5c811120841042664644444444244902002114c418638c21841042"
    "082184104208214444444444444444240900005110638c31c618630c21c418636666"
    "864692040080000000c000000000010000"
)


def _zstd(endpoint: str) -> str:
    return endpoint.replace("tcp://", "zstd+tcp://", 1)


def _payload(seq: int, size: int = 1024) -> bytes:
    head = f'{{"kind":"quote","symbol":"OMQ","seq":{seq},"pad":"'.encode()
    tail = b'"}'
    return head + (b"A" * (size - len(head) - len(tail))) + tail


def test_zstd_push_pull_custom_level(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.rcvtimeo = 2000
        push.compression_level = 1
        assert push.compression_level == 1
        ep = pull.bind(_zstd(tcp_endpoint))
        push.connect(ep)
        msg = _payload(1, 4096)
        push.send(msg)
        assert pull.recv() == msg
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_zstd_push_pull_static_dict(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.rcvtimeo = 2000
        push.compression_level = 1
        push.compression_dict = ZSTD_DICT
        assert push.compression_dict == ZSTD_DICT
        ep = pull.bind(_zstd(tcp_endpoint))
        push.connect(ep)
        msg = _payload(2)
        push.send(msg)
        assert pull.recv() == msg
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_zstd_push_pull_auto_train(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.rcvtimeo = 2000
        push.compression_auto_train = 1
        assert push.compression_auto_train == 1
        ep = pull.bind(_zstd(tcp_endpoint))
        push.connect(ep)
        messages = [_payload(i) for i in range(130)]
        for msg in messages:
            push.send(msg)
        assert [pull.recv() for _ in messages] == messages
    finally:
        push.close()
        pull.close()
        ctx.term()
