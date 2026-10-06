"""Wire regressions using a raw peer, independent of the native encoder."""

import socket
import struct
from contextlib import contextmanager

import pyomq as zmq
import pytest


def _frame(flags, body):
    return bytes([flags, len(body)]) + body


def _command(name, body=b""):
    return _frame(4, bytes([len(name)]) + name + body)


def _exact(stream, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = stream.recv(size - len(chunks))
        if not chunk:
            raise EOFError("peer closed")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_frame(stream):
    flags = _exact(stream, 1)[0]
    size = int.from_bytes(_exact(stream, 8 if flags & 2 else 1), "big")
    assert size < 4096
    return flags, _exact(stream, size)


@contextmanager
def _raw_peer(kind, peer_type, version=(3, 1)):
    ctx = zmq.Context()
    local = ctx.socket(kind)
    local.linger = 0
    local.rcvtimeo = 2000
    local.bind("tcp://127.0.0.1:0")
    host, port = local.last_endpoint.decode().removeprefix("tcp://").split(":")
    raw = socket.create_connection((host, int(port)), timeout=2)
    try:
        greeting = bytearray(64)
        greeting[0], greeting[9] = 255, 127
        greeting[10], greeting[11] = version
        greeting[12:16] = b"NULL"
        props = b"\x0bSocket-Type" + struct.pack("!I", len(peer_type)) + peer_type
        raw.sendall(bytes(greeting) + _command(b"READY", props))
        assert _exact(raw, 64)[10:12] == b"\x03\x01"
        assert _read_frame(raw)[1].startswith(b"\x05READY")
        yield local, raw
    finally:
        raw.close()
        local.close()
        ctx.term()


@pytest.mark.parametrize(
    "kind,peer_type",
    [
        (zmq.CLIENT, b"SERVER"),
        (zmq.SERVER, b"CLIENT"),
        (zmq.GATHER, b"SCATTER"),
        (zmq.CHANNEL, b"CHANNEL"),
    ],
)
def test_single_frame_receive_discards_entire_multipart(kind, peer_type):
    with _raw_peer(kind, peer_type) as (local, raw):
        raw.sendall(_frame(1, b"first") + _frame(0, b"last") + _frame(0, b"valid"))
        assert local.recv_multipart() == [b"valid"]
        raw.sendall(_frame(0, b"next"))
        assert local.recv() == b"next"


@pytest.mark.parametrize("version", [(3, 0), (3, 1), (4, 0)])
def test_subscription_wire_encoding_matches_peer_version(version):
    with _raw_peer(zmq.SUB, b"PUB", version) as (local, raw):
        local.subscribe(b"topic")
        subscribe = _read_frame(raw)
        local.unsubscribe(b"topic")
        cancel = _read_frame(raw)
        if version == (3, 0):
            assert subscribe == (0, b"\x01topic")
            assert cancel == (0, b"\x00topic")
        else:
            assert subscribe == (4, b"\x09SUBSCRIBEtopic")
            assert cancel == (4, b"\x06CANCELtopic")


@pytest.mark.parametrize("transport", ["tcp", "inproc"])
@pytest.mark.parametrize("prefix", [b"", b"topic"])
def test_duplicate_subscription_retains_one_copy(transport, prefix):
    ctx = zmq.Context()
    pub, sub = ctx.socket(zmq.XPUB), ctx.socket(zmq.SUB)
    pub.linger = sub.linger = 0
    pub.rcvtimeo = sub.rcvtimeo = 2000
    try:
        sub.subscribe(prefix)
        sub.subscribe(prefix)
        endpoint = (
            "tcp://127.0.0.1:0" if transport == "tcp" else "inproc://duplicate-sub"
        )
        pub.bind(endpoint)
        sub.connect(pub.last_endpoint)
        # Notifications form a barrier after publisher matching has been updated.
        assert pub.recv() == b"\x01" + prefix
        assert pub.recv() == b"\x01" + prefix
        sub.unsubscribe(prefix)
        assert pub.recv() == b"\x00" + prefix
        pub.send(b"topic/retained")
        assert sub.recv() == b"topic/retained"
        sub.unsubscribe(prefix)
        assert pub.recv() == b"\x00" + prefix
        pub.send(b"topic/dropped")
        sub.rcvtimeo = 50
        with pytest.raises(zmq.Again):
            sub.recv()
    finally:
        sub.close()
        pub.close()
        ctx.term()


def test_peer_ping_ttl_closes_silent_connection():
    with _raw_peer(zmq.PAIR, b"PAIR") as (_, raw):
        raw.sendall(_command(b"PING", b"\x00\x01ctx"))
        assert _read_frame(raw) == (4, b"\x04PONGctx")
        assert raw.recv(1) == b""
