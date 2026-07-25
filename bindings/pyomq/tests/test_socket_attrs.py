"""Socket attribute-style option access (pyzmq compat)."""

import pytest

import pyomq as zmq


def test_linger_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.linger = 0
        assert sock.linger == 0
        sock.linger = 500
        assert sock.linger == 500
    finally:
        sock.close()
        ctx.term()


def test_sndhwm_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.sndhwm = 100
        assert sock.sndhwm == 100
    finally:
        sock.close()
        ctx.term()


def test_rcvhwm_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.rcvhwm = 200
        assert sock.rcvhwm == 200
    finally:
        sock.close()
        ctx.term()


def test_identity_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.identity = b"myid"
        assert sock.identity == b"myid"
    finally:
        sock.close()
        ctx.term()


def test_rcvtimeo_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.rcvtimeo = 1000
        assert sock.rcvtimeo == 1000
    finally:
        sock.close()
        ctx.term()


def test_sndtimeo_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.sndtimeo = 2000
        assert sock.sndtimeo == 2000
    finally:
        sock.close()
        ctx.term()


def test_router_mandatory_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.ROUTER)
    try:
        sock.router_mandatory = 1
        assert sock.router_mandatory == 1
    finally:
        sock.close()
        ctx.term()


def test_conflate_attr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.conflate = 1
        assert sock.conflate == 1
    finally:
        sock.close()
        ctx.term()


def test_unknown_attr_raises():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        with pytest.raises(AttributeError):
            _ = sock.nonexistent_attribute  # type: ignore[ty:unresolved-attribute]
    finally:
        sock.close()
        ctx.term()


def test_private_attrs_work():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        assert sock._closed is False
        assert sock._context is ctx
    finally:
        sock.close()
        ctx.term()
