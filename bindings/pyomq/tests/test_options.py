"""setsockopt / getsockopt round-trip + behaviour for Group A, B, C."""

import time

import pytest

import pyomq as zmq


def _push():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    return ctx, s


# Group A: direct value mapping.

def test_linger_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.LINGER, 50)
        assert s.getsockopt(zmq.LINGER) == 50
        s.setsockopt(zmq.LINGER, -1)  # forever
        assert s.getsockopt(zmq.LINGER) == -1
    finally:
        s.close()
        ctx.term()


def test_sndhwm_rcvhwm():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    r = ctx.socket(zmq.PULL)
    try:
        s.setsockopt(zmq.SNDHWM, 64)
        r.setsockopt(zmq.RCVHWM, 32)
        assert s.getsockopt(zmq.SNDHWM) == 64
        assert r.getsockopt(zmq.RCVHWM) == 32
    finally:
        s.close()
        r.close()
        ctx.term()


def test_identity_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.IDENTITY, b"my-id")
        assert s.getsockopt(zmq.IDENTITY) == b"my-id"
    finally:
        s.close()
        ctx.term()


def test_router_mandatory():
    ctx = zmq.Context()
    s = ctx.socket(zmq.ROUTER)
    try:
        s.setsockopt(zmq.ROUTER_MANDATORY, 1)
        assert s.getsockopt(zmq.ROUTER_MANDATORY) == 1
    finally:
        s.close()
        ctx.term()


def test_type_is_readonly():
    ctx, s = _push()
    try:
        assert s.getsockopt(zmq.TYPE) == zmq.PUSH
        with pytest.raises(zmq.ZMQError):
            s.setsockopt(zmq.TYPE, zmq.PULL)
    finally:
        s.close()
        ctx.term()


# Group B: wrapper-only.

def test_rcvtimeo_raises_eagain():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PULL)
    s.bind(f"inproc://timeout-{id(s)}")
    s.setsockopt(zmq.RCVTIMEO, 50)  # 50 ms
    try:
        t0 = time.monotonic()
        with pytest.raises(zmq.ZMQError) as excinfo:
            s.recv()
        elapsed = time.monotonic() - t0
        assert excinfo.value.errno is not None
        # Timed out promptly (within an order of magnitude of the budget).
        assert elapsed < 1.0
    finally:
        s.close()
        ctx.term()


def test_immediate_and_ipv6_accepted_as_noops():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.IMMEDIATE, 1)
        s.setsockopt(zmq.IPV6, 0)
    finally:
        s.close()
        ctx.term()


# Group C: TCP keepalive.

def test_tcp_keepalive_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.TCP_KEEPALIVE, 1)
        s.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)
        s.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 5)
        s.setsockopt(zmq.TCP_KEEPALIVE_CNT, 3)
        assert s.getsockopt(zmq.TCP_KEEPALIVE) == 1
        assert s.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 30
        assert s.getsockopt(zmq.TCP_KEEPALIVE_INTVL) == 5
        assert s.getsockopt(zmq.TCP_KEEPALIVE_CNT) == 3
    finally:
        s.close()
        ctx.term()


def test_tcp_keepalive_disabled():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.TCP_KEEPALIVE, 0)
        assert s.getsockopt(zmq.TCP_KEEPALIVE) == 0
    finally:
        s.close()
        ctx.term()


# Group C "not implemented" raises ZMQError.

def test_unsupported_options_raise():
    ctx, s = _push()
    try:
        for opt in (zmq.AFFINITY, zmq.BACKLOG):
            with pytest.raises(zmq.ZMQError):
                s.setsockopt(opt, 1)
    finally:
        s.close()
        ctx.term()
