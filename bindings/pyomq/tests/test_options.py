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


def test_hwm_rejects_negative():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.SNDHWM, 64)
        s.setsockopt(zmq.RCVHWM, 32)

        with pytest.raises(ValueError):
            s.setsockopt(zmq.SNDHWM, -1)
        with pytest.raises(ValueError):
            s.setsockopt(zmq.RCVHWM, -1)

        assert s.getsockopt(zmq.SNDHWM) == 64
        assert s.getsockopt(zmq.RCVHWM) == 32
    finally:
        s.close()
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


# Group D: newly readable options (Phase 4).


def test_reconnect_ivl_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.RECONNECT_IVL, 500)
        assert s.getsockopt(zmq.RECONNECT_IVL) == 500
    finally:
        s.close()
        ctx.term()


def test_heartbeat_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.HEARTBEAT_IVL, 1000)
        s.setsockopt(zmq.HEARTBEAT_TTL, 3000)
        s.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)
        assert s.getsockopt(zmq.HEARTBEAT_IVL) == 1000
        assert s.getsockopt(zmq.HEARTBEAT_TTL) == 3000
        assert s.getsockopt(zmq.HEARTBEAT_TIMEOUT) == 5000
    finally:
        s.close()
        ctx.term()


def test_conflate_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.CONFLATE, 1)
        assert s.getsockopt(zmq.CONFLATE) == 1
    finally:
        s.close()
        ctx.term()


def test_handshake_ivl_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.HANDSHAKE_IVL, 2000)
        assert s.getsockopt(zmq.HANDSHAKE_IVL) == 2000
    finally:
        s.close()
        ctx.term()


# Group E: SNDBUF / RCVBUF round-trip.


def test_sndbuf_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.SNDBUF, 65536)
        assert s.getsockopt(zmq.SNDBUF) == 65536
    finally:
        s.close()
        ctx.term()


def test_rcvbuf_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.RCVBUF, 32768)
        assert s.getsockopt(zmq.RCVBUF) == 32768
    finally:
        s.close()
        ctx.term()


# Group F: PLAIN auth option round-trip.


def test_plain_options_round_trip():
    ctx, s = _push()
    try:
        s.setsockopt(zmq.PLAIN_SERVER, 1)
        assert s.getsockopt(zmq.PLAIN_SERVER) == 1
        s.setsockopt(zmq.PLAIN_USERNAME, b"admin")
        assert s.getsockopt(zmq.PLAIN_USERNAME) == b"admin"
        s.setsockopt(zmq.PLAIN_PASSWORD, b"secret")
        assert s.getsockopt(zmq.PLAIN_PASSWORD) == b"secret"
    finally:
        s.close()
        ctx.term()


# Group G: no-op options don't raise.


def test_noop_options_accepted():
    ctx, s = _push()
    try:
        for opt in (
            zmq.XPUB_VERBOSE,
            zmq.PROBE_ROUTER,
            zmq.REQ_CORRELATE,
            zmq.REQ_RELAXED,
            zmq.ROUTER_HANDOVER,
            zmq.ZAP_DOMAIN,
            zmq.RATE,
            zmq.CONNECT_TIMEOUT,
            zmq.RECOVERY_IVL,
        ):
            s.setsockopt(opt, 0)
    finally:
        s.close()
        ctx.term()


def test_has_feature():
    assert zmq.has("ipc") is True
    assert zmq.has("inproc") is True
    assert zmq.has("pgm") is False
    assert isinstance(zmq.has("curve"), bool)
    assert isinstance(zmq.has("lz4"), bool)
    assert zmq.has("zstd") is True
    assert isinstance(zmq.has("plain"), bool)
    assert zmq.has("gssapi") is False
    assert zmq.has("INPROC") is True


# Group H: omq-specific options.


def test_on_mute_round_trip():
    ctx, s = _push()
    try:
        assert s.getsockopt(zmq.OMQ_ON_MUTE) == zmq.OMQ_ON_MUTE_BLOCK
        s.setsockopt(zmq.OMQ_ON_MUTE, zmq.OMQ_ON_MUTE_DROP_NEWEST)
        assert s.getsockopt(zmq.OMQ_ON_MUTE) == zmq.OMQ_ON_MUTE_DROP_NEWEST
        s.setsockopt(zmq.OMQ_ON_MUTE, zmq.OMQ_ON_MUTE_DROP_OLDEST)
        assert s.getsockopt(zmq.OMQ_ON_MUTE) == zmq.OMQ_ON_MUTE_DROP_OLDEST
        s.setsockopt(zmq.OMQ_ON_MUTE, zmq.OMQ_ON_MUTE_BLOCK)
        assert s.getsockopt(zmq.OMQ_ON_MUTE) == zmq.OMQ_ON_MUTE_BLOCK
    finally:
        s.close()
        ctx.term()


def test_on_mute_rejects_invalid():
    ctx, s = _push()
    try:
        with pytest.raises((zmq.ZMQError, ValueError)):
            s.setsockopt(zmq.OMQ_ON_MUTE, 99)
    finally:
        s.close()
        ctx.term()

        ctx.term()


def test_compression_dict_round_trip():
    ctx, s = _push()
    try:
        assert s.getsockopt(zmq.OMQ_COMPRESSION_DICT) == b""
        d = b"my-dict-bytes-1234"
        s.setsockopt(zmq.OMQ_COMPRESSION_DICT, d)
        assert s.getsockopt(zmq.OMQ_COMPRESSION_DICT) == d
        s.setsockopt(zmq.OMQ_COMPRESSION_DICT, b"")
        assert s.getsockopt(zmq.OMQ_COMPRESSION_DICT) == b""
    finally:
        s.close()
        ctx.term()


def test_compression_level_round_trip():
    ctx, s = _push()
    try:
        assert s.getsockopt(zmq.OMQ_COMPRESSION_LEVEL) == 0
        s.setsockopt(zmq.OMQ_COMPRESSION_LEVEL, 1)
        assert s.getsockopt(zmq.OMQ_COMPRESSION_LEVEL) == 1
        s.compression_level = 3
        assert s.compression_level == 3
    finally:
        s.close()
        ctx.term()


def test_compression_level_rejects_invalid():
    ctx, s = _push()
    try:
        for level in (-9, 5):
            with pytest.raises((zmq.ZMQError, ValueError)):
                s.setsockopt(zmq.OMQ_COMPRESSION_LEVEL, level)
    finally:
        s.close()
        ctx.term()


def test_compression_dict_rejects_oversize():
    ctx, s = _push()
    try:
        oversize = b"\x00" * (8 * 1024 + 1)
        with pytest.raises((zmq.ZMQError, ValueError)):
            s.setsockopt(zmq.OMQ_COMPRESSION_DICT, oversize)
    finally:
        s.close()
        ctx.term()


def test_compression_auto_train_round_trip():
    ctx, s = _push()
    try:
        assert s.getsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN) == 0
        s.setsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN, 1)
        assert s.getsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN) == 1
        s.setsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN, 0)
        assert s.getsockopt(zmq.OMQ_COMPRESSION_AUTO_TRAIN) == 0
    finally:
        s.close()
        ctx.term()
