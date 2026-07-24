"""Exception hierarchy and promotion tests."""

import errno

import pytest

import pyomq as zmq


def test_again_on_rcvtimeo(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.setsockopt(zmq.RCVTIMEO, 50)
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        pull.close()
        ctx.term()


def test_again_is_zmqerror():
    assert issubclass(zmq.Again, zmq.ZMQError)


def test_again_is_zmqbaseerror():
    assert issubclass(zmq.Again, zmq.error.ZMQBaseError)


def test_again_errno(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.setsockopt(zmq.RCVTIMEO, 50)
        with pytest.raises(zmq.Again) as exc_info:
            pull.recv()
        assert exc_info.value.errno == errno.EAGAIN
    finally:
        pull.close()
        ctx.term()


def test_closed_socket_raises_context_terminated(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.close()
        with pytest.raises(zmq.ContextTerminated):
            pull.recv()
    finally:
        ctx.term()


def test_context_terminated_errno(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.close()
        with pytest.raises(zmq.ContextTerminated) as exc_info:
            pull.recv()
        assert exc_info.value.errno == 156
    finally:
        ctx.term()


def test_zmqerror_on_bind_invalid_address():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        with pytest.raises(zmq.ZMQError):
            sock.bind("tcp://999.999.999.999:0")
    finally:
        sock.close()
        ctx.term()


def test_zmqbinderror_is_zmqbaseerror():
    assert issubclass(zmq.ZMQBindError, zmq.error.ZMQBaseError)


def test_zmqbinderror_not_zmqerror():
    assert not issubclass(zmq.ZMQBindError, zmq.ZMQError)


def test_zmqerror_catches_subclasses(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.setsockopt(zmq.RCVTIMEO, 50)
        with pytest.raises(zmq.ZMQError):
            pull.recv()
    finally:
        pull.close()
        ctx.term()


def test_zmqbaseerror_catches_all(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    try:
        ep = pull.bind(tcp_endpoint)
        pull.setsockopt(zmq.RCVTIMEO, 50)
        with pytest.raises(zmq.error.ZMQBaseError):
            pull.recv()
    finally:
        pull.close()
        ctx.term()


def test_zmqbaseerror_catches_binderror():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        with pytest.raises(zmq.error.ZMQBaseError):
            sock.bind("tcp://999.999.999.999:0")
    finally:
        sock.close()
        ctx.term()


def test_from_native_unknown_errno():
    from pyomq import _native

    exc = _native.ZMQError("test error")
    exc.errno = 9999
    promoted = zmq.error.from_native(exc)
    assert type(promoted) is zmq.ZMQError
    assert promoted.errno == 9999


def test_not_implemented_error():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        with pytest.raises(zmq.error.NotImplementedError):
            sock.setsockopt(zmq.AFFINITY, 0)
    finally:
        sock.close()
        ctx.term()
