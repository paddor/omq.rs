"""Message, Frame, MessageTracker compat classes."""

import pyomq as zmq


def test_message_bytes_property():
    m = zmq.Message(b"hello")
    assert m.bytes == b"hello"


def test_message_buffer_property():
    m = zmq.Message(b"world")
    assert bytes(m.buffer) == b"world"


def test_message_is_bytes():
    m = zmq.Message(b"test")
    assert isinstance(m, bytes)
    assert m == b"test"


def test_frame_alias():
    assert zmq.Frame is zmq.Message


def test_message_default_empty():
    m = zmq.Message()
    assert m == b""


def test_message_tracker_done():
    t = zmq.MessageTracker()
    assert t.done is True
    t.wait()


def test_message_tracker_pending():
    t = zmq.MessageTracker(_pending=True)
    assert t.done is False
    try:
        t.wait()
        assert False, "should have raised NotDone"
    except zmq.NotDone:
        pass


def test_isinstance_sync_socket():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    try:
        assert isinstance(s, zmq.Socket)
    finally:
        s.close()
        ctx.term()


def test_isinstance_async_socket():
    import pyomq.asyncio as zmq_async

    ctx = zmq_async.Context()
    s = ctx.socket(zmq.PUSH)
    try:
        assert isinstance(s, zmq.Socket)
    finally:
        s.close()
