"""Runtime checks for public contracts not exercised by native stubtest."""

import errno

import pyomq as zmq
import pyomq.asyncio as azmq
import pytest


@pytest.mark.parametrize("context", [zmq.Context, azmq.Context])
def test_bytes_endpoints_and_metadata(context):
    with context() as ctx, ctx.socket(zmq.PUSH) as socket:
        assert socket.bind(b"inproc://bytes-contract") == b"inproc://bytes-contract"
        assert socket.last_endpoint == b"inproc://bytes-contract"
        assert socket.getsockopt(zmq.LAST_ENDPOINT) == b"inproc://bytes-contract"
        socket.unbind(b"inproc://bytes-contract")
        socket.connect(b"inproc://bytes-contract")
        socket.disconnect(b"inproc://bytes-contract")
        assert socket.connection_info(999_999) is None


@pytest.mark.parametrize("context", [zmq.Context, azmq.Context])
def test_auth_accepts_one_shot_iterables(context):
    with context() as ctx, ctx.socket(zmq.PULL) as socket:
        socket.set_plain_auth(pair for pair in [("user", "password")])
        public, _ = zmq.curve_keypair()
        socket.set_curve_auth(key for key in [public])


def test_error_constructor_contract():
    error = zmq.ZMQError(errno.EAGAIN)
    assert error.errno == errno.EAGAIN
    assert error.strerror == zmq.strerror(errno.EAGAIN)
    assert str(error) == error.strerror
    error = zmq.ZMQError(errno=errno.EINVAL, msg="invalid")
    assert error.errno == errno.EINVAL
    assert str(error) == error.strerror == "invalid"
    assert zmq.ZMQError("message").strerror == "message"
    assert zmq.ZMQError(msg="message").strerror == "message"
    assert zmq.ZMQError().errno is None


@pytest.mark.asyncio
async def test_stream_callbacks_receive_parts_and_tracker():
    with (
        zmq.Context() as ctx,
        ctx.socket(zmq.PUSH) as push,
        ctx.socket(zmq.PULL) as pull,
    ):
        pull.bind("inproc://stream-callbacks")
        push.connect("inproc://stream-callbacks")
        stream = zmq.ZMQStream(push)
        called = []
        stream.on_send(
            lambda parts, tracker: called.append(("default", parts, tracker))
        )
        tracker = stream.send(b"first", copy=False, track=True)
        assert called == [("default", [b"first"], tracker)]
        assert pull.recv() == b"first"
        tracker = stream.send_multipart(
            (part for part in [b"second", b"third"]),
            copy=False,
            track=True,
            callback=lambda parts, tracker: called.append(("override", parts, tracker)),
        )
        assert called[-1] == ("override", [b"second", b"third"], tracker)
        assert len(called) == 2
        assert pull.recv_multipart() == [b"second", b"third"]

        class FalseyCallback:
            def __bool__(self):
                return False

            def __call__(self, parts, tracker):
                called.append(("falsey", parts, tracker))

        stream.send(b"fourth", callback=FalseyCallback())
        assert called[-1] == ("falsey", [b"fourth"], None)
        assert pull.recv() == b"fourth"
        stream.send_multipart(iter([b"fifth"]), callback=FalseyCallback())
        assert called[-1] == ("falsey", [b"fifth"], None)
        assert pull.recv_multipart() == [b"fifth"]
        stream.close()
