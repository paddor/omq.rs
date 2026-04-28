"""PUB/SUB and XPUB/XSUB."""

import time

import pyomq as zmq


def test_pub_sub_prefix_filter(tcp_endpoint):
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    sub = ctx.socket(zmq.SUB)
    try:
        pub.bind(tcp_endpoint)
        sub.connect(tcp_endpoint)
        sub.setsockopt(zmq.SUBSCRIBE, b"weather/")
        # PUB/SUB has no built-in handshake; give the SUBSCRIBE a moment
        # to propagate.
        time.sleep(0.2)
        pub.send(b"sports/score-12")  # filtered
        pub.send(b"weather/sunny")    # delivered
        pub.send(b"weather/rain")     # delivered
        sub.setsockopt(zmq.RCVTIMEO, 500)
        got = [sub.recv() for _ in range(2)]
        assert got == [b"weather/sunny", b"weather/rain"]
    finally:
        pub.close()
        sub.close()
        ctx.term()


def test_unsubscribe_drops_topic(tcp_endpoint):
    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    sub = ctx.socket(zmq.SUB)
    try:
        pub.bind(tcp_endpoint)
        sub.connect(tcp_endpoint)
        sub.setsockopt(zmq.SUBSCRIBE, b"a")
        sub.setsockopt(zmq.SUBSCRIBE, b"b")
        time.sleep(0.1)
        sub.setsockopt(zmq.UNSUBSCRIBE, b"a")
        time.sleep(0.1)
        pub.send(b"a-one")  # filtered
        pub.send(b"b-two")  # delivered
        sub.setsockopt(zmq.RCVTIMEO, 500)
        assert sub.recv() == b"b-two"
    finally:
        pub.close()
        sub.close()
        ctx.term()
