"""device() function tests."""

import threading
import time

import pyomq as zmq


def test_device_forwards_messages(tcp_endpoint):
    ctx = zmq.Context()
    frontend = ctx.socket(zmq.PULL)
    backend = ctx.socket(zmq.PUSH)
    sender = ctx.socket(zmq.PUSH)
    receiver = ctx.socket(zmq.PULL)
    try:
        fe_ep = frontend.bind(tcp_endpoint)
        be_ep = backend.bind("tcp://127.0.0.1:0")

        sender.connect(fe_ep)
        receiver.connect(be_ep)

        t = threading.Thread(
            target=zmq.device, args=(zmq.STREAMER, frontend, backend), daemon=True
        )
        t.start()

        time.sleep(0.1)
        sender.send(b"through-device")
        receiver.setsockopt(zmq.RCVTIMEO, 5000)
        assert receiver.recv() == b"through-device"
    finally:
        sender.close()
        receiver.close()
        frontend.close()
        backend.close()
        ctx.term()
