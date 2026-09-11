"""Soak: sustained PUSH/PULL over TCP.

Sender and receiver on separate threads, continuous small messages.
"""

import threading
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration, tcp_ep


def test_push_pull_sustained():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    ep = pull.bind(tcp_ep())
    push.connect(ep)
    push.setsockopt(zmq.SNDTIMEO, 2000)
    pull.setsockopt(zmq.RCVTIMEO, 2000)

    stop = False
    sent = 0
    recvd = 0

    def sender():
        nonlocal sent, stop
        while not stop:
            try:
                push.send(b"soak")
                sent += 1
            except zmq.Again:
                pass

    def receiver():
        nonlocal recvd, stop
        while not stop:
            try:
                pull.recv()
                recvd += 1
            except zmq.Again:
                pass

    t_send = threading.Thread(target=sender)
    t_recv = threading.Thread(target=receiver)
    t_send.start()
    t_recv.start()

    start = time.monotonic()
    last_log = start
    while time.monotonic() - start < duration:
        time.sleep(1)
        if time.monotonic() - last_log >= 30:
            elapsed = time.monotonic() - start
            print(f"[push_pull] {elapsed:.0f}s, sent {sent}, recvd {recvd}")
            last_log = time.monotonic()

    stop = True
    t_send.join(timeout=5)
    t_recv.join(timeout=5)

    print(f"[push_pull] done: sent {sent}, recvd {recvd} in {duration:.1f}s")

    push.close()
    pull.close()
    ctx.term()

    report = monitor.stop()
    report.assert_no_leak("push_pull")
