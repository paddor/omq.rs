"""Soak: large message sustained throughput.

PUSH/PULL over TCP with 1 MiB messages. Sender and receiver on
separate threads. Asserts RSS stays proportional to HWM, not time.
"""

import threading
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration, tcp_ep

MSG_SIZE = 1024 * 1024


def test_large_message_throughput():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    pull.setsockopt(zmq.RCVHWM, 4)
    push.setsockopt(zmq.SNDHWM, 4)
    pull.bind(tcp_ep())
    ep = pull.last_endpoint
    push.connect(ep)
    push.setsockopt(zmq.SNDTIMEO, 5000)
    pull.setsockopt(zmq.RCVTIMEO, 5000)

    payload = bytes(i & 0xFF for i in range(MSG_SIZE))
    stop = False
    sent = 0
    recvd = 0

    def sender():
        nonlocal sent, stop
        while not stop:
            try:
                push.send(payload)
                sent += 1
            except zmq.Again:
                pass

    def receiver():
        nonlocal recvd, stop
        while not stop:
            try:
                msg = pull.recv()
                assert len(msg) == MSG_SIZE
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
        now = time.monotonic()
        if now - last_log >= 30:
            mib_s = recvd * MSG_SIZE / (now - start) / 1_048_576
            print(
                f"[large_msg] {now - start:.0f}s, "
                f"sent {sent}, recvd {recvd} ({mib_s:.0f} MiB/s)"
            )
            last_log = now

    stop = True
    t_send.join(timeout=5)
    t_recv.join(timeout=5)

    mib_s = recvd * MSG_SIZE / duration / 1_048_576
    print(
        f"[large_msg] done: sent {sent}, recvd {recvd} "
        f"in {duration:.1f}s ({mib_s:.0f} MiB/s)"
    )

    push.close()
    pull.close()
    ctx.term()

    report = monitor.stop()
    report.assert_no_leak("large_msg")
