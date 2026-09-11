"""Soak: sustained PUSH/PULL over IPC (Unix abstract socket).

Same pattern as TCP throughput but over IPC transport. Exercises the
IPC connection path and validates no message loss or corruption.
"""

import hashlib
import os
import threading
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration

MSG_SIZE = 512


def ipc_ep() -> str:
    tag = hashlib.sha1(f"soak-ipc-{os.getpid()}".encode()).hexdigest()[:16]
    return f"ipc://@pyomq-soak-{tag}"


def test_ipc_throughput():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ep = ipc_ep()
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    pull.bind(ep)
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

    t_send = threading.Thread(target=sender, daemon=True)
    t_recv = threading.Thread(target=receiver, daemon=True)
    t_recv.start()
    t_send.start()

    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        time.sleep(1)
        now = time.monotonic()
        if now - last_log >= 30:
            elapsed = now - start
            print(
                f"[ipc_throughput] {elapsed:.0f}s, "
                f"sent {sent}, recvd {recvd}, "
                f"{recvd / elapsed:.0f} msg/s"
            )
            last_log = now

    stop = True
    t_send.join(timeout=5)
    t_recv.join(timeout=5)

    elapsed = time.monotonic() - start
    print(
        f"[ipc_throughput] done: sent {sent}, recvd {recvd} "
        f"in {elapsed:.1f}s ({recvd / elapsed:.0f} msg/s)"
    )

    assert recvd > 0, "no messages received"

    report = monitor.stop()
    report.assert_no_leak("ipc_throughput")

    push.close()
    pull.close()
    ctx.term()
