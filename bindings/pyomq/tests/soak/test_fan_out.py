"""Soak: high fan-out PUB -> many SUBs (all-subscribe).

One PUB with many connected SUBs all subscribed to everything. Exercises
the fan-out multicast path (pre-encode once, distribute to N peers) and
validates no corruption or message loss under N-way replication.
"""

import struct
import threading
import time

import pyomq as zmq

from conftest import ResourceMonitor, soak_duration, tcp_ep

NUM_SUBS = 16
MSG_SIZE = 128


def test_fan_out():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.SNDHWM, 500)
    ep = pub.bind(tcp_ep())

    subs = []
    for _ in range(NUM_SUBS):
        s = ctx.socket(zmq.SUB)
        s.setsockopt(zmq.RCVTIMEO, 2000)
        s.setsockopt(zmq.SUBSCRIBE, b"")
        s.connect(ep)
        subs.append(s)

    time.sleep(0.3)

    stop = False
    sent = 0
    recv_counts = [0] * NUM_SUBS

    def publisher():
        nonlocal sent, stop
        seq = 0
        while not stop:
            msg = struct.pack("<Q", seq) + bytes([seq & 0xFF] * MSG_SIZE)
            try:
                pub.send(msg)
                seq += 1
                sent = seq
            except zmq.Again:
                pass

    def subscriber(idx):
        nonlocal stop
        while not stop:
            try:
                msg = subs[idx].recv()
                seq = struct.unpack("<Q", msg[:8])[0]
                expected = bytes([seq & 0xFF] * MSG_SIZE)
                assert msg[8:] == expected, f"sub[{idx}] corruption at seq {seq}"
                recv_counts[idx] += 1
            except zmq.Again:
                pass

    t_pub = threading.Thread(target=publisher, daemon=True)
    t_subs = [
        threading.Thread(target=subscriber, args=(i,), daemon=True)
        for i in range(NUM_SUBS)
    ]
    for t in t_subs:
        t.start()
    t_pub.start()

    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        time.sleep(1)
        now = time.monotonic()
        if now - last_log >= 30:
            elapsed = now - start
            total = sum(recv_counts)
            lo = min(recv_counts)
            hi = max(recv_counts)
            print(
                f"[fan_out] {elapsed:.0f}s, sent {sent}, "
                f"total recvd {total}, range [{lo}, {hi}]"
            )
            last_log = now

    stop = True
    t_pub.join(timeout=5)
    for t in t_subs:
        t.join(timeout=5)

    elapsed = time.monotonic() - start
    total = sum(recv_counts)
    lo = min(recv_counts)
    hi = max(recv_counts)
    print(
        f"[fan_out] done: sent {sent}, total recvd {total} "
        f"in {elapsed:.1f}s, range [{lo}, {hi}]"
    )

    for i, count in enumerate(recv_counts):
        assert count > 0, f"sub[{i}] received nothing"

    report = monitor.stop()
    report.assert_no_leak("fan_out")

    for s in subs:
        s.close()
    pub.close()
    ctx.term()
