"""Soak: sustained PUB/SUB throughput with topic filtering.

One PUB, multiple SUBs with different prefix filters. Validates that
subscribers only receive messages matching their subscription and that
content is intact. Exercises the fan-out path and subscription matching.
"""

import struct
import threading
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration, tcp_ep

NUM_SUBS = 4
TOPICS = [b"A:", b"B:", b"C:", b"D:"]
BODY_SIZE = 256


def test_pub_sub_throughput():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.SNDHWM, 1000)
    pub.bind(tcp_ep())
    ep = pub.last_endpoint

    subs = []
    for i in range(NUM_SUBS):
        s = ctx.socket(zmq.SUB)
        s.setsockopt(zmq.RCVTIMEO, 2000)
        s.setsockopt(zmq.SUBSCRIBE, TOPICS[i])
        s.connect(ep)
        subs.append(s)

    time.sleep(0.2)

    stop = False
    sent = 0
    recv_counts = [0] * NUM_SUBS

    def publisher():
        nonlocal sent, stop
        seq = 0
        while not stop:
            topic_idx = seq % NUM_SUBS
            header = TOPICS[topic_idx] + struct.pack("<Q", seq)
            body = bytes((seq + i) & 0xFF for i in range(BODY_SIZE))
            try:
                pub.send(header + body)
                seq += 1
                sent = seq
            except zmq.Again:
                pass

    def subscriber(idx):
        nonlocal stop
        topic = TOPICS[idx]
        while not stop:
            try:
                msg = subs[idx].recv()
                assert msg[:2] == topic, (
                    f"sub[{idx}] got wrong topic: {msg[:2]!r} != {topic!r}"
                )
                seq = struct.unpack("<Q", msg[2:10])[0]
                expected_body = bytes((seq + i) & 0xFF for i in range(BODY_SIZE))
                assert msg[10:] == expected_body, (
                    f"sub[{idx}] body corruption at seq {seq}"
                )
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
            total_recv = sum(recv_counts)
            print(
                f"[pub_sub_throughput] {elapsed:.0f}s, "
                f"sent {sent}, recvd {total_recv}, "
                f"per-sub: {[r for r in recv_counts]}"
            )
            last_log = now

    stop = True
    t_pub.join(timeout=5)
    for t in t_subs:
        t.join(timeout=5)

    elapsed = time.monotonic() - start
    total_recv = sum(recv_counts)
    print(
        f"[pub_sub_throughput] done: sent {sent}, total recvd {total_recv} "
        f"in {elapsed:.1f}s, per-sub: {recv_counts}"
    )

    for i, count in enumerate(recv_counts):
        assert count > 0, f"sub[{i}] received nothing"

    for s in subs:
        s.close()
    pub.close()
    ctx.term()

    report = monitor.stop()
    report.assert_no_leak("pub_sub_throughput")
