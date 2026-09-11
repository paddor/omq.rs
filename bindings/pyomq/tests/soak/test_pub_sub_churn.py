"""Soak: PUB/SUB with subscriber churn.

PUB bound on TCP. Subscribers connect with prefix filters, receive
briefly, disconnect. Exercises the PyO3 subscribe/close lifecycle.
"""

import random
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration, tcp_ep

TOPICS = [b"fast.", b"slow.", b"all.", b"rare."]


def test_pub_sub_churn():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.SNDTIMEO, 100)
    ep = pub.bind(tcp_ep())

    subs: list = []
    pub_count = 0
    start = time.monotonic()
    last_churn = start
    last_log = start

    while time.monotonic() - start < duration:
        # Publish a burst.
        topic = TOPICS[pub_count % len(TOPICS)]
        for _ in range(1000):
            try:
                pub.send(topic + f"{pub_count}".encode())
                pub_count += 1
            except zmq.Again:
                break

        # Drain all subscribers.
        for sub in subs:
            sub.setsockopt(zmq.RCVTIMEO, 0)
            try:
                while True:
                    sub.recv()
            except zmq.Again:
                pass

        # Churn subscribers every ~500ms.
        now = time.monotonic()
        if now - last_churn >= 0.5:
            last_churn = now
            if subs and random.random() < 0.5:
                idx = random.randrange(len(subs))
                subs[idx].close()
                subs.pop(idx)
            if len(subs) < 10:
                sub = ctx.socket(zmq.SUB)
                sub.connect(ep)
                sub.setsockopt(zmq.SUBSCRIBE, random.choice(TOPICS))
                subs.append(sub)

        if now - last_log >= 30:
            print(
                f"[pub_sub_churn] {now - start:.0f}s, "
                f"pub_count {pub_count}, subs {len(subs)}"
            )
            last_log = now

    for sub in subs:
        sub.close()
    pub.close()
    ctx.term()

    print(f"[pub_sub_churn] done: {pub_count} published in {duration:.1f}s")

    report = monitor.stop()
    report.assert_no_leak("pub_sub_churn")
