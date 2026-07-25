"""Soak: peer churn + network partitions under sustained PUSH load.

PUSH bound on TCP, continuous send. Pool of PULL peers with realistic
churn patterns: most ticks just deliver traffic; occasional ticks
simulate network partitions (disconnect/reconnect existing peers);
rare ticks close+replace a peer entirely.

Tick rate is throttled to ~10 Hz to keep socket create/destroy rate
in a realistic range (well under 10/sec on average).
"""

import random
import time

import pyomq as zmq
from pyomq import Socket

from conftest import ResourceMonitor, soak_duration, tcp_ep

NUM_PEERS = 20
TICK_HZ = 10
PARTITION_PROB = 0.15
CHURN_PROB = 0.05


def test_peer_churn():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDTIMEO, 1)
    push.setsockopt(zmq.SNDHWM, 1024)
    ep = push.bind(tcp_ep())

    peers: list[tuple[Socket, bool]] = []
    for _ in range(NUM_PEERS):
        p = ctx.socket(zmq.PULL)
        p.setsockopt(zmq.RCVTIMEO, 0)
        p.connect(ep)
        peers.append((p, True))
    time.sleep(0.1)

    sent = 0
    partitions = 0
    heals = 0
    replaced = 0
    start = time.monotonic()
    last_log = start
    tick = 1.0 / TICK_HZ

    while time.monotonic() - start < duration:
        tick_start = time.monotonic()
        roll = random.random()

        if roll < PARTITION_PROB:
            connected_idx = [i for i, (_, c) in enumerate(peers) if c]
            disconnected_idx = [i for i, (_, c) in enumerate(peers) if not c]
            if disconnected_idx and (not connected_idx or random.random() < 0.5):
                i = random.choice(disconnected_idx)
                p, _ = peers[i]
                p.connect(ep)
                peers[i] = (p, True)
                heals += 1
            elif connected_idx:
                i = random.choice(connected_idx)
                p, _ = peers[i]
                p.disconnect(ep)
                peers[i] = (p, False)
                partitions += 1
        elif roll < PARTITION_PROB + CHURN_PROB:
            i = random.randrange(len(peers))
            old, _ = peers[i]
            old.close()
            new = ctx.socket(zmq.PULL)
            new.setsockopt(zmq.RCVTIMEO, 0)
            new.connect(ep)
            peers[i] = (new, True)
            replaced += 1

        for _ in range(100):
            try:
                push.send(b"soak")
                sent += 1
            except zmq.Again:
                break

        for p, connected in peers:
            if not connected:
                continue
            try:
                while True:
                    p.recv()
            except zmq.Again:
                pass

        now = time.monotonic()
        if now - last_log >= 30:
            connected_n = sum(1 for _, c in peers if c)
            print(
                f"[peer_churn] {now - start:.0f}s, sent {sent}, "
                f"connected {connected_n}/{len(peers)}, "
                f"partitions {partitions}, heals {heals}, "
                f"replaced {replaced}"
            )
            last_log = now

        elapsed = time.monotonic() - tick_start
        if elapsed < tick:
            time.sleep(tick - elapsed)

    for p, _ in peers:
        p.close()
    push.close()
    ctx.term()

    print(
        f"[peer_churn] done: {sent} messages, "
        f"{partitions} partitions, {heals} heals, "
        f"{replaced} socket replacements in {duration:.1f}s"
    )

    report = monitor.stop()
    report.assert_no_leak("peer_churn")
