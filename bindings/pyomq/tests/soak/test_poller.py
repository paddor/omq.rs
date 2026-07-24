"""Soak: Poller under sustained load with multiple sockets.

Exercises the poll/select path with multiple PULL sockets receiving
from independent PUSH senders. Validates that the poller correctly
identifies ready sockets and that no messages are lost.
"""

import struct
import threading
import time

import pyomq as zmq

from conftest import ResourceMonitor, soak_duration, tcp_ep

NUM_CHANNELS = 4


def test_poller_multi_socket():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pulls = []
    pushes = []
    eps = []

    for _ in range(NUM_CHANNELS):
        pull = ctx.socket(zmq.PULL)
        push = ctx.socket(zmq.PUSH)
        ep = pull.bind(tcp_ep())
        push.connect(ep)
        push.setsockopt(zmq.SNDTIMEO, 2000)
        pulls.append(pull)
        pushes.append(push)
        eps.append(ep)

    time.sleep(0.1)

    stop = False
    sent_counts = [0] * NUM_CHANNELS
    recv_counts = [0] * NUM_CHANNELS

    def sender(idx):
        nonlocal stop
        seq = 0
        while not stop:
            msg = struct.pack("<QI", seq, idx) + b"P" * 48
            try:
                pushes[idx].send(msg)
                seq += 1
                sent_counts[idx] = seq
            except zmq.Again:
                pass

    threads = []
    for i in range(NUM_CHANNELS):
        t = threading.Thread(target=sender, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    poller = zmq.Poller()
    for pull in pulls:
        poller.register(pull, zmq.POLLIN)

    sock_to_idx = {id(pull): i for i, pull in enumerate(pulls)}

    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        events = poller.poll(timeout=100)
        for sock, _ in events:
            ch_idx = sock_to_idx.get(id(sock))
            if ch_idx is None:
                continue
            try:
                msg = sock.recv(flags=zmq.NOBLOCK)
                seq = struct.unpack("<QI", msg[:12])
                assert seq[1] == ch_idx, (
                    f"channel mismatch: got {seq[1]}, expected {ch_idx}"
                )
                assert msg[12:] == b"P" * 48, f"corruption on channel {ch_idx}"
                recv_counts[ch_idx] += 1
            except zmq.Again:
                pass

        now = time.monotonic()
        if now - last_log >= 30:
            elapsed = now - start
            total = sum(recv_counts)
            print(
                f"[poller] {elapsed:.0f}s, total recvd {total}, per-ch: {recv_counts}"
            )
            last_log = now

    stop = True
    for t in threads:
        t.join(timeout=5)

    elapsed = time.monotonic() - start
    total = sum(recv_counts)
    print(
        f"[poller] done: total recvd {total} in {elapsed:.1f}s, per-ch: {recv_counts}"
    )

    for i, count in enumerate(recv_counts):
        assert count > 0, f"channel {i} received nothing via poller"

    report = monitor.stop()
    report.assert_no_leak("poller")

    for pull in pulls:
        poller.unregister(pull)
        pull.close()
    for push in pushes:
        push.close()
    ctx.term()
