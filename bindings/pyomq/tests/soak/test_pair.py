"""Soak: PAIR socket bidirectional traffic.

Both sides send and receive concurrently over a single PAIR connection.
Validates that messages flow in both directions without loss or
corruption. PAIR is exclusive (1:1), so this exercises the simplest
connection topology under sustained bidirectional load.
"""

import struct
import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration, tcp_ep


def test_pair_bidirectional():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.setsockopt(zmq.SNDTIMEO, 1000)
    a.setsockopt(zmq.RCVTIMEO, 1)
    b.setsockopt(zmq.SNDTIMEO, 1000)
    b.setsockopt(zmq.RCVTIMEO, 1)
    ep = a.bind(tcp_ep())
    b.connect(ep)

    time.sleep(0.1)

    start = time.monotonic()
    last_log = start
    a_sent = 0
    b_sent = 0
    a_recvd = 0
    b_recvd = 0

    while time.monotonic() - start < duration:
        # Send burst A -> B
        for _ in range(100):
            msg_a = struct.pack("<Q", a_sent) + b"A" * 32
            try:
                a.send(msg_a)
                a_sent += 1
            except zmq.Again:
                break

        # Send burst B -> A
        for _ in range(100):
            msg_b = struct.pack("<Q", b_sent) + b"B" * 32
            try:
                b.send(msg_b)
                b_sent += 1
            except zmq.Again:
                break

        # Drain B
        try:
            while True:
                got = b.recv()
                seq = struct.unpack("<Q", got[:8])[0]
                assert got[8:] == b"A" * 32, f"B got corrupt data at seq {seq}"
                b_recvd += 1
        except zmq.Again:
            pass

        # Drain A
        try:
            while True:
                got = a.recv()
                seq = struct.unpack("<Q", got[:8])[0]
                assert got[8:] == b"B" * 32, f"A got corrupt data at seq {seq}"
                a_recvd += 1
        except zmq.Again:
            pass

        now = time.monotonic()
        if now - last_log >= 30:
            elapsed = now - start
            print(
                f"[pair] {elapsed:.0f}s, "
                f"A->B: {a_sent}/{b_recvd}, B->A: {b_sent}/{a_recvd}"
            )
            last_log = now

    elapsed = time.monotonic() - start
    print(
        f"[pair] done in {elapsed:.1f}s: "
        f"A->B: sent {a_sent} recvd {b_recvd}, "
        f"B->A: sent {b_sent} recvd {a_recvd}"
    )

    assert b_recvd > 0, "B received nothing from A"
    assert a_recvd > 0, "A received nothing from B"

    report = monitor.stop()
    report.assert_no_leak("pair")

    a.close()
    b.close()
    ctx.term()
