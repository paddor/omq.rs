"""Soak: multipart message integrity.

PUSH/PULL over TCP with multi-frame messages. Validates multipart
atomicity (all frames delivered together or not at all), frame count,
frame sizes, content integrity, and monotonic sequencing. Sender and
receiver on separate threads.

Also tests DEALER/ROUTER identity routing with multipart in a
single-threaded request-reply loop (pyomq sockets are not safe for
concurrent send+recv from different threads).
"""

import struct
import threading
import time

import pyomq as zmq
from typing import List

from conftest import ResourceMonitor, soak_duration, tcp_ep

FRAME_B = 128
NUM_FRAMES = 3


def build_multipart(seq: int) -> List[bytes]:
    header = struct.pack("<Q", seq)
    body = bytes((seq + i) & 0xFF for i in range(FRAME_B))
    trailer = struct.pack("<Q", seq ^ 0xFFFFFFFFFFFFFFFF)
    return [header, body, trailer]


def validate_multipart(parts: List[bytes], min_seq: int) -> int:
    assert len(parts) == NUM_FRAMES, (
        f"frame count mismatch: got {len(parts)}, expected {NUM_FRAMES}"
    )

    seq = struct.unpack("<Q", parts[0])[0]
    assert seq >= min_seq, f"sequence went backwards: got {seq}, expected >= {min_seq}"

    assert len(parts[1]) == FRAME_B, (
        f"body frame size: got {len(parts[1])}, expected {FRAME_B}"
    )
    expected_body = bytes((seq + i) & 0xFF for i in range(FRAME_B))
    assert parts[1] == expected_body, f"body corruption at seq {seq}"

    expected_trailer = struct.pack("<Q", seq ^ 0xFFFFFFFFFFFFFFFF)
    assert parts[2] == expected_trailer, f"trailer corruption at seq {seq}"

    return seq


def test_multipart_push_pull():
    """Sustained multipart over PUSH/PULL TCP (separate threads)."""
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    ep = pull.bind(tcp_ep())
    push.connect(ep)
    push.setsockopt(zmq.SNDTIMEO, 5000)
    pull.setsockopt(zmq.RCVTIMEO, 5000)

    stop = False
    sent = 0
    recvd = 0

    def sender():
        nonlocal sent, stop
        seq = 0
        while not stop:
            try:
                push.send_multipart(build_multipart(seq))
                seq += 1
                sent = seq
            except zmq.Again:
                pass

    def receiver():
        nonlocal recvd, stop
        last_seq = 0
        while not stop:
            try:
                parts = pull.recv_multipart()
                last_seq = validate_multipart(parts, last_seq)
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
                f"[multipart_push_pull] {elapsed:.0f}s, "
                f"sent {sent}, recvd {recvd}, "
                f"{recvd / elapsed:.0f} msg/s"
            )
            last_log = now

    stop = True
    t_send.join(timeout=5)
    t_recv.join(timeout=5)

    elapsed = time.monotonic() - start
    print(
        f"[multipart_push_pull] done: sent {sent}, recvd {recvd} "
        f"in {elapsed:.1f}s ({recvd / elapsed:.0f} msg/s)"
    )

    assert recvd > 0, "no messages received"

    report = monitor.stop()
    report.assert_no_leak("multipart_push_pull")

    push.close()
    pull.close()
    ctx.term()


def test_multipart_dealer_router():
    """DEALER/ROUTER identity routing with multipart (single-threaded loop)."""
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    dealer = ctx.socket(zmq.DEALER)
    ep = router.bind(tcp_ep())
    dealer.setsockopt(zmq.IDENTITY, b"soak-client")
    dealer.connect(ep)
    dealer.setsockopt(zmq.SNDTIMEO, 5000)
    dealer.setsockopt(zmq.RCVTIMEO, 5000)
    router.setsockopt(zmq.RCVTIMEO, 5000)
    router.setsockopt(zmq.SNDTIMEO, 5000)

    time.sleep(0.1)

    start = time.monotonic()
    last_log = start
    cycles = 0
    last_seq = 0

    while time.monotonic() - start < duration:
        dealer.send_multipart(build_multipart(cycles))

        parts = router.recv_multipart()
        assert parts[0] == b"soak-client", f"identity mismatch: {parts[0]!r}"
        router.send_multipart(parts)

        reply = dealer.recv_multipart()
        last_seq = validate_multipart(reply, last_seq)
        cycles += 1

        now = time.monotonic()
        if now - last_log >= 30:
            elapsed = now - start
            print(
                f"[multipart_dealer_router] {elapsed:.0f}s, "
                f"{cycles} cycles, {cycles / elapsed:.0f} rtt/s"
            )
            last_log = now

    elapsed = time.monotonic() - start
    print(
        f"[multipart_dealer_router] done: {cycles} cycles "
        f"in {elapsed:.1f}s ({cycles / elapsed:.0f} rtt/s)"
    )

    assert cycles > 0, "no round-trips completed"

    report = monitor.stop()
    report.assert_no_leak("multipart_dealer_router")

    dealer.close()
    router.close()
    ctx.term()
