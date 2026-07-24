"""Soak: reconnect storm.

PUSH connects to a TCP port. PULL binds, exchanges one message,
closes. New PULL rebinds, PUSH reconnects. Repeated for the full
duration. Exercises the PyO3 socket creation/teardown path.
"""

import errno
import time

import pyomq as zmq

from conftest import ResourceMonitor, soak_duration, tcp_ep


def _is_eaddrinuse(exc):
    return getattr(
        exc, "errno", None
    ) == errno.EADDRINUSE or "Address already in use" in str(exc)


def _new_pull(ctx):
    pull = ctx.socket(zmq.PULL)
    pull.setsockopt(zmq.RCVTIMEO, 5000)
    return pull


def test_reconnect_storm():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDTIMEO, 5000)
    push.setsockopt(zmq.RECONNECT_IVL, 10)

    pull = _new_pull(ctx)
    ep = pull.bind(tcp_ep())
    push.connect(ep)

    start = time.monotonic()
    cycles = 0
    delivered = 0
    last_log = start

    while time.monotonic() - start < duration:
        if pull is None:
            pull = _new_pull(ctx)
            bound = False
            for _ in range(40):
                try:
                    pull.bind(ep)
                    bound = True
                    break
                except zmq.ZMQError as exc:
                    if not _is_eaddrinuse(exc):
                        pull.close()
                        raise
                    time.sleep(0.025)
            if not bound:
                pull.close()
                pull = None
                continue

        tag = f"c-{cycles}".encode()
        try:
            push.send(tag)
        except zmq.Again:
            pull.close()
            pull = None
            cycles += 1
            continue

        try:
            msg = pull.recv()
            assert msg == tag
            delivered += 1
        except zmq.Again:
            pass

        pull.close()
        pull = None
        cycles += 1

        if time.monotonic() - last_log >= 30:
            elapsed = time.monotonic() - start
            print(
                f"[reconnect_storm] {elapsed:.0f}s, "
                f"cycles {cycles}, delivered {delivered}"
            )
            last_log = time.monotonic()

    if pull is not None:
        pull.close()
    push.close()
    ctx.term()

    pct = delivered / cycles * 100 if cycles else 100
    print(
        f"[reconnect_storm] done: {delivered}/{cycles} "
        f"delivered ({pct:.1f}%) in {duration:.1f}s"
    )
    report = monitor.stop()
    report.assert_no_leak("reconnect_storm")
