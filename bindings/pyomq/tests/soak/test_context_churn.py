"""Soak: Context and Socket creation/teardown churn.

Rapidly create and destroy Context + Socket pairs. This is the most
direct test of the PyO3 reference counting and drop lifecycle: every
cycle allocates Rust state, wraps it in Python objects, then drops
everything. Leaks in the binding layer show up here first.
"""

import time

import pyomq as zmq
from conftest import ResourceMonitor, soak_duration


def test_context_churn():
    duration = soak_duration()
    monitor = ResourceMonitor()

    cycles = 0
    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        ep = f"inproc://ctx-churn-{cycles}"
        ctx = zmq.Context()
        pull = ctx.socket(zmq.PULL)
        push = ctx.socket(zmq.PUSH)
        pull.bind(ep)
        push.connect(ep)

        pull.setsockopt(zmq.RCVTIMEO, 5000)
        push.send(b"x")
        msg = pull.recv()
        assert msg == b"x"

        push.close()
        pull.close()
        ctx.term()
        cycles += 1

        now = time.monotonic()
        if now - last_log >= 30:
            print(f"[ctx_churn] {now - start:.0f}s, cycles {cycles}")
            last_log = now

    print(f"[ctx_churn] done: {cycles} cycles in {duration:.1f}s")

    report = monitor.stop()
    report.assert_no_leak("ctx_churn")
