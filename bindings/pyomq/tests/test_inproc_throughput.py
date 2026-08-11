"""Smoke test: inproc PUSH/PULL throughput stays above 500k msg/s at 64B."""

import os
import threading
import time

import pytest

import pyomq as zmq

ATTEMPTS = 3
MIN_RATE = 650_000


def _measure_inproc_throughput():
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    ep = f"inproc://tp-{time.monotonic_ns()}"
    pull.bind(ep)
    push.connect(ep)

    n = 40_000
    payload = b"x" * 64

    def sender():
        for _ in range(n):
            push.send(payload)

    t = threading.Thread(target=sender)
    start = time.monotonic()
    t.start()
    for _ in range(n):
        pull.recv()
    elapsed = time.monotonic() - start
    t.join()

    push.close()
    pull.close()
    return n / elapsed


@pytest.mark.skipif(
    os.environ.get("OMQ_SKIP_PERF") == "1",
    reason="OMQ_SKIP_PERF=1 skips local perf gates",
)
def test_inproc_throughput_above_500k():
    best = 0.0
    for _ in range(ATTEMPTS):
        rate = _measure_inproc_throughput()
        best = max(best, rate)
        if best > MIN_RATE:
            return
    assert best > MIN_RATE, (
        f"inproc throughput {best / 1e6:.2f}M msg/s, expected >0.65M"
    )
