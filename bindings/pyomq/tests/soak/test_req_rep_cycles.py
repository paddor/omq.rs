"""Soak: REQ/REP request-reply cycles.

Continuous synchronous request-reply over TCP. Exercises the
send/recv alternation and envelope handling on every cycle.
"""

import threading
import time

import pyomq as zmq

from conftest import ResourceMonitor, soak_duration, tcp_ep


def test_req_rep_cycles():
    duration = soak_duration()
    monitor = ResourceMonitor()

    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    ep = rep.bind(tcp_ep())
    req.connect(ep)
    rep.setsockopt(zmq.RCVTIMEO, 100)
    rep.setsockopt(zmq.SNDTIMEO, 5000)
    req.setsockopt(zmq.RCVTIMEO, 5000)
    req.setsockopt(zmq.SNDTIMEO, 5000)

    stop = False
    cycles = 0

    def server():
        nonlocal stop
        while not stop:
            try:
                msg = rep.recv()
                rep.send(msg)
            except zmq.Again:
                pass

    t = threading.Thread(target=server)
    t.start()

    start = time.monotonic()
    last_log = start

    while time.monotonic() - start < duration:
        tag = f"r-{cycles}".encode()
        try:
            req.send(tag)
            reply = req.recv()
            assert reply == tag
            cycles += 1
        except zmq.Again:
            pass

        now = time.monotonic()
        if now - last_log >= 30:
            print(f"[req_rep] {now - start:.0f}s, cycles {cycles}")
            last_log = now

    stop = True
    t.join(timeout=5)
    assert not t.is_alive(), "REP server thread did not stop"

    print(f"[req_rep] done: {cycles} cycles in {duration:.1f}s")

    req.close()
    rep.close()
    ctx.term()

    report = monitor.stop()
    report.assert_no_leak("req_rep")
