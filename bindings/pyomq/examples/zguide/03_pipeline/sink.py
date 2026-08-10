"""ZGuide 03 — Sink (result collector).

PULL socket binds and collects results from workers. Prints per-worker
distribution when the expected count is reached.

    python sink.py [sink_ep] [expected_count]
"""
import sys
from collections import defaultdict

import pyomq as zmq

sink_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-03-sink"
expected = int(sys.argv[2]) if len(sys.argv) > 2 else 100

with zmq.Context() as ctx:
    pull = ctx.socket(zmq.PULL)
    pull.setsockopt(zmq.RCVTIMEO, 5000)
    pull.bind(sink_ep)

    print(f"sink: listening on {sink_ep}, expecting {expected} results")

    counts = defaultdict(int)
    received = 0

    for i in range(expected):
        try:
            body = pull.recv_string()
        except zmq.Again:
            print(f"sink: timeout after {received} results")
            break
        received += 1
        worker_id = body.split(":")[0]
        counts[worker_id] += 1
        if received % 25 == 0 or received == expected:
            print(f"sink: received {received}/{expected}")

    print(f"sink: done — {received} results from {len(counts)} workers")
    for worker, count in sorted(counts.items()):
        print(f"  {worker}: {count} items")
