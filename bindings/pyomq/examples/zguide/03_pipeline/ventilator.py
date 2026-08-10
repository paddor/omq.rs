"""ZGuide 03 — Ventilator (task producer).

PUSH socket binds and sends tasks followed by END sentinels so each
worker knows when to stop.

    python ventilator.py [vent_ep] [n_tasks] [n_workers]
"""
import sys
import time

import pyomq as zmq

vent_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-03-ventilator"
n_tasks = int(sys.argv[2]) if len(sys.argv) > 2 else 100
n_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 3

with zmq.Context() as ctx:
    push = ctx.socket(zmq.PUSH)
    push.bind(vent_ep)
    time.sleep(0.2)

    for i in range(n_tasks):
        push.send_string(f"task-{i}")

    for _ in range(n_workers):
        push.send(b"END")

    print(f"ventilator: sent {n_tasks} tasks + {n_workers} END sentinels on {vent_ep}")
