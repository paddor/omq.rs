"""ZGuide 03 — Pipeline worker.

PULL from ventilator, PUSH to sink. Forwards each task with a worker-ID
prefix. Exits on END sentinel.

    python worker.py [vent_ep] [sink_ep] [worker_id]
"""
import sys

import pyomq as zmq

vent_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-03-ventilator"
sink_ep = sys.argv[2] if len(sys.argv) > 2 else "ipc://@omq-zguide-03-sink"
worker_id = sys.argv[3] if len(sys.argv) > 3 else "0"

with zmq.Context() as ctx:
    pull = ctx.socket(zmq.PULL)
    pull.setsockopt(zmq.RCVTIMEO, 5000)
    pull.connect(vent_ep)

    push = ctx.socket(zmq.PUSH)
    push.connect(sink_ep)

    print(f"worker-{worker_id}: ready")

    while True:
        try:
            body = pull.recv_string()
        except zmq.Again:
            print(f"worker-{worker_id}: timeout, exiting")
            break
        if body == "END":
            print(f"worker-{worker_id}: done")
            break
        push.send_string(f"worker-{worker_id}:{body}")
