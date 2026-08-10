"""ZGuide 01 — REP worker.

Connects to the broker's DEALER backend. Echoes requests with a
worker-ID prefix. Runs until interrupted.

    python worker.py [backend] [id]
"""
import sys

import pyomq as zmq

backend_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-01-backend"
worker_id = sys.argv[2] if len(sys.argv) > 2 else "0"

with zmq.Context() as ctx:
    rep = ctx.socket(zmq.REP)
    rep.connect(backend_ep)

    print(f"worker-{worker_id}: ready")

    while True:
        body = rep.recv_string()
        reply = f"worker-{worker_id}:{body}"
        print(f"worker-{worker_id}: {body} -> {reply}")
        rep.send_string(reply)
