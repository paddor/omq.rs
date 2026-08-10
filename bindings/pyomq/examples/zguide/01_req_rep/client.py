"""ZGuide 01 — REQ client.

Connects to the broker's ROUTER frontend, sends requests, prints replies.

    python client.py [frontend] [n_requests]
"""
import sys

import pyomq as zmq

frontend_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-01-frontend"
n = int(sys.argv[2]) if len(sys.argv) > 2 else 9

with zmq.Context() as ctx:
    req = ctx.socket(zmq.REQ)
    req.connect(frontend_ep)

    for i in range(n):
        req.send_string(f"request-{i}")
        reply = req.recv_string()
        print(f"client: request-{i} -> {reply}")

    print(f"done: {n} replies")
