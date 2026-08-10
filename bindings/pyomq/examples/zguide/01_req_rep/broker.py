"""ZGuide 01 — ROUTER/DEALER broker.

Forwards messages between a ROUTER frontend (clients) and a DEALER
backend (workers). Runs until interrupted.

    python broker.py [frontend] [backend]
"""
import sys

import pyomq as zmq

frontend_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-01-frontend"
backend_ep = sys.argv[2] if len(sys.argv) > 2 else "ipc://@omq-zguide-01-backend"

with zmq.Context() as ctx:
    frontend = ctx.socket(zmq.ROUTER)
    frontend.bind(frontend_ep)

    backend = ctx.socket(zmq.DEALER)
    backend.bind(backend_ep)

    print(f"broker: frontend={frontend_ep} backend={backend_ep}")
    zmq.proxy(frontend, backend)
