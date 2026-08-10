"""ZGuide 02 — SUB/PUB forwarding proxy.

Connects a SUB socket upstream (subscribes to all topics) and binds a
PUB socket downstream. Runs until interrupted.

    python proxy.py [upstream] [downstream]
"""
import sys

import pyomq as zmq

upstream_ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-02-upstream"
downstream_ep = sys.argv[2] if len(sys.argv) > 2 else "ipc://@omq-zguide-02-downstream"

with zmq.Context() as ctx:
    upstream = ctx.socket(zmq.SUB)
    upstream.connect(upstream_ep)
    upstream.subscribe(b"")

    downstream = ctx.socket(zmq.PUB)
    downstream.bind(downstream_ep)

    print(f"proxy: upstream={upstream_ep} downstream={downstream_ep}")
    zmq.proxy(upstream, downstream)
