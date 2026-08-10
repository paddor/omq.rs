"""ZGuide 01 — Basic REQ/REP echo.

Single-process demo: REP server echoes messages back to a REQ client.

    python echo.py [endpoint]
"""
import sys
import threading
import time

import pyomq as zmq

ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-01-echo"

with zmq.Context() as ctx:
    rep = ctx.socket(zmq.REP)
    rep.bind(ep)

    req = ctx.socket(zmq.REQ)
    req.connect(ep)
    time.sleep(0.05)

    def server():
        for _ in range(3):
            body = rep.recv_string()
            rep.send_string("echo:" + body)

    t = threading.Thread(target=server, daemon=True)
    t.start()

    for i in range(3):
        req.send_string(f"hello-{i}")
        reply = req.recv_string()
        print(f"client: hello-{i} -> {reply}")

    t.join()
    print("done: 3 request-reply cycles")
