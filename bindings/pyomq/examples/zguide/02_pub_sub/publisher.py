"""ZGuide 02 — PUB publisher.

Binds a PUB socket and publishes weather and sports data in a loop.
If count is given, publishes that many rounds then exits. Otherwise
runs until interrupted.

    python publisher.py [endpoint] [count]
"""
import sys
import time

import pyomq as zmq

ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-02-pubsub"
count = int(sys.argv[2]) if len(sys.argv) > 2 else None

with zmq.Context() as ctx:
    pub = ctx.socket(zmq.PUB)
    pub.bind(ep)

    print(f"publisher: bound to {ep}")
    time.sleep(0.2)

    i = 0
    while count is None or i < count:
        nyc_temp = 55 + (i % 30)
        sfo_temp = 60 + (i % 20)
        chi_temp = 40 + (i % 35)
        pub.send_string(f"weather.nyc {nyc_temp}F")
        pub.send_string(f"weather.sfo {sfo_temp}F")
        pub.send_string(f"weather.chi {chi_temp}F")
        pub.send_string(f"sports.nba score-{i}")
        time.sleep(0.05)
        i += 1

    print(f"publisher: done ({i} rounds)")
