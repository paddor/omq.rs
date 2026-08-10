"""ZGuide 02 — SUB subscriber.

Connects a SUB socket, subscribes to a topic prefix, and prints
matching messages. If count is given, exits after that many messages.

    python subscriber.py [endpoint] [topic] [count]
"""
import sys

import pyomq as zmq

ep = sys.argv[1] if len(sys.argv) > 1 else "ipc://@omq-zguide-02-pubsub"
topic = sys.argv[2] if len(sys.argv) > 2 else "weather.nyc"
count = int(sys.argv[3]) if len(sys.argv) > 3 else None

with zmq.Context() as ctx:
    sub = ctx.socket(zmq.SUB)
    sub.connect(ep)
    sub.subscribe(topic.encode())

    print(f"subscriber: connected to {ep}, topic={topic!r}")

    i = 0
    while count is None or i < count:
        body = sub.recv_string()
        print(f"subscriber[{topic}]: [{i}] {body}")
        i += 1

    print(f"subscriber: done ({i} messages)")
