# 02 — Publish-Subscribe

PUB/SUB fan-out with topic prefix filtering and a SUB/PUB forwarding
proxy.

## Run

    # All-in-one (publisher + 2 subscribers):
    ./run.sh

    # Or manually:
    python publisher.py ipc://@omq-zguide-02-pubsub 20 &
    sleep 0.3
    python subscriber.py ipc://@omq-zguide-02-pubsub weather.nyc
    python subscriber.py ipc://@omq-zguide-02-pubsub weather.sfo

    # With proxy:
    python publisher.py ipc://@omq-zguide-02-upstream &
    sleep 0.3
    python proxy.py ipc://@omq-zguide-02-upstream ipc://@omq-zguide-02-downstream &
    sleep 0.3
    python subscriber.py ipc://@omq-zguide-02-downstream weather.nyc

## Custom endpoints

    python publisher.py tcp://127.0.0.1:5555 20
    python subscriber.py tcp://127.0.0.1:5555 weather.nyc

## What it demonstrates

The publisher binds a PUB socket and sends weather updates for three
cities plus sports scores. Subscribers connect with a topic prefix and
only receive matching messages. The proxy forwards all messages from
an upstream PUB to downstream subscribers using a SUB/PUB relay.
