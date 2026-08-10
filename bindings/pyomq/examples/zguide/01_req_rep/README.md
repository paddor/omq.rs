# 01 — Request-Reply

Basic REQ/REP echo and a ROUTER/DEALER broker that load-balances
requests across multiple workers.

## Run

    # All-in-one (starts broker, 3 workers, client):
    ./run.sh

    # Or manually:
    python broker.py &
    python worker.py ipc://@omq-zguide-01-backend 0 &
    python worker.py ipc://@omq-zguide-01-backend 1 &
    python worker.py ipc://@omq-zguide-01-backend 2 &
    python client.py

    # Standalone echo (no broker needed):
    python echo.py

## Custom endpoints

    python broker.py tcp://127.0.0.1:5555 tcp://127.0.0.1:5556
    python worker.py tcp://127.0.0.1:5556 0
    python client.py tcp://127.0.0.1:5555

## What it demonstrates

The echo script shows the simplest possible REQ/REP cycle. The
broker/worker/client trio shows how ROUTER/DEALER can sit between
clients and workers, forwarding envelopes transparently so multiple
workers share the load.
