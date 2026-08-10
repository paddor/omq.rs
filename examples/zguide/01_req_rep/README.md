# 01 — Request-Reply

Basic REQ/REP echo and a ROUTER/DEALER broker that load-balances
requests across multiple workers.

## Run

    # All-in-one (starts broker, 3 workers, client):
    ./run.sh

    # Or manually:
    cargo run --bin zg01_broker &
    cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 0 &
    cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 1 &
    cargo run --bin zg01_worker -- ipc://@omq-zguide-01-backend 2 &
    cargo run --bin zg01_client

    # Standalone echo (no broker needed):
    cargo run --bin zg01_echo

## Custom endpoints

    cargo run --bin zg01_broker -- tcp://127.0.0.1:5555 tcp://127.0.0.1:5556
    cargo run --bin zg01_worker -- tcp://127.0.0.1:5556 0
    cargo run --bin zg01_client -- tcp://127.0.0.1:5555

## What it demonstrates

The echo binary shows the simplest possible REQ/REP cycle. The
broker/worker/client trio shows how ROUTER/DEALER can sit between
clients and workers, forwarding envelopes transparently so that
multiple workers share the load.
