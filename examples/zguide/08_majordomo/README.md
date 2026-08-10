# 08 — Majordomo

Service-oriented broker. Workers register by service name, clients
request a service by name, and the broker routes to the right worker
pool using LRU dispatch.

## Run

    # All-in-one (starts broker, 2 echo workers, 1 upper worker, client):
    ./run.sh

    # Or manually:
    cargo run --bin zg08_broker -- ipc://@omq-zguide-08-frontend ipc://@omq-zguide-08-backend 3 &
    cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend echo 0 &
    cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend echo 1 &
    cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend upper 0 &
    cargo run --bin zg08_client

## What it demonstrates

Two ROUTER sockets in the broker provide full envelope control.
Workers use DEALER with explicit identities and register with a
`["READY", service]` message. The broker maintains per-service worker
pools and routes client requests by service name, returning workers
to the pool after each reply.
