# 09 — Titanic

Disconnected reliability via disk-based store-and-forward. The frontend
persists requests to disk before forwarding to the dispatcher. Clients
get a ticket ID and poll for results later.

## Run

    # All-in-one (starts frontend, dispatcher, client):
    ./run.sh

    # Or manually:
    STORE=$(mktemp -d)
    cargo run --bin zg09_frontend -- ipc://@omq-zguide-09-frontend ipc://@omq-zguide-09-dispatch $STORE &
    sleep 0.3
    cargo run --bin zg09_dispatcher -- ipc://@omq-zguide-09-dispatch $STORE &
    sleep 0.3
    cargo run --bin zg09_client

## Custom endpoints

    cargo run --bin zg09_frontend -- tcp://127.0.0.1:5555 tcp://127.0.0.1:5556 /tmp/my-store &
    cargo run --bin zg09_dispatcher -- tcp://127.0.0.1:5556 /tmp/my-store &
    cargo run --bin zg09_client -- tcp://127.0.0.1:5555

## What it demonstrates

Three processes cooperate: the frontend (REP + PUSH) accepts client
requests and writes them to disk, the dispatcher (PULL) reads pending
requests and writes results, and the client (REQ) submits work and
polls for results by ticket ID. Two services are supported: `echo`
(prepends "echo:") and `upper` (uppercases the body).
