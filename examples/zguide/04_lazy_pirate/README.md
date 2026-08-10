# 04 — Lazy Pirate

Client-side reliability via timeout, retry, and socket recreation.
The server simulates a crash (800ms delay) on request #3. The client
detects the timeout, drops the socket, creates a new one, and retries.

## Run

    ./run.sh

    # Or manually:
    cargo run --bin zg04_server &
    sleep 0.3
    cargo run --bin zg04_client

## Custom endpoints

    cargo run --bin zg04_server -- tcp://127.0.0.1:5555 &
    sleep 0.3
    cargo run --bin zg04_client -- tcp://127.0.0.1:5555

## What it demonstrates

The Lazy Pirate pattern: a REQ client that handles unresponsive servers
by timing out, destroying the socket, and reconnecting with a fresh one.
Each request is retried up to 3 times before giving up.
