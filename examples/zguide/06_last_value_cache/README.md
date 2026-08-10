# 06 — Last Value Cache

A caching proxy sits between publishers and subscribers. It caches
the latest value for each topic and serves snapshots to late joiners
via REQ/REP.

## Run

    # All-in-one (starts cache, publisher, subscriber):
    ./run.sh

    # Or manually:
    cargo run --bin zg06_cache &
    sleep 0.3
    cargo run --bin zg06_publisher -- ipc://@omq-zguide-06-publisher 5
    sleep 0.5
    cargo run --bin zg06_subscriber

## What it demonstrates

PULL/PUB forwarding with a `HashMap` cache. A late-joining subscriber
sends a SNAPSHOT request via REQ/REP to get the current state before
subscribing for live updates. Three socket types cooperate: PUSH/PULL
for ingestion, PUB/SUB for fan-out, REQ/REP for snapshots.
