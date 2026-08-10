# 07 — Clone

Reliable state synchronization via the Clone pattern. A server
maintains a sequenced key-value store, publishes updates via PUB,
and serves snapshots via REQ/REP. Clients subscribe first, request
a snapshot, then merge buffered live updates by sequence number.

## Run

    # All-in-one (starts server, then client):
    ./run.sh

    # Or manually:
    cargo run --bin zg07_server &
    sleep 0.3
    cargo run --bin zg07_client

## What it demonstrates

The core Clone technique from ZGuide Chapter 5: subscribe before
snapshot so no updates are lost, then discard buffered updates that
the snapshot already includes (seq <= snapshot_seq). PUB/SUB for
fan-out, REQ/REP for point-in-time snapshots.
