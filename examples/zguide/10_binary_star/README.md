# 10 — Binary Star

Active/passive high-availability pair. The primary server handles
client requests while sending heartbeats to a backup via PUB/SUB.
When the primary dies, the backup detects heartbeat loss and takes
over. The client retries against the backup on timeout.

## Run

    # All-in-one (starts primary, backup, sends requests, kills primary,
    # sends more requests to demonstrate failover):
    ./run.sh

    # Or manually:
    cargo run --bin zg10_primary &
    cargo run --bin zg10_backup &
    sleep 0.5
    cargo run --bin zg10_client -- ipc://@omq-zguide-10-primary ipc://@omq-zguide-10-backup 2
    kill %1          # kill primary
    sleep 0.5
    cargo run --bin zg10_client -- ipc://@omq-zguide-10-primary ipc://@omq-zguide-10-backup 2

## Custom endpoints

    cargo run --bin zg10_primary -- tcp://127.0.0.1:5555 tcp://127.0.0.1:5556
    cargo run --bin zg10_backup -- tcp://127.0.0.1:5556 tcp://127.0.0.1:5557
    cargo run --bin zg10_client -- tcp://127.0.0.1:5555 tcp://127.0.0.1:5557

## What it demonstrates

Three processes cooperate for high availability. The primary binds
REP + PUB. The backup subscribes to heartbeats and binds its own
REP; it stays passive until heartbeats stop. The client creates a
fresh REQ per request and falls back to the backup on timeout.
