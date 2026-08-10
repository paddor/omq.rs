# 11 — Freelance

Brokerless reliability: client talks directly to multiple servers.
Three models:

1. **Sequential failover** -- try servers in order, skip on timeout.
2. **Shotgun** -- blast request to all servers, take first reply.
3. **Tracked** -- remember which server is alive, prefer it.

## Run

    # All three demos back-to-back:
    ./run.sh

    # Or manually:

    # Model 1:
    cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 server2 0 &
    cargo run --bin zg11_client_sequential -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2

    # Model 2:
    cargo run --bin zg11_server -- ipc://@omq-zguide-11-server1 fast 0 &
    cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 slow 0.3 &
    cargo run --bin zg11_client_shotgun -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2

    # Model 3:
    cargo run --bin zg11_server -- ipc://@omq-zguide-11-server1 server1 0 &
    cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 server2 0 &
    cargo run --bin zg11_client_tracked -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2

## What it demonstrates

Direct client-to-server reliability without a broker. Model 1 is
simplest (try each server in turn). Model 2 minimizes latency by
racing all servers. Model 3 combines fast-path (sticky routing to
known-good server) with failover on timeout.
