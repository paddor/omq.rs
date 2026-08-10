# 05 — Heartbeat

PUB/SUB liveness detection. The publisher sends periodic heartbeats,
pauses to simulate a failure, then resumes. The monitor detects the
alive, dead, and recovered state transitions.

## Run

    ./run.sh

    # Or manually:
    cargo run --bin zg05_publisher &
    cargo run --bin zg05_monitor

## Custom endpoints

    cargo run --bin zg05_publisher -- tcp://127.0.0.1:5555 &
    cargo run --bin zg05_monitor -- tcp://127.0.0.1:5555

## What it demonstrates

Heartbeat-based liveness monitoring over PUB/SUB. The monitor uses a
timeout of 150ms (3x the 50ms heartbeat interval) to detect when the
publisher goes silent, and recognizes recovery when heartbeats resume.
