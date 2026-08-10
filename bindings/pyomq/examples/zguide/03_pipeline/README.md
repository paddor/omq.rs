# 03 — Pipeline (Divide and Conquer)

A ventilator pushes work items to workers via PUSH/PULL. Workers
process items and push results to a sink. Demonstrates fan-out/fan-in
with load balancing across workers.

## Run

    # All-in-one (starts sink, 3 workers, ventilator):
    ./run.sh

    # Or manually:
    python sink.py &
    python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
    python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
    python worker.py ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
    python ventilator.py

## Custom endpoints

    python sink.py tcp://127.0.0.1:5558 100
    python worker.py tcp://127.0.0.1:5557 tcp://127.0.0.1:5558 0
    python ventilator.py tcp://127.0.0.1:5557 100 3

## What it demonstrates

The ventilator distributes tasks round-robin across connected PULL
workers. Each worker tags its output with an ID and pushes to the
sink. The sink collects all results and reports per-worker counts,
showing that work was balanced across workers.
