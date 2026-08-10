# 03 — Pipeline (Divide and Conquer)

A ventilator pushes work items to workers via PUSH/PULL.
Workers process items and push results to a sink.
Demonstrates fan-out/fan-in with load balancing across workers.

## Run

    # All-in-one (starts sink, 3 workers, ventilator):
    ./run.sh

    # Or manually:
    cargo run --bin zg03_sink &
    cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 0 &
    cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 1 &
    cargo run --bin zg03_worker -- ipc://@omq-zguide-03-ventilator ipc://@omq-zguide-03-sink 2 &
    cargo run --bin zg03_ventilator

## Custom endpoints

    cargo run --bin zg03_sink -- tcp://127.0.0.1:5558 100
    cargo run --bin zg03_worker -- tcp://127.0.0.1:5557 tcp://127.0.0.1:5558 0
    cargo run --bin zg03_ventilator -- tcp://127.0.0.1:5557 100 3

## What it demonstrates

The ventilator distributes tasks round-robin across connected PULL
workers. Each worker tags its output with an ID and forwards to the
sink. The sink collects all results and reports per-worker counts,
showing that work was balanced across workers.
