#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

cargo run --bin zg08_broker -- ipc://@omq-zguide-08-frontend ipc://@omq-zguide-08-backend 3 &
sleep 0.3
cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend echo 0 &
cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend echo 1 &
cargo run --bin zg08_worker -- ipc://@omq-zguide-08-backend upper 0 &
sleep 0.3
cargo run --bin zg08_client
