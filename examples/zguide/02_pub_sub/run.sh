#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

cargo run --bin zg02_publisher -- ipc://@omq-zguide-02-pubsub 20 &
sleep 0.3
cargo run --bin zg02_subscriber -- ipc://@omq-zguide-02-pubsub weather.nyc 10 &
cargo run --bin zg02_subscriber -- ipc://@omq-zguide-02-pubsub weather.sfo 10 &
wait
