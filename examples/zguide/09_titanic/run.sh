#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
STORE=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$STORE"' EXIT

cargo run --bin zg09_frontend -- ipc://@omq-zguide-09-frontend ipc://@omq-zguide-09-dispatch "$STORE" &
sleep 0.3
cargo run --bin zg09_dispatcher -- ipc://@omq-zguide-09-dispatch "$STORE" &
sleep 0.3
cargo run --bin zg09_client
