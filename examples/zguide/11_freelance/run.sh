#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

echo "=== Model 1: Sequential Failover ==="
cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 server2 0 &
S2=$!
sleep 0.2
cargo run --bin zg11_client_sequential -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2 ipc://@omq-zguide-11-server3
kill $S2 2>/dev/null || true; wait $S2 2>/dev/null || true

echo ""
echo "=== Model 2: Shotgun ==="
cargo run --bin zg11_server -- ipc://@omq-zguide-11-server1 fast 0 &
S1=$!
cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 slow 0.3 &
S2=$!
sleep 0.2
cargo run --bin zg11_client_shotgun -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2
kill $S1 $S2 2>/dev/null || true; wait $S1 $S2 2>/dev/null || true

echo ""
echo "=== Model 3: Tracked ==="
cargo run --bin zg11_server -- ipc://@omq-zguide-11-server1 server1 0 &
S1=$!
cargo run --bin zg11_server -- ipc://@omq-zguide-11-server2 server2 0 &
S2=$!
sleep 0.2
cargo run --bin zg11_client_tracked -- ipc://@omq-zguide-11-server1 ipc://@omq-zguide-11-server2 &
CLIENT=$!
sleep 0.8
kill $S1 2>/dev/null || true
echo "--- server1 killed ---"
wait $CLIENT 2>/dev/null || true
kill $S2 2>/dev/null || true
