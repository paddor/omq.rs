#!/usr/bin/env python3
import sys
import time
from pathlib import Path

import zmq


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: pyzmq_pull_once.py <endpoint-file> <payload-file>", file=sys.stderr)
        return 2
    endpoint_file = Path(sys.argv[1])
    payload_file = Path(sys.argv[2])

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.linger = 0
    sock.rcvtimeo = 5000
    port = sock.bind_to_random_port("tcp://127.0.0.1")
    endpoint_file.write_text(f"tcp://127.0.0.1:{port}", encoding="utf-8")
    payload = sock.recv()
    payload_file.write_bytes(payload)
    sock.close()
    ctx.term()
    time.sleep(0.01)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
