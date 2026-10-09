#!/usr/bin/env python3
"""Run one rival at a time with one peer per process; append measured JSONL."""

import argparse
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from statistics import median

SIZES = {
    "throughput": (
        16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
        32768, 262144, 4194304, 8388608,
    ),
    "latency": (16, 32, 64, 256, 1024, 4096),
}
TRANSPORT = {"aeron": "udp", "zenoh": "tcp", "iroh": "quic"}
IMPL = {name: f"{name}-{transport}-2proc" for name, transport in TRANSPORT.items()}
RESULT = re.compile(r"^impl=(\S+) kind=(\S+) size=(\d+) round=(\d+) (.*)$")
BAD = re.compile(r"\bwarn(?:ing)?\b|timeout", re.IGNORECASE)


def command(name, mode, role, size, port, control, pin):
    if name == "aeron":
        jar = Path(os.environ["AERON_JAR"])
        classes = Path(os.environ["AERON_CLASSES"])
        cmd = [
            "java", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "-cp", f"{classes}:{jar}", "AeronUdpPeer", mode, role,
            str(size), str(port), str(control),
        ]
    else:
        binary = Path(os.environ["CARGO_TARGET_DIR"]) / "release/omq_rivals"
        cmd = [str(binary), name, mode, role, str(size), str(port), str(control)]
    return (["taskset", "-c", "3-4" if role == "server" else "1-2"] + cmd) if pin else cmd


def run_pair(name, mode, size, port, control, pin):
    server = subprocess.Popen(
        command(name, mode, "server", size, port, control, pin),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
    )
    client = None
    sel = selectors.DefaultSelector()
    output = []
    try:
        time.sleep(0.5)
        if server.poll() is not None:
            raise RuntimeError("server exited before client started")
        client = subprocess.Popen(
            command(name, mode, "client", size, port, control, pin),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        for proc, role in ((server, "server"), (client, "client")):
            sel.register(proc.stdout, selectors.EVENT_READ, (role, "stdout"))
            sel.register(proc.stderr, selectors.EVENT_READ, (role, "stderr"))
        deadline = time.monotonic() + 300
        while sel.get_map():
            if time.monotonic() >= deadline:
                raise RuntimeError("benchmark timeout")
            for key, _ in sel.select(timeout=1):
                line = key.fileobj.readline()
                if not line:
                    sel.unregister(key.fileobj)
                    continue
                role, stream = key.data
                print(f"{name} {mode} {size} {role}/{stream}: {line}", end="", flush=True)
                if BAD.search(line):
                    raise RuntimeError("warning or timeout in peer output")
                if role == "client" and stream == "stdout":
                    output.append(line.strip())
            if server.poll() not in (None, 0) or client.poll() not in (None, 0):
                raise RuntimeError("peer failed")
        if server.wait() != 0 or client.wait() != 0:
            raise RuntimeError("peer failed")
        return output
    finally:
        sel.close()
        for proc in (server, client):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


def parse_rounds(lines, name, mode, size):
    rounds = []
    for line in lines:
        match = RESULT.fullmatch(line)
        if match is None:
            continue
        impl_name, kind, got_size, round_number, fields = match.groups()
        if (impl_name, kind, int(got_size)) != (IMPL[name], mode, size):
            raise RuntimeError(f"unexpected result: {line}")
        values = {}
        for item in fields.split():
            key, value = item.split("=", 1)
            values[key] = float(value)
        rounds.append((int(round_number), values))
    expected = [1, 2, 3] if mode == "throughput" else [1]
    if [n for n, _ in rounds] != expected:
        raise RuntimeError(f"expected rounds {expected}; got {rounds}")
    return rounds


def comparison_row(name, mode, size, rounds, run_id):
    row = {
        "run_id": run_id,
        "impl": IMPL[name],
        "kind": mode,
        "transport": TRANSPORT[name],
        "msg_size": size,
    }
    values = [value for _, value in rounds]
    fields = ("msgs_s", "mbps") if mode == "throughput" else (
        "p50_us", "p99_us", "p999_us",
    )
    for field in fields:
        row[field] = median(value[field] for value in values)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", choices=TRANSPORT)
    parser.add_argument("--mode", choices=SIZES, help="run only throughput or latency")
    parser.add_argument("--sizes", help="comma-separated subset of sizes for the selected mode")
    parser.add_argument("--pin", action="store_true", help="client CPUs 1-2, server CPUs 3-4")
    parser.add_argument("--port-base", type=int, default=43100)
    parser.add_argument(
        "--output", type=Path,
        default=Path.home() / ".cache/omq/comparisons.jsonl",
    )
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{args.name}-2proc"
    rows = []
    with tempfile.TemporaryDirectory(prefix="omq-rivals-", dir=os.getenv("TMPDIR")) as temp:
        modes = (args.mode,) if args.mode else SIZES
        requested = None if args.sizes is None else tuple(int(s) for s in args.sizes.split(","))
        if requested is not None and args.mode is None:
            parser.error("--sizes requires --mode")
        for mode in modes:
            sizes = SIZES[mode] if requested is None else requested
            if any(size not in SIZES[mode] for size in sizes):
                parser.error(f"invalid {mode} size")
            for size in sizes:
                port = args.port_base + len(rows) * 2
                control = Path(temp) / f"{args.name}-{mode}-{size}"
                lines = run_pair(args.name, mode, size, port, control, args.pin)
                rounds = parse_rounds(lines, args.name, mode, size)
                rows.append(comparison_row(args.name, mode, size, rounds, run_id))
    with args.output.open("a", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
    print(f"appended {len(rows)} measured rows to {args.output}")


if __name__ == "__main__":
    main()
