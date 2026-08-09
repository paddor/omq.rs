#!/usr/bin/env python3
"""Measure OMQ.java vs JeroMQ PUSH/PULL throughput over TCP loopback."""

import argparse
import datetime as dt
import json
import os
import selectors
import socket
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
CLASS = "io.omq.perf.PushPullTcpPeer"
DEFAULT_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [8, 128, 1024, 4096, 32768]
DEFAULT_IMPLS = ["omq", "omq-into", "jeromq", "jeromq-into"]
JSONL = (
    Path(os.environ.get("OMQ_JAVA_CACHE_DIR", Path.home() / ".cache" / "omq.java"))
    / "pushpull-tcp.jsonl"
)


def parse_csv_ints(value):
    return [int(part) for part in value.split(",") if part]


def parse_csv_strings(value):
    return [part for part in value.split(",") if part]


def run(cmd, cwd=ROOT, timeout=None):
    print("+ " + " ".join(str(part) for part in cmd), flush=True)
    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        raise SystemExit(result.returncode)
    if result.stdout:
        sys.stdout.write(result.stdout)


def build(args):
    if args.no_build:
        return
    run(
        [
            "cargo",
            "build",
            "--release",
            "--features",
            "plain,curve,lz4,zstd",
            "--manifest-path",
            str(ROOT / "native" / "Cargo.toml"),
        ],
        cwd=REPO,
    )
    run(
        [
            "mvn",
            "-q",
            "-f",
            str(ROOT / "pom.xml"),
            "-DskipTests",
            "test-compile",
            "dependency:build-classpath",
            "-Dmdep.outputFile=target/perf-classpath.txt",
        ],
        cwd=REPO,
    )


def classpath():
    deps_file = ROOT / "target" / "perf-classpath.txt"
    deps = deps_file.read_text().strip() if deps_file.exists() else ""
    parts = [ROOT / "target" / "classes", ROOT / "target" / "test-classes"]
    if deps:
        parts.append(deps)
    return os.pathsep.join(str(part) for part in parts)


def free_endpoint():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    finally:
        sock.close()
    return f"tcp://127.0.0.1:{port}"


def message_count(size, args):
    by_bytes = args.target_bytes // max(size, 1)
    return max(args.min_messages, min(args.max_messages, by_bytes))


def java_cmd(cp, impl, role, endpoint, size, messages, warmup, batch):
    return [
        "java",
        "-Djava.library.path=" + str(ROOT / "native" / "target" / "release"),
        "-cp",
        cp,
        CLASS,
        impl,
        role,
        endpoint,
        str(size),
        str(messages),
        str(warmup),
        str(batch),
    ]


def read_line_timeout(proc, seconds):
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    try:
        events = selector.select(seconds)
        if not events:
            return None
        return proc.stdout.readline()
    finally:
        selector.close()


def parse_result(output):
    for line in output.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT ") :])
    raise RuntimeError("missing RESULT line")


def fail_on_noise(name, stdout, stderr):
    text = (stdout or "") + "\n" + (stderr or "")
    lowered = text.lower()
    if "warning" in lowered or "timeout" in lowered:
        raise RuntimeError(f"{name} printed warning/timeout:\n{text}")


def kill(proc):
    if proc.poll() is None:
        proc.kill()
        proc.communicate(timeout=5)


def run_cell_once(cp, impl, size, messages, warmup, batch, timeout):
    endpoint = free_endpoint()
    pull = subprocess.Popen(
        java_cmd(cp, impl, "pull", endpoint, size, messages, warmup, batch),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        ready = read_line_timeout(pull, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = pull.communicate(timeout=1) if pull.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        push = subprocess.run(
            java_cmd(cp, impl, "push", endpoint, size, messages, warmup, batch),
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        fail_on_noise("sender", push.stdout, push.stderr)
        if push.returncode != 0:
            raise RuntimeError(f"sender failed:\n{push.stdout}{push.stderr}")

        out, err = pull.communicate(timeout=timeout)
        out = ready + out
        fail_on_noise("receiver", out, err)
        if pull.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        return parse_result(out)
    except Exception:
        kill(pull)
        raise


def run_cell(cp, impl, size, args):
    messages = message_count(size, args)
    warmup = args.warmup_messages if args.warmup_messages is not None else max(1000, messages // 20)
    runs = []
    total = args.warmup_rounds + args.rounds
    for round_index in range(total):
        result = run_cell_once(cp, impl, size, messages, warmup, args.batch_size, args.timeout)
        if round_index >= args.warmup_rounds:
            runs.append(result)
        print(
            f"  {impl:8s} size={size:6d} round={round_index + 1}/{total} "
            f"{result['msgs_s']:12.0f} msg/s {result['gb_s']:7.3f} GB/s",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["msgs_s"])[len(runs) // 2]


def append_jsonl(rows):
    JSONL.parent.mkdir(parents=True, exist_ok=True)
    with JSONL.open("a") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")


def print_table(rows, sizes, impls):
    by_key = {(row["msg_size"], row["impl"]): row for row in rows}
    print()
    print("size  impl          msg/s      GB/s   vs jeromq")
    for size in sizes:
        base = by_key.get((size, "jeromq"))
        base_msgs = base["msgs_s"] if base else 0.0
        for impl in impls:
            row = by_key[(size, impl)]
            ratio = row["msgs_s"] / base_msgs if base_msgs else 0.0
            print(
                f"{size:5d} {impl:8s} {row['msgs_s']:11.0f} "
                f"{row['gb_s']:8.3f} {ratio:9.2f}x"
            )
        print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes", type=parse_csv_ints)
    parser.add_argument("--impls", type=parse_csv_strings, default=DEFAULT_IMPLS)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--target-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--min-messages", type=int, default=20_000)
    parser.add_argument("--max-messages", type=int, default=1_000_000)
    parser.add_argument("--warmup-messages", type=int)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    build(args)
    cp = classpath()
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rows = []

    print(f"run_id={run_id}")
    for size in sizes:
        for impl in args.impls:
            row = run_cell(cp, impl, size, args)
            row["run_id"] = run_id
            row["kind"] = "pushpull_tcp"
            rows.append(row)

    append_jsonl(rows)
    print_table(rows, sizes, args.impls)
    print(f"appended {len(rows)} rows to {JSONL}")


if __name__ == "__main__":
    main()
