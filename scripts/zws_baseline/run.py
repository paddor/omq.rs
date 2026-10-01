#!/usr/bin/env python3
"""Serial WS/WSS/TCP OMQ measurements using frozen comparison-peer executables."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import selectors
import signal
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
BAD = re.compile(r"warning|panic|timed out|timeout|error:|lagged|failed", re.I)


def cases():
    result = []
    for transport in ("tcp", "ws", "wss"):
        for direction in ("server-send", "client-send"):
            for size in (16, 1024, 4096, 65536, 1048576):
                result.append(dict(suite="core", transport=transport, direction=direction,
                                   pattern="push", size=size, peers=1, io_threads=1))
        for pattern in ("pub", "fanin"):
            for peers, size in ((8, 16), (8, 4096), (32, 1024)):
                result.append(dict(suite="scaling", transport=transport,
                                   direction="server-send" if pattern == "pub" else "client-send",
                                   pattern=pattern, size=size, peers=peers, io_threads=2))
        for size in (16, 4096):
            result.append(dict(suite="latency", transport=transport, direction="round-trip",
                               pattern="latency", size=size, peers=1, io_threads=1))
    for transport in ("lz4+tcp", "lz4+ws", "zstd+tcp"):
        for size in (1024, 65536):
            result.append(dict(suite="codec", transport=transport, direction="server-send",
                               pattern="push", size=size, peers=1, io_threads=1))
    return result


def case_id(case):
    return "-".join(str(case[key]) for key in
                    ("transport", "pattern", "direction", "size", "peers", "io_threads"))


def credentials(folder):
    folder.mkdir(parents=True, exist_ok=True)
    cert, key = folder / "cert.pem", folder / "key.pem"
    if not cert.exists() or not key.exists():
        subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt",
                        "ec_paramgen_curve:P-256", "-nodes", "-days", "30", "-subj",
                        "/CN=localhost", "-addext", "basicConstraints=critical,CA:FALSE",
                        "-addext", "keyUsage=critical,digitalSignature",
                        "-addext", "extendedKeyUsage=serverAuth",
                        "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                        "-keyout", str(key), "-out", str(cert)], check=True)
        key.chmod(0o600)
    return {"OMQ_BENCH_TLS_CERT_FILE": str(cert), "OMQ_BENCH_TLS_KEY_FILE": str(key),
            "OMQ_BENCH_TLS_TRUST_FILE": str(cert), "OMQ_BENCH_TLS_NAME": "localhost"}


def roles(case, duration, iterations):
    size, peers = str(case["size"]), str(case["peers"])
    if case["pattern"] == "latency":
        return ["rep", size], ["req", size, str(iterations), "1000"], "connector"
    if case["direction"] == "client-send":
        sender = ["multi-push", size, peers] if case["pattern"] == "fanin" else ["push-connect", size]
        return ["pull-bind", size, str(duration)], sender, "listener"
    if case["pattern"] == "pub":
        return ["pub", size, peers], ["multi-sub", size, str(duration), peers], "connector"
    return ["push", size, peers], ["pull", size, str(duration)], "connector"


class Peers:
    def __init__(self, binary, env, profile):
        self.binary, self.env, self.profile = binary, env, profile
        self.processes, self.logs, self.errors, self.watchers = {}, {}, [], []

    def stop(self):
        for process in self.processes.values():
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass

    def start(self, side, role, endpoint):
        command = ["taskset", "-c", "0-2" if side == "listener" else "3-5"]
        if self.profile:
            command += ["perf", "record", "--quiet", "-e", "cpu-clock", "-F", "199",
                        "--mmap-pages", "256", "--call-graph", "dwarf,16384", "-o",
                        str(self.profile / f"{side}.data"), "--"]
        command += [str(self.binary), role[0], endpoint, *role[1:]]
        process = subprocess.Popen(command, env=self.env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        self.processes[side], self.logs[side] = process, []

        def watch():
            for line in process.stderr:
                self.logs[side].append(line.rstrip())
                if BAD.search(line):
                    self.errors.append(f"{side}: {line.rstrip()}")
                    print(self.errors[-1], flush=True)
                    self.stop()

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        self.watchers.append(watcher)
        return process

    def finish(self):
        self.stop()
        for process in self.processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                self.errors.append("peer failed to stop")
        for watcher in self.watchers:
            watcher.join(timeout=2)


def read_ready(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(15):
            raise RuntimeError("listener readiness timeout")
    line = process.stdout.readline().strip()
    if not line.startswith("PORT "):
        raise RuntimeError(f"expected bound port, got {line!r}")
    return int(line.split()[1])


def check_profiles(folder):
    for side in ("listener", "connector"):
        result = subprocess.run(["perf", "report", "--stdio", "--no-children", "--call-graph",
                                 "none", "--percent-limit", "1", "-i", str(folder / f"{side}.data")],
                                check=True, capture_output=True, text=True)
        if result.stderr or re.search(r"Total Lost Samples:\s*[1-9]", result.stdout):
            raise RuntimeError(f"profile warning/lost samples: {result.stderr or result.stdout}")
        (folder / f"{side}.txt").write_text(result.stdout)


def run_case(args, binary, label, case, round_number, tls):
    profile = args.profile / f"{case_id(case)}-{label}-{round_number}" if args.profile else None
    if profile:
        profile.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith("OMQ_")}
    env.update(tls)
    env.update(OMQ_IO_THREADS=str(case["io_threads"]), OMQ_BENCH_PAYLOAD="json",
               OMQ_BENCH_START_AT=str(time.time() + 1.5), OMQ_BENCH_WARMUP_MS="500")
    listener_role, connector_role, measured_side = roles(case, args.duration, args.iterations)
    peers = Peers(binary, env, profile)
    cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    try:
        suffix = "/" if case["transport"].endswith(("ws", "wss")) else ""
        listener = peers.start("listener", listener_role, f'{case["transport"]}://127.0.0.1:0{suffix}')
        port = read_ready(listener)
        peers.start("connector", connector_role, f'{case["transport"]}://127.0.0.1:{port}{suffix}')
        measured = peers.processes[measured_side]
        while measured.poll() is None:
            if peers.errors:
                raise RuntimeError("; ".join(peers.errors))
            if time.monotonic() - started > 40:
                raise RuntimeError("measurement timeout")
            time.sleep(0.02)
        if measured.returncode != 0:
            raise RuntimeError(f"measurement exit {measured.returncode}")
        lines = [line for line in measured.stdout.read().splitlines() if line.strip()]
        values = [float(x) for x in lines[0].split()]
    finally:
        peers.finish()
    if peers.errors:
        raise RuntimeError("; ".join(peers.errors))
    cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    row = dict(case, case=case_id(case), label=label, round=round_number,
               timestamp=time.time(), binary=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
               revision=args.revision, duration=args.duration, iterations=args.iterations,
               cpu_masks={"listener": "0-2", "connector": "3-5"}, profile=str(profile) if profile else None,
               cpu_children_total_s=cpu_after.ru_utime+cpu_after.ru_stime-cpu_before.ru_utime-cpu_before.ru_stime,
               raw=values, stderr=peers.logs)
    if case["pattern"] == "latency":
        row.update(p50_us=values[0], p99_us=values[1], p999_us=values[2], cpu_receiver_measure_s=values[5])
        assert int(values[4]) == args.iterations
    else:
        count, elapsed, size, cpu = values[:4]
        if not (count > 0 and elapsed >= args.duration * 0.99 and size == case["size"]):
            raise RuntimeError(f"invalid delivered measurement: {values}; logs={peers.logs}")
        row.update(delivered=int(count), elapsed_s=elapsed, messages_s=count/elapsed,
                   payload_gb_s=count*size/elapsed/1e9, cpu_receiver_measure_s=cpu)
        if case["pattern"] == "pub":
            assert int(values[4]) == case["peers"] and values[5] > 0
            row["per_subscriber_min_messages_s"] = values[5]
            row["per_subscriber_max_messages_s"] = values[6]
    if profile:
        check_profiles(profile)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, help="LABEL=/absolute/frozen/executable")
    parser.add_argument("--suite", default="all", choices=("all", "core", "scaling", "latency", "codec"))
    parser.add_argument("--case", default="", help="Substring of case identifier")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--duration", type=float, default=1.5)
    parser.add_argument("--iterations", type=int, default=20000)
    parser.add_argument("--revision", default=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--output", type=Path, default=Path.home()/".cache/omq/zws-baseline.jsonl")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    selected = [c for c in cases() if (args.suite == "all" or c["suite"] == args.suite) and args.case in case_id(c)]
    if not selected:
        parser.error("no matching cases")
    if args.list:
        print("\n".join(case_id(c) for c in selected))
        return
    binaries = [(label, Path(path).resolve(strict=True)) for label, path in (s.split("=", 1) for s in args.binary)]
    tls = credentials(ROOT/"tmp/zws-baseline/tls")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for round_number in range(1, args.rounds + 1):
        ordered = selected if round_number % 2 else list(reversed(selected))
        for case in ordered:
            for label, binary in binaries if round_number % 2 else reversed(binaries):
                print(f"RUN {round_number} {label} {case_id(case)}", flush=True)
                row = run_case(args, binary, label, case, round_number, tls)
                with args.output.open("a") as out:
                    out.write(json.dumps(row) + "\n")
                print(json.dumps({k: row[k] for k in ("case", "label", "round", "payload_gb_s", "messages_s", "p50_us", "p99_us") if k in row}), flush=True)


if __name__ == "__main__":
    main()
