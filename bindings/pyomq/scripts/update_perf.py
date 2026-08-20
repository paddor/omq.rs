#!/usr/bin/env python3
"""Measure pyomq vs pyzmq throughput and latency (sync + async).

Run from the pyomq root (bindings/pyomq/) after `maturin develop --release`.
Full runs append to doc/charts/bindings.jsonl (latest run_id wins per impl).
They also generate doc/charts/bindings.svg and update the README proxy table.
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time

DEFAULT_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [8, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
SIZES = DEFAULT_SIZES.copy()
TARGET_RUNTIME_S = 2.5
THROUGHPUT_WARMUP_S = 0.5
N_ROUNDS = 3
WARMUP_ROUNDS = 0
LATENCY_WARMUP_S = 0.5
LATENCY_RUNTIME_S = 1.5
SUBPROCESS_TIMEOUT_S = 30.0
LATENCY_TIMEOUT_S = 60.0
SUBPROCESS_RETRIES = 2
PROXY_DURATION_S = 2.0
THROUGHPUT_MAX_BYTES = None
README = os.path.join(os.path.dirname(__file__), "..", "README.md")
CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "doc", "charts")
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CACHE_DIR = os.path.join(
    os.environ.get("XDG_CACHE_HOME", os.path.join(os.path.expanduser("~"), ".cache")),
    "omq",
)
JSONL_FILE = os.path.join(_CACHE_DIR, "bindings.jsonl")


def load_jsonl():
    rows = []
    try:
        with open(JSONL_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


def append_jsonl(rows):
    os.makedirs(os.path.dirname(JSONL_FILE), exist_ok=True)
    with open(JSONL_FILE, "a") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def save_results(
    run_id, impl, tp_inproc, tp_tcp, atp_tcp, lat, alat, proxy_pp, proxy_rr
):
    rows = []

    def latency_fields(values):
        fields = {
            "p50_us": values[0],
            "p99_us": values[1],
        }
        if len(values) >= 4:
            fields["messages"] = values[2]
            fields["seconds"] = values[3]
            fields["target_seconds"] = LATENCY_RUNTIME_S
            fields["warmup_seconds"] = LATENCY_WARMUP_S
        return fields

    for i, size in enumerate(SIZES):
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "throughput",
                "mode": "sync",
                "transport": "inproc",
                "msg_size": size,
                "msgs_s": tp_inproc[i],
            }
        )
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "throughput",
                "mode": "sync",
                "transport": "tcp",
                "msg_size": size,
                "msgs_s": tp_tcp[i],
            }
        )
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "throughput",
                "mode": "async",
                "transport": "tcp",
                "msg_size": size,
                "msgs_s": atp_tcp[i],
            }
        )
    for i, size in enumerate(latency_sizes_from(SIZES)):
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "latency",
                "mode": "sync",
                "msg_size": size,
                **latency_fields(lat[i]),
            }
        )
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "latency",
                "mode": "async",
                "msg_size": size,
                **latency_fields(alat[i]),
            }
        )
    rows.append(
        {
            "run_id": run_id,
            "impl": impl,
            "kind": "proxy",
            "pattern": "pushpull",
            "msgs_s": proxy_pp,
        }
    )
    rows.append(
        {
            "run_id": run_id,
            "impl": impl,
            "kind": "proxy",
            "pattern": "reqrep",
            "msgs_s": proxy_rr,
        }
    )
    append_jsonl(rows)
    print(f"  appended {len(rows)} rows to {JSONL_FILE}")


def save_proxy_results(run_id, impl, proxy_pp, proxy_rr):
    rows = [
        {
            "run_id": run_id,
            "impl": impl,
            "kind": "proxy",
            "pattern": "pushpull",
            "msgs_s": proxy_pp,
        },
        {
            "run_id": run_id,
            "impl": impl,
            "kind": "proxy",
            "pattern": "reqrep",
            "msgs_s": proxy_rr,
        },
    ]
    append_jsonl(rows)
    print(f"  appended {len(rows)} rows to {JSONL_FILE}")


def chart_data_from_jsonl():
    rows = load_jsonl()
    latency_sizes = latency_sizes_from(SIZES)

    latest = {}
    for r in rows:
        impl = r.get("impl")
        kind = r.get("kind")
        mode = r.get("mode", "")
        transport = r.get("transport", "")
        size = r.get("msg_size", 0)
        pattern = r.get("pattern", "")
        key = (impl, kind, mode, transport, size, pattern)
        prev = latest.get(key)
        if prev is None or r.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = r

    def get_tp(mode, impl, transport, size):
        r = latest.get((impl, "throughput", mode, transport, size, ""))
        return r["msgs_s"] if r else 0.0

    def get_lat(mode, impl, size):
        r = latest.get((impl, "latency", mode, "", size, ""))
        return r["p50_us"] if r else 0.0

    sync_omq_tp = [get_tp("sync", "pyomq", "tcp", s) for s in SIZES]
    sync_pz_tp = [get_tp("sync", "pyzmq", "tcp", s) for s in SIZES]
    async_omq_tp = [get_tp("async", "pyomq", "tcp", s) for s in SIZES]
    async_pz_tp = [get_tp("async", "pyzmq", "tcp", s) for s in SIZES]
    sync_omq_lat = [get_lat("sync", "pyomq", s) for s in latency_sizes]
    sync_pz_lat = [get_lat("sync", "pyzmq", s) for s in latency_sizes]
    async_omq_lat = [get_lat("async", "pyomq", s) for s in latency_sizes]
    async_pz_lat = [get_lat("async", "pyzmq", s) for s in latency_sizes]

    return {
        "sync_omq_tp": sync_omq_tp,
        "sync_pz_tp": sync_pz_tp,
        "async_omq_tp": async_omq_tp,
        "async_pz_tp": async_pz_tp,
        "sync_omq_lat": sync_omq_lat,
        "sync_pz_lat": sync_pz_lat,
        "async_omq_lat": async_omq_lat,
        "async_pz_lat": async_pz_lat,
    }


# helpers


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def median(values, key=lambda value: value):
    ordered = sorted(values, key=key)
    return ordered[len(ordered) // 2]


def free_tcp():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{port}"


def fmt_rate(rate):
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f} M/s"
    return f"{rate / 1_000:.0f} k/s"


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def fmt_int(n):
    return f"{n:,.0f}"


def parse_sizes(value):
    sizes = []
    for raw in value.split(","):
        raw = raw.strip().lower()
        if not raw:
            continue
        multiplier = 1
        if raw.endswith("kib"):
            multiplier = 1024
            raw = raw[:-3]
        elif raw.endswith("kb"):
            multiplier = 1000
            raw = raw[:-2]
        elif raw.endswith("k"):
            multiplier = 1024
            raw = raw[:-1]
        size = int(raw.strip()) * multiplier
        if size <= 0:
            raise argparse.ArgumentTypeError("sizes must be positive")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one size is required")
    return sizes


def configure_benchmark(args):
    global SIZES
    global TARGET_RUNTIME_S
    global THROUGHPUT_WARMUP_S
    global N_ROUNDS
    global WARMUP_ROUNDS
    global LATENCY_WARMUP_S
    global LATENCY_RUNTIME_S
    global SUBPROCESS_TIMEOUT_S
    global LATENCY_TIMEOUT_S
    global SUBPROCESS_RETRIES
    global PROXY_DURATION_S
    global THROUGHPUT_MAX_BYTES

    THROUGHPUT_MAX_BYTES = None

    if args.quick:
        SIZES = QUICK_SIZES.copy()
        TARGET_RUNTIME_S = 0.5
        THROUGHPUT_WARMUP_S = 0.1
        N_ROUNDS = 1
        WARMUP_ROUNDS = 0
        LATENCY_WARMUP_S = 0.1
        LATENCY_RUNTIME_S = 0.5
        SUBPROCESS_TIMEOUT_S = 15.0
        LATENCY_TIMEOUT_S = 20.0
        SUBPROCESS_RETRIES = 0
        PROXY_DURATION_S = 1.0
        THROUGHPUT_MAX_BYTES = 128 * 1024 * 1024

    if args.sizes is not None:
        SIZES = args.sizes
    if args.rounds is not None:
        N_ROUNDS = args.rounds
    if args.target_runtime is not None:
        TARGET_RUNTIME_S = args.target_runtime
        if args.latency_duration is None:
            LATENCY_RUNTIME_S = min(TARGET_RUNTIME_S, LATENCY_RUNTIME_S)
    if args.warmup_duration is not None:
        THROUGHPUT_WARMUP_S = args.warmup_duration
        if args.latency_warmup_duration is None:
            LATENCY_WARMUP_S = THROUGHPUT_WARMUP_S
    if args.latency_warmup_duration is not None:
        LATENCY_WARMUP_S = args.latency_warmup_duration
    if args.latency_duration is not None:
        LATENCY_RUNTIME_S = args.latency_duration
    if args.timeout is not None:
        SUBPROCESS_TIMEOUT_S = args.timeout
        LATENCY_TIMEOUT_S = max(args.timeout, 1.0)
    if args.proxy_duration is not None:
        PROXY_DURATION_S = args.proxy_duration

    if N_ROUNDS < 1:
        raise argparse.ArgumentTypeError("--rounds must be at least 1")
    if TARGET_RUNTIME_S <= 0:
        raise argparse.ArgumentTypeError("--target-runtime must be positive")
    if THROUGHPUT_WARMUP_S < 0:
        raise argparse.ArgumentTypeError("--warmup-duration cannot be negative")
    if LATENCY_WARMUP_S < 0:
        raise argparse.ArgumentTypeError("--latency-warmup-duration cannot be negative")
    if LATENCY_RUNTIME_S <= 0:
        raise argparse.ArgumentTypeError("--latency-duration must be positive")
    if SUBPROCESS_TIMEOUT_S <= 0 or LATENCY_TIMEOUT_S <= 0:
        raise argparse.ArgumentTypeError("--timeout must be positive")
    if PROXY_DURATION_S <= 0:
        raise argparse.ArgumentTypeError("--proxy-duration must be positive")


# subprocess runner


def _run_subprocess(code, label, timeout=None, retries=None):
    timeout = SUBPROCESS_TIMEOUT_S if timeout is None else timeout
    try:
        r = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"{label} timeout after {timeout}s") from error
    if r.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{r.stdout}{r.stderr}")
    return json.loads(r.stdout.strip())


def _measure_throughput_subprocess(lib_name, transport, size, duration=None):
    """Run a throughput measurement. TCP uses 2 separate processes (push +
    pull) so each gets its own runtime. Inproc must stay single-process."""
    duration = TARGET_RUNTIME_S if duration is None else duration
    if lib_name == "pyzmq":
        lib_import = "import zmq as lib"
    else:
        lib_import = "import pyomq as lib"

    if transport == "inproc":
        code = f"""
import threading, time, json
{lib_import}
payload = b'x' * {size}
stop = b'__OMQ_BENCH_STOP__'
duration = {duration}
ep = f'inproc://bench-{{time.monotonic_ns()}}'
ctx = lib.Context()
pull = ctx.socket(lib.PULL)
push = ctx.socket(lib.PUSH)
pull.linger = 0
push.linger = 0
pull.bind(ep)
push.connect(ep)
def sender():
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        push.send(payload)
    push.send(stop)
t = threading.Thread(target=sender)
t.start()
start = None
count = 0
while True:
    msg = pull.recv()
    if msg == stop:
        break
    if start is None:
        start = time.monotonic()
    count += 1
elapsed = time.monotonic() - start if start is not None else 0.0
t.join()
push.close(); pull.close()
print(json.dumps(count / elapsed if elapsed > 0 else 0.0))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
        result = _run_subprocess(code, f"{lib_name} inproc {size}B")
        return result if result is not None else 0.0

    push_code = f"""
import time, sys
{lib_import}
payload = b'x' * {size}
stop = b'__OMQ_BENCH_STOP__'
duration = {duration}
ctx = lib.Context()
push = ctx.socket(lib.PUSH)
push.linger = 0
push.bind('tcp://127.0.0.1:0')
ep = push.last_endpoint
if isinstance(ep, bytes): ep = ep.decode()
port = ep.rsplit(':', 1)[1]
print(port, flush=True)
deadline = time.monotonic() + duration
while time.monotonic() < deadline:
    push.send(payload)
push.send(stop)
sys.stdin.readline()
push.close()
import os; os._exit(0)
"""
    pull_code = f"""
import time, json, sys
{lib_import}
port = sys.argv[1]
stop = b'__OMQ_BENCH_STOP__'
ctx = lib.Context()
pull = ctx.socket(lib.PULL)
pull.linger = 0
pull.connect(f'tcp://127.0.0.1:{{port}}')
start = None
count = 0
while True:
    msg = pull.recv()
    if msg == stop:
        break
    if start is None:
        start = time.monotonic()
    count += 1
elapsed = time.monotonic() - start if start is not None else 0.0
pull.close()
print(json.dumps(count / elapsed if elapsed > 0 else 0.0))
sys.stdout.flush()
import os; os._exit(0)
"""
    push_proc = subprocess.Popen(
        [sys.executable, "-c", push_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.PIPE,
        text=True,
    )
    push_stdout = push_proc.stdout
    push_stdin = push_proc.stdin
    assert push_stdout is not None
    assert push_stdin is not None
    try:
        port_line = push_stdout.readline().strip()
        if not port_line:
            push_proc.terminate()
            push_proc.wait(timeout=5)
            return 0.0
        label = f"{lib_name} {transport} {size}B"
        pull_proc = subprocess.Popen(
            [sys.executable, "-c", pull_code, port_line],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            stdout, stderr = pull_proc.communicate(timeout=SUBPROCESS_TIMEOUT_S)
            result = json.loads(stdout.strip())
        except subprocess.TimeoutExpired:
            sys.stderr.write(f"  [{label} timeout]\n")
            pull_proc.kill()
            pull_proc.wait()
            result = 0.0
        except (json.JSONDecodeError, ValueError):
            sys.stderr.write(f"  [{label} invalid output]\n")
            if stderr:
                sys.stderr.write(stderr)
            result = 0.0
    finally:
        try:
            push_stdin.write("\n")
            push_stdin.flush()
        except OSError:
            pass
        push_proc.terminate()
        try:
            push_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            push_proc.kill()
            push_proc.wait()
    return result


def run_throughput(lib_name):
    inproc_results = []
    tcp_results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        inproc_runs = []
        tcp_runs = []
        for _ in range(N_ROUNDS):
            if THROUGHPUT_WARMUP_S > 0:
                _measure_throughput_subprocess(
                    lib_name, "inproc", size, duration=THROUGHPUT_WARMUP_S
                )
            inproc_runs.append(_measure_throughput_subprocess(lib_name, "inproc", size))
            if THROUGHPUT_WARMUP_S > 0:
                _measure_throughput_subprocess(
                    lib_name, "tcp", size, duration=THROUGHPUT_WARMUP_S
                )
            tcp_runs.append(_measure_throughput_subprocess(lib_name, "tcp", size))
        inproc = median(inproc_runs)
        tcp = median(tcp_runs)
        inproc_results.append(inproc)
        tcp_results.append(tcp)
        print(f" inproc {fmt_rate(inproc):>10}  tcp {fmt_rate(tcp):>10}")

    return inproc_results, tcp_results


# async PUSH/PULL throughput


def _measure_async_subprocess(lib_name, size, duration=None):
    """Async throughput: push in one process, async pull in another."""
    duration = TARGET_RUNTIME_S if duration is None else duration
    if lib_name == "pyzmq":
        lib_import = "import zmq as lib; import zmq.asyncio as alib"
        push_import = "import zmq as lib"
    else:
        lib_import = "import pyomq as lib; import pyomq.asyncio as alib"
        push_import = "import pyomq as lib"

    push_code = f"""
import sys, time
{push_import}
payload = b'x' * {size}
stop = b'__OMQ_BENCH_STOP__'
duration = {duration}
ctx = lib.Context()
push = ctx.socket(lib.PUSH)
push.linger = 0
push.bind('tcp://127.0.0.1:0')
ep = push.last_endpoint
if isinstance(ep, bytes): ep = ep.decode()
port = ep.rsplit(':', 1)[1]
print(port, flush=True)
deadline = time.monotonic() + duration
while time.monotonic() < deadline:
    push.send(payload)
push.send(stop)
sys.stdin.readline()
push.close()
import os; os._exit(0)
"""
    pull_code = f"""
import asyncio, time, json, sys
{lib_import}
async def run():
    port = sys.argv[1]
    stop = b'__OMQ_BENCH_STOP__'
    ctx = alib.Context()
    pull = ctx.socket(lib.PULL)
    pull.linger = 0
    pull.connect(f'tcp://127.0.0.1:{{port}}')
    count = 0; start = None
    while True:
        msg = await pull.recv()
        if msg == stop:
            break
        if start is None:
            start = time.monotonic()
        count += 1
    elapsed = time.monotonic() - start if start is not None else 0.0
    pull.close()
    print(json.dumps(count / elapsed if elapsed > 0 else 0.0))
    sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    push_proc = subprocess.Popen(
        [sys.executable, "-c", push_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.PIPE,
        text=True,
    )
    push_stdout = push_proc.stdout
    push_stdin = push_proc.stdin
    assert push_stdout is not None
    assert push_stdin is not None
    try:
        port_line = push_stdout.readline().strip()
        if not port_line:
            push_proc.terminate()
            push_proc.wait(timeout=5)
            return 0.0
        label = f"{lib_name} async tcp {size}B"
        pull_proc = subprocess.Popen(
            [sys.executable, "-c", pull_code, port_line],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            stdout, stderr = pull_proc.communicate(timeout=SUBPROCESS_TIMEOUT_S)
            result = json.loads(stdout.strip())
        except subprocess.TimeoutExpired:
            sys.stderr.write(f"  [{label} timeout]\n")
            pull_proc.kill()
            pull_proc.wait()
            result = 0.0
        except (json.JSONDecodeError, ValueError):
            sys.stderr.write(f"  [{label} invalid output]\n")
            if stderr:
                sys.stderr.write(stderr)
            result = 0.0
    finally:
        try:
            push_stdin.write("\n")
            push_stdin.flush()
        except OSError:
            pass
        push_proc.terminate()
        try:
            push_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            push_proc.kill()
            push_proc.wait()
    return result


def run_async_throughput(lib_name):
    results = []
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        runs = []
        for _ in range(N_ROUNDS):
            if THROUGHPUT_WARMUP_S > 0:
                _measure_async_subprocess(lib_name, size, duration=THROUGHPUT_WARMUP_S)
            runs.append(_measure_async_subprocess(lib_name, size))
        tcp = median(runs)
        results.append(tcp)
        print(f" {fmt_rate(tcp):>10}")

    return results


# sync REQ/REP latency


def _measure_latency_subprocess(lib_name, size, warmup_seconds, duration_seconds):
    code = f"""
import time, threading, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
if '{lib_name}' == 'pyzmq':
    import zmq as lib
else:
    import pyomq as lib
payload = b'x' * {size}
ep = free_tcp()
ctx = lib.Context()
rep = ctx.socket(lib.REP)
req = ctx.socket(lib.REQ)
rep.linger = 0
req.linger = 0
rep.bind(ep)
req.connect(ep)
time.sleep(0.05)
def echo():
    try:
        while True:
            msg = rep.recv()
            rep.send(msg)
            if msg == b'__OMQ_BENCH_STOP__':
                break
    except Exception:
        pass
t = threading.Thread(target=echo, daemon=True)
t.start()
warmup_deadline = time.monotonic() + {warmup_seconds}
while time.monotonic() < warmup_deadline:
    req.send(payload)
    req.recv()
rtts = []
start = time.monotonic()
deadline = start + {duration_seconds}
while True:
    t0 = time.monotonic()
    req.send(payload)
    req.recv()
    rtts.append(time.monotonic() - t0)
    if time.monotonic() >= deadline:
        break
elapsed = time.monotonic() - start
req.send(b'__OMQ_BENCH_STOP__')
req.recv()
req.close()
rep.close()
t.join(timeout=1.0)
rtts.sort()
p50 = rtts[len(rtts)*50//100]*1e6
p99 = rtts[len(rtts)*99//100]*1e6
print(json.dumps([p50, p99, len(rtts), elapsed]))
import sys; sys.stdout.flush(); import os; os._exit(0)
"""
    result = _run_subprocess(
        code,
        f"{lib_name} lat {size}B",
        timeout=LATENCY_TIMEOUT_S,
    )
    return tuple(result) if result is not None else (999999.0, 999999.0)


def run_latency(lib_name):
    results = []
    for size in latency_sizes_from(SIZES):
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        warmup = (0.0, 0.0)
        for _ in range(WARMUP_ROUNDS):
            warmup = _measure_latency_subprocess(
                lib_name,
                size,
                min(LATENCY_WARMUP_S, 0.05),
                min(LATENCY_RUNTIME_S, 0.05),
            )
        if warmup == (999999.0, 999999.0):
            results.append(warmup)
            print(" timeout")
            continue

        runs = [
            _measure_latency_subprocess(
                lib_name,
                size,
                LATENCY_WARMUP_S,
                LATENCY_RUNTIME_S,
            )
            for _ in range(N_ROUNDS)
        ]
        selected = median(runs, key=lambda row: row[0])
        results.append(selected)
        print(f" p50 {selected[0]:.1f} μs  p99 {selected[1]:.1f} μs  n={selected[2]}")

    return results


# sync REQ/REP latency


def _measure_async_latency_subprocess(lib_name, size, warmup_seconds, duration_seconds):
    if lib_name == "pyzmq":
        lib_import = "import zmq; import zmq.asyncio; lib = zmq; actx = zmq.asyncio"
    else:
        lib_import = "import pyomq; import pyomq.asyncio as actx; lib = pyomq"

    send_await = "await " if lib_name == "pyzmq" else ""
    code = f"""
import asyncio, time, json, socket as sock
def free_tcp():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return f'tcp://127.0.0.1:{{port}}'
{lib_import}
async def run():
    payload = b'x' * {size}
    ep = free_tcp()
    ctx = actx.Context()
    rep = ctx.socket(lib.REP)
    req = ctx.socket(lib.REQ)
    rep.bind(ep)
    req.connect(ep)
    await asyncio.sleep(0.05)
    async def echo():
        try:
            while True:
                msg = await rep.recv()
                {send_await}rep.send(msg)
                if msg == b'__OMQ_BENCH_STOP__':
                    break
        except Exception:
            pass
    task = asyncio.create_task(echo())
    warmup_deadline = time.monotonic() + {warmup_seconds}
    while time.monotonic() < warmup_deadline:
        {send_await}req.send(payload)
        await req.recv()
    rtts = []
    start = time.monotonic()
    deadline = start + {duration_seconds}
    while True:
        t0 = time.monotonic()
        {send_await}req.send(payload)
        await req.recv()
        rtts.append(time.monotonic() - t0)
        if time.monotonic() >= deadline:
            break
    elapsed = time.monotonic() - start
    {send_await}req.send(b'__OMQ_BENCH_STOP__')
    await req.recv()
    await task
    rtts.sort()
    p50 = rtts[len(rtts)*50//100]*1e6
    p99 = rtts[len(rtts)*99//100]*1e6
    print(json.dumps([p50, p99, len(rtts), elapsed]))
    import sys; sys.stdout.flush(); import os; os._exit(0)
asyncio.run(run())
"""
    result = _run_subprocess(
        code,
        f"{lib_name} async lat {size}B",
        timeout=LATENCY_TIMEOUT_S,
    )
    return tuple(result) if result is not None else (999999.0, 999999.0)


def run_async_latency(lib_name):
    results = []
    for size in latency_sizes_from(SIZES):
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()

        warmup = (0.0, 0.0)
        for _ in range(WARMUP_ROUNDS):
            warmup = _measure_async_latency_subprocess(
                lib_name,
                size,
                min(LATENCY_WARMUP_S, 0.05),
                min(LATENCY_RUNTIME_S, 0.05),
            )
        if warmup == (999999.0, 999999.0):
            results.append(warmup)
            print(" timeout")
            continue

        runs = [
            _measure_async_latency_subprocess(
                lib_name, size, LATENCY_WARMUP_S, LATENCY_RUNTIME_S
            )
            for _ in range(N_ROUNDS)
        ]
        selected = median(runs, key=lambda row: row[0])
        results.append(selected)
        print(f" p50 {selected[0]:.1f} μs  p99 {selected[1]:.1f} μs  n={selected[2]}")

    return results


# proxy forwarding (2-process)


def _measure_proxy_subprocess(lib_name, pattern, duration):
    if lib_name == "pyzmq":
        lib_import = "import zmq as lib"
    else:
        lib_import = "import pyomq as lib"

    proxy_code = f"""
import json, sys, socket as sock
{lib_import}
def pick_port():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port
ctx = lib.Context()
fe_port = pick_port()
be_port = pick_port()
"""
    if pattern == "pushpull":
        proxy_code += """
frontend = ctx.socket(lib.PULL)
backend = ctx.socket(lib.PUSH)
"""
    else:
        proxy_code += """
frontend = ctx.socket(lib.ROUTER)
backend = ctx.socket(lib.DEALER)
"""
    proxy_code += """
frontend.bind(f'tcp://127.0.0.1:{fe_port}')
backend.bind(f'tcp://127.0.0.1:{be_port}')
print(json.dumps([fe_port, be_port]), flush=True)
try:
    lib.proxy(frontend, backend)
except Exception:
    pass
"""

    proxy_proc = subprocess.Popen(
        [sys.executable, "-c", proxy_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    proxy_stdout = proxy_proc.stdout
    assert proxy_stdout is not None
    try:
        line = proxy_stdout.readline()
        fe_port, be_port = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        proxy_proc.terminate()
        proxy_proc.wait(timeout=5)
        return 0.0
    fe_ep = f"tcp://127.0.0.1:{fe_port}"
    be_ep = f"tcp://127.0.0.1:{be_port}"

    if pattern == "pushpull":
        bench_code = f"""
import threading, time, json, sys, os
{lib_import}
payload = b'x' * 128
stop = b'__OMQ_BENCH_STOP__'
duration = {duration}
ctx = lib.Context()
push = ctx.socket(lib.PUSH)
pull = ctx.socket(lib.PULL)
push.linger = 0
push.connect('{fe_ep}')
pull.connect('{be_ep}')
warmup_deadline = time.monotonic() + min(duration, 0.05)
while time.monotonic() < warmup_deadline:
    push.send(b'w')
    pull.recv()
def sender():
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        push.send(payload)
    push.send(stop)
t = threading.Thread(target=sender)
start = time.monotonic()
t.start()
count = 0
while True:
    msg = pull.recv()
    if msg == stop:
        break
    count += 1
elapsed = time.monotonic() - start
t.join()
push.close()
pull.close()
print(json.dumps(count / elapsed))
sys.stdout.flush(); os._exit(0)
"""
    else:
        bench_code = f"""
import threading, time, json, sys, os
{lib_import}
payload = b'x' * 128
duration = {duration}
ctx = lib.Context()
client = ctx.socket(lib.REQ)
worker = ctx.socket(lib.REP)
client.linger = 0
worker.linger = 0
client.connect('{fe_ep}')
worker.connect('{be_ep}')
warmup_deadline = time.monotonic() + min(duration, 0.05)
while time.monotonic() < warmup_deadline:
    client.send(b'w')
    worker.send(worker.recv())
    client.recv()
start = time.monotonic()
count = 0
deadline = start + duration
while time.monotonic() < deadline:
    client.send(payload)
    worker.send(worker.recv())
    client.recv()
    count += 1
elapsed = time.monotonic() - start
client.close()
worker.close()
print(json.dumps(count / elapsed))
sys.stdout.flush(); os._exit(0)
"""

    try:
        r = subprocess.run(
            [sys.executable, "-c", bench_code],
            capture_output=True,
            text=True,
            timeout=LATENCY_TIMEOUT_S,
        )
        if r.returncode != 0:
            return 0.0
        return json.loads(r.stdout.strip())
    except (subprocess.TimeoutExpired, json.JSONDecodeError, ValueError):
        return 0.0
    finally:
        proxy_proc.terminate()
        proxy_proc.wait(timeout=5)


_SCRIPT_DIR = os.path.dirname(__file__)
_REPO_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", ".."))
BENCH_PROXY_CLIENTS = [
    os.path.join(_REPO_ROOT, "target", "release", "omq_bench_proxy_client"),
    os.path.join(_SCRIPT_DIR, "..", "target", "release", "bench_proxy_client"),
]


def _bench_proxy_client():
    for path in BENCH_PROXY_CLIENTS:
        if os.path.isfile(path):
            return path
    return None


def _measure_proxy_native(lib_name, client, duration=None):
    duration = PROXY_DURATION_S if duration is None else duration
    if lib_name == "pyzmq":
        lib_import = "import zmq as lib"
    else:
        lib_import = "import pyomq as lib"

    proxy_code = f"""
import json, sys, socket as sock
{lib_import}
def pick_port():
    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port
ctx = lib.Context()
frontend = ctx.socket(lib.PULL)
backend = ctx.socket(lib.PUSH)
fe_port = pick_port()
be_port = pick_port()
frontend.bind(f'tcp://127.0.0.1:{{fe_port}}')
backend.bind(f'tcp://127.0.0.1:{{be_port}}')
print(json.dumps([fe_port, be_port]), flush=True)
try:
    lib.proxy(frontend, backend)
except Exception:
    pass
"""

    proxy_proc = subprocess.Popen(
        [sys.executable, "-c", proxy_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    proxy_stdout = proxy_proc.stdout
    assert proxy_stdout is not None
    try:
        line = proxy_stdout.readline()
        fe_port, be_port = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        proxy_proc.terminate()
        proxy_proc.wait(timeout=5)
        return 0.0

    try:
        r = subprocess.run(
            [client, str(fe_port), str(be_port), "128", str(duration)],
            capture_output=True,
            text=True,
            timeout=duration + 10,
        )
        if r.returncode != 0:
            return 0.0
        parts = r.stdout.strip().split()
        count, elapsed = int(parts[0]), float(parts[1])
        return count / elapsed
    except (subprocess.TimeoutExpired, ValueError, IndexError):
        return 0.0
    finally:
        proxy_proc.terminate()
        proxy_proc.wait(timeout=5)


def run_proxy(lib_name):
    client = _bench_proxy_client()

    sys.stdout.write("  PUSH/PULL ...")
    sys.stdout.flush()
    if client is not None:
        runs = []
        for _ in range(N_ROUNDS):
            if THROUGHPUT_WARMUP_S > 0:
                _measure_proxy_native(
                    lib_name, client, min(PROXY_DURATION_S, THROUGHPUT_WARMUP_S)
                )
            runs.append(_measure_proxy_native(lib_name, client))
        pushpull_rate = median(runs)
    else:
        runs = []
        for _ in range(N_ROUNDS):
            if THROUGHPUT_WARMUP_S > 0:
                _measure_proxy_subprocess(
                    lib_name, "pushpull", min(PROXY_DURATION_S, THROUGHPUT_WARMUP_S)
                )
            runs.append(
                _measure_proxy_subprocess(lib_name, "pushpull", PROXY_DURATION_S)
            )
        pushpull_rate = median(runs)
    print(f" {fmt_rate(pushpull_rate)}")

    sys.stdout.write("  REQ/REP ...")
    sys.stdout.flush()
    runs = []
    for _ in range(N_ROUNDS):
        if THROUGHPUT_WARMUP_S > 0:
            _measure_proxy_subprocess(
                lib_name, "reqrep", min(PROXY_DURATION_S, THROUGHPUT_WARMUP_S)
            )
        runs.append(_measure_proxy_subprocess(lib_name, "reqrep", PROXY_DURATION_S))
    reqrep_rate = median(runs)
    print(f" {fmt_rate(reqrep_rate)}")

    return pushpull_rate, reqrep_rate


# SVG chart generation

# Colors: warm = pyomq, cool = pyzmq
C_PYOMQ = "#ef4444"
C_PYOMQ_ASYNC = "#fb923c"
C_PYZMQ = "#60a5fa"
C_PYZMQ_ASYNC = "#a855f7"


def _nice_ceil(v):
    if v <= 0:
        return 1
    exp = math.floor(math.log10(v))
    base = 10**exp
    for m in [1, 2, 5, 10]:
        candidate = m * base
        if candidate >= v:
            return candidate
    return 10 * base


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def _fmt_y_us(val):
    if val >= 1000:
        return f"{val / 1000:g} ms"
    return f"{val:g} μs"


def _fmt_mbps(val):
    if val >= 1000:
        return f"{val / 1000:g} GB/s"
    if val >= 10:
        return f"{val:.0f} MB/s"
    return f"{val:.1f} MB/s"


def _read_chart_hw():
    config = {}
    path = os.path.join(REPO_ROOT, ".chart_hw")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if sep:
                    config[key.strip()] = value.strip()
    except OSError:
        pass
    return config


def _detect_hardware():
    hw_conf = _read_chart_hw()
    prefix = os.environ.get("OMQ_HW_PREFIX") or hw_conf.get("prefix")
    postfix = os.environ.get("OMQ_HW_POSTFIX") or hw_conf.get("postfix")
    if prefix and postfix:
        return f"{prefix}, {postfix}"
    if prefix:
        return prefix
    if postfix:
        return postfix
    return None


def gen_combined_chart(data, path):
    latency_sizes = latency_sizes_from(SIZES)
    lat_n = len(latency_sizes)
    hw_label = _detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 810 + hw_offset
    x_left, x_right = 60, 790
    plot_w = x_right - x_left
    top_left, top_mid, top_right = 60, 395, 790
    top_right_left = 455

    t1_top = 95 + hw_offset
    t1_bot = 430 + hw_offset
    t1_h = t1_bot - t1_top
    t2_top = t1_bot + 105
    t2_bot = t2_top + 200
    t2_h = t2_bot - t2_top

    small_sizes = [s for s in SIZES if s <= 1024]
    large_sizes = [s for s in SIZES if s >= 256]
    small_indices = [SIZES.index(s) for s in small_sizes]
    large_indices = [SIZES.index(s) for s in large_sizes]
    small_xs = [
        top_left + i * (top_mid - top_left) / max(len(small_sizes) - 1, 1)
        for i in range(len(small_sizes))
    ]
    large_xs = [
        top_right_left + i * (top_right - top_right_left) / max(len(large_sizes) - 1, 1)
        for i in range(len(large_sizes))
    ]
    lat_xs = [x_left + i * plot_w / max(lat_n - 1, 1) for i in range(lat_n)]
    mid_x = (x_left + x_right) / 2

    sync_omq_tp = data["sync_omq_tp"]
    sync_pz_tp = data["sync_pz_tp"]
    async_omq_tp = data["async_omq_tp"]
    async_pz_tp = data["async_pz_tp"]

    tp_values = [sync_omq_tp, sync_pz_tp, async_omq_tp, async_pz_tp]
    msg_max = 2_000_000
    gbs_values = [
        values[i] * SIZES[i] / 1_000_000_000
        for values in tp_values
        for i in large_indices
    ]
    gbs_max = max(1, math.ceil(max(gbs_values, default=0)))

    def y_msg(v):
        frac = v / msg_max if msg_max > 0 else 0
        return t1_bot - frac * t1_h

    def y_gbs(v):
        frac = v / gbs_max if gbs_max > 0 else 0
        return t1_bot - frac * t1_h

    lat_max = 200.0
    lat_step = 20

    def y_lat(v):
        return t2_bot - (v / lat_max) * t2_h

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>')

    # TOP PANEL: THROUGHPUT

    L.append(
        f'  <text x="{mid_x}" y="{t1_top - 65}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>"
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{t1_top - 51}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    for panel_left, panel_right, panel_xs, ticks, panel_max, formatter, label_x in (
        (
            top_left,
            top_mid,
            small_xs,
            [200_000 * i for i in range(1, (msg_max // 200_000) + 1)],
            msg_max,
            _fmt_y_rate,
            top_left - 8,
        ),
        (
            top_right_left,
            top_right,
            large_xs,
            [i / 2 for i in range(1, int(gbs_max * 2) + 1)],
            gbs_max,
            lambda value: f"{value:g} GB/s",
            top_right + 8,
        ),
    ):
        for tick in ticks:
            yy = t1_bot - (tick / panel_max) * t1_h
            L.append(
                f'  <line x1="{panel_left}" y1="{yy:.1f}" x2="{panel_right}" y2="{yy:.1f}" stroke="#374151" stroke-width="1"/>'
            )
            anchor = "end" if label_x < panel_left else "start"
            L.append(
                f'  <text x="{label_x}" y="{yy:.1f}" text-anchor="{anchor}" dominant-baseline="middle" fill="#e5e7eb" font-size="10">{formatter(tick)}</text>'
            )
        for x in panel_xs:
            L.append(
                f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}" stroke="#374151" stroke-width="1"/>'
            )
        if panel_left == top_left:
            L.append(
                f'  <line x1="{panel_left}" y1="{t1_top}" x2="{panel_left}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        if panel_right == top_right:
            L.append(
                f'  <line x1="{panel_right}" y1="{t1_top}" x2="{panel_right}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        L.append(
            f'  <line x1="{panel_left}" y1="{t1_bot}" x2="{panel_right}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
        )

    L.append(
        f'  <text x="{(top_left + top_mid) / 2:.1f}" y="{t1_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">small messages</text>'
    )
    L.append(
        f'  <text x="{(top_right_left + top_right) / 2:.1f}" y="{t1_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">medium/large messages</text>'
    )

    tp_series = [
        ("pyomq", C_PYOMQ, sync_omq_tp),
        ("pyomq async", C_PYOMQ_ASYNC, async_omq_tp),
        ("pyzmq", C_PYZMQ, sync_pz_tp),
        ("pyzmq async", C_PYZMQ_ASYNC, async_pz_tp),
    ]

    for _, color, vals in tp_series:
        pts = " ".join(
            f"{small_xs[j]:.1f},{y_msg(vals[i]):.1f}"
            for j, i in enumerate(small_indices)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    for _, color, vals in tp_series:
        gbs = [vals[i] * SIZES[i] / 1e9 for i in large_indices]
        pts = " ".join(f"{large_xs[j]:.1f},{y_gbs(v):.1f}" for j, v in enumerate(gbs))
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for j, v in enumerate(gbs):
            yy = y_gbs(v)
            L.append(
                f'  <circle cx="{large_xs[j]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )

    for i, size in enumerate(small_sizes):
        L.append(
            f'  <text x="{small_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    for i, size in enumerate(large_sizes):
        L.append(
            f'  <text x="{large_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    L.append(
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate · solid = bandwidth</text>'
    )

    # BOTTOM PANEL: LATENCY

    L.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 μs (lower is better)</text>"
    )

    sync_omq_lat = data["sync_omq_lat"]
    sync_pz_lat = data["sync_pz_lat"]
    async_omq_lat = data["async_omq_lat"]
    async_pz_lat = data["async_pz_lat"]

    for v in range(int(lat_step), int(lat_max) + 1, int(lat_step)):
        yy = y_lat(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
            f"{_fmt_y_us(v)}</text>"
        )

    for x in lat_xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#374151" stroke-width="1"/>'
        )

    L.append(
        f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    lat_series = [
        ("pyomq", C_PYOMQ, sync_omq_lat),
        ("pyomq async", C_PYOMQ_ASYNC, async_omq_lat),
        ("pyzmq", C_PYZMQ, sync_pz_lat),
        ("pyzmq async", C_PYZMQ_ASYNC, async_pz_lat),
    ]

    for _, color, vals in lat_series:
        pts = " ".join(f"{lat_xs[i]:.1f},{y_lat(v):.1f}" for i, v in enumerate(vals))
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, v in enumerate(vals):
            yy = y_lat(v)
            L.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )

    for i in range(lat_n):
        L.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(latency_sizes[i])}</text>'
        )

    # LEGEND

    leg_y = t2_bot + 40
    legend_items = [
        ("pyomq", C_PYOMQ),
        ("pyomq async", C_PYOMQ_ASYNC),
        ("pyzmq", C_PYZMQ),
        ("pyzmq async", C_PYZMQ_ASYNC),
    ]
    item_w = 140
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for idx, (label, color) in enumerate(legend_items):
        lx = start_x + idx * item_w
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

    footer_y = leg_y + 18
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"dashed = msg/s (left) · solid = throughput (right)</text>"
    )

    L.append("</svg>")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(L))
        f.write("\n")
    print(f"  wrote {path}")


# README tables


def build_proxy_table():
    rows = load_jsonl()
    latest = {}
    for r in rows:
        if r.get("kind") != "proxy":
            continue
        key = (r["impl"], r["pattern"])
        prev = latest.get(key)
        if prev is None or r.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = r

    pp_omq = latest.get(("pyomq", "pushpull"), {}).get("msgs_s", 0)
    pp_pz = latest.get(("pyzmq", "pushpull"), {}).get("msgs_s", 0)
    rr_omq = latest.get(("pyomq", "reqrep"), {}).get("msgs_s", 0)
    rr_pz = latest.get(("pyzmq", "reqrep"), {}).get("msgs_s", 0)
    pp_ratio = pp_omq / pp_pz if pp_pz > 0 else 0
    rr_ratio = rr_omq / rr_pz if rr_pz > 0 else 0

    return "\n".join(
        [
            "|                    | pyomq     | pyzmq     | ratio     |",
            "|--------------------|----------:|----------:|----------:|",
            f"| PUSH/PULL msg/s    | {fmt_rate(pp_omq):>9} "
            f"| {fmt_rate(pp_pz):>9} | **{pp_ratio:.2f}x** |",
            f"| REQ/REP rt/s       | {fmt_int(rr_omq) + '/s':>9} "
            f"| {fmt_int(rr_pz) + '/s':>9} | **{rr_ratio:.2f}x** |",
        ]
    )


# README update


def update_marker(content, marker, table):
    pattern = rf"<!-- {marker}:START -->\n.*?\n<!-- {marker}:END -->"
    replacement = f"<!-- {marker}:START -->\n{table}\n<!-- {marker}:END -->"
    new_content, count = re.subn(pattern, replacement, content, flags=re.DOTALL)
    if count == 0:
        print(
            f"ERROR: <!-- {marker}:START -->...<!-- {marker}:END --> "
            f"markers not found in README.md"
        )
        sys.exit(1)
    return new_content


def update_readme_proxy_table():
    proxy_table = build_proxy_table()
    with open(README) as f:
        content = f.read()
    content = update_marker(content, "PROXY_PERF", proxy_table)
    with open(README, "w") as f:
        f.write(content)
    print(f"\nUpdated {README}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--impl",
        action="append",
        dest="impls",
        choices=["pyomq", "pyzmq"],
        help="implementation(s) to benchmark (default: both)",
    )
    parser.add_argument(
        "--quick", action="store_true", help="run a short local-only benchmark"
    )
    parser.add_argument(
        "--sizes",
        type=parse_sizes,
        help="comma-separated message sizes, e.g. 8,128,1k,32k",
    )
    parser.add_argument("--rounds", type=int, help="measured rounds per cell")
    parser.add_argument(
        "--target-runtime",
        type=float,
        help="target throughput runtime per round in seconds",
    )
    parser.add_argument(
        "--warmup-duration",
        type=float,
        help="PUSH/PULL throughput warmup duration per round in seconds",
    )
    parser.add_argument(
        "--latency-warmup-duration",
        type=float,
        help="REQ/REP latency warmup duration in seconds",
    )
    parser.add_argument(
        "--latency-duration",
        type=float,
        help="REQ/REP latency measurement duration in seconds",
    )
    parser.add_argument(
        "--timeout", type=float, help="subprocess timeout per attempt in seconds"
    )
    parser.add_argument(
        "--proxy-duration",
        type=float,
        help="native proxy throughput duration in seconds",
    )
    parser.add_argument(
        "--no-save", action="store_true", help="print results without appending JSONL"
    )
    parser.add_argument(
        "--no-docs", action="store_true", help="skip README and chart updates"
    )
    parser.add_argument(
        "--chart-only",
        action="store_true",
        help="regenerate SVG from existing JSONL, no benchmarking",
    )
    parser.add_argument(
        "--proxy-only",
        action="store_true",
        help="benchmark proxy only and update README proxy table",
    )
    args = parser.parse_args()

    if args.chart_only and args.proxy_only:
        parser.error("--chart-only and --proxy-only are mutually exclusive")
    if args.chart_only and any(
        value is not None
        for value in (
            args.sizes,
            args.rounds,
            args.target_runtime,
            args.warmup_duration,
            args.latency_warmup_duration,
            args.latency_duration,
            args.timeout,
            args.proxy_duration,
        )
    ):
        parser.error("--chart-only cannot be combined with benchmark knobs")
    if args.chart_only and args.quick:
        parser.error("--chart-only cannot be combined with --quick")

    if args.chart_only:
        print("Generating chart from existing JSONL...")
        data = chart_data_from_jsonl()
        gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))
        return

    try:
        configure_benchmark(args)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))

    run_id = time.strftime("%Y-%m-%dT%H:%M:%S")
    impls = args.impls or ["pyomq", "pyzmq"]
    diagnostic_knobs = any(
        value is not None
        for value in (
            args.sizes,
            args.rounds,
            args.target_runtime,
            args.warmup_duration,
            args.latency_warmup_duration,
            args.latency_duration,
            args.timeout,
            args.proxy_duration,
        )
    )
    save_enabled = not args.no_save and not args.quick and not diagnostic_knobs
    docs_enabled = save_enabled and not args.no_docs

    for impl in impls:
        print(f"\n{'=' * 40}")
        print(f"Benchmarking {impl}")
        print(f"{'=' * 40}")

        if args.proxy_only:
            print(f"\n{impl} zmq.proxy() forwarding...")
            proxy_pp, proxy_rr = run_proxy(impl)
            if save_enabled:
                print("\nSaving proxy results...")
                save_proxy_results(run_id, impl, proxy_pp, proxy_rr)
            continue

        print(f"\n{impl} sync PUSH/PULL throughput...")
        tp_inproc, tp_tcp = run_throughput(impl)

        print(f"\n{impl} async PUSH/PULL throughput...")
        atp_tcp = run_async_throughput(impl)

        print(f"\n{impl} sync REQ/REP latency (TCP)...")
        lat = run_latency(impl)

        print(f"\n{impl} async REQ/REP latency (TCP)...")
        alat = run_async_latency(impl)

        print(f"\n{impl} zmq.proxy() forwarding...")
        proxy_pp, proxy_rr = run_proxy(impl)

        if save_enabled:
            print("\nSaving results...")
            save_results(
                run_id, impl, tp_inproc, tp_tcp, atp_tcp, lat, alat, proxy_pp, proxy_rr
            )

    if not save_enabled:
        print("\nSkipping JSONL, README, and chart updates.")
        return

    if docs_enabled:
        update_readme_proxy_table()

    if args.proxy_only:
        return

    if docs_enabled:
        print("\nGenerating chart...")
        data = chart_data_from_jsonl()
        gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))


if __name__ == "__main__":
    main()
