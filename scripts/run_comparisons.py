#!/usr/bin/env python3
"""Consolidated benchmark comparison runner.

Runs PUSH/PULL throughput and REQ/REP latency benchmarks across
implementations (omq-compio, omq-tokio, libzmq, zmq.rs) and writes
results to benchmarks/comparisons.jsonl.

Usage:
  scripts/run_comparisons.py                        # all impls, tcp+inproc+ipc, latency on
  scripts/run_comparisons.py --quick-run            # 3 sizes only
  scripts/run_comparisons.py --impl rzmq            # single impl
  scripts/run_comparisons.py --impl omq-compio --impl libzmq  # subset
  scripts/run_comparisons.py --transport tcp         # TCP only
  scripts/run_comparisons.py --no-latency           # skip REQ/REP latency
"""

import argparse
import atexit
import glob
import json
import os
import random
import selectors
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _cleanup_ipc_sockets():
    """Remove stale IPC socket files left by benchmark peers."""
    for p in glob.glob(str(ROOT / "@omq-bench-cmp-*")):
        try:
            os.unlink(p)
        except OSError:
            pass
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"
FULL_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [32, 1024, 4096]
DEFAULT_DURATION = float(os.environ.get("OMQ_BENCH_DURATION", "2.0"))
QUICK_DURATION = 1.5
DEFAULT_ROUNDS = int(os.environ.get("OMQ_BENCH_ROUNDS", "3"))
QUICK_ROUNDS = 1
LATENCY_ITERATIONS = 5_000
LATENCY_WARMUP = 500
LATENCY_TIMEOUT = 15


# ── formatting ────────────────────────────────────────────────────

def size_label(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n // (1024 * 1024)} MiB"
    if n >= 1024:
        return f"{n // 1024} KiB"
    return f"{n} B"


# ── build ─────────────────────────────────────────────────────────

def cargo_build(crate: str, binary: str, features: list[str] | None = None):
    cmd = ["cargo", "build", "--release", "-p", crate, "--bin", binary, "-q"]
    if features:
        cmd += ["--features", ",".join(features)]
    subprocess.run(cmd, cwd=ROOT, check=True)


def gcc_build(src: Path, out: Path):
    subprocess.run(
        ["gcc", "-O2", "-o", str(out), str(src), "-lzmq", "-lpthread"],
        check=True,
    )


def cargo_version(crate: str, manifest: Path | None = None) -> str:
    cmd = ["cargo", "metadata", "--format-version", "1", "--no-deps"]
    if manifest:
        cmd += ["--manifest-path", str(manifest)]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True, cwd=ROOT,
        )
        pkgs = json.loads(result.stdout)["packages"]
        for p in pkgs:
            if p["name"] == crate:
                return p["version"]
    except Exception:
        pass
    return "?"


def libzmq_version() -> str:
    try:
        result = subprocess.run(
            ["pkg-config", "--modversion", "libzmq"],
            capture_output=True, text=True,
        )
        v = result.stdout.strip()
        return v if v else "?"
    except Exception:
        return "?"


# ── process management ────────────────────────────────────────────

def spawn_process(binary: str, *args: str, env: dict | None = None) -> subprocess.Popen:
    merged = {**os.environ, **(env or {})} if env else None
    return subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=merged,
    )


def read_bound_port(proc: subprocess.Popen, timeout: float = 5.0) -> int | None:
    """Read 'PORT <n>' from the process's first stdout line."""
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    ready = sel.select(timeout=timeout)
    sel.close()
    if not ready:
        return None
    line = proc.stdout.readline().strip()
    if line.startswith("PORT "):
        return int(line.split()[1])
    return None


def capture_with_cpu(binary: str, *args: str, timeout: int = 15,
                     env: dict | None = None) -> tuple[str, float]:
    """Run a single-process bench and return (stdout, cpu_seconds)."""
    merged = {**os.environ, **(env or {})} if env else None
    proc = subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=False,
        env=merged,
    )
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    chunks = []
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(f"WARNING: timeout: {binary} {' '.join(args)}", file=sys.stderr)
            proc.kill()
            proc.wait()
            sel.close()
            return "", 0.0
        ready = sel.select(timeout=remaining)
        if ready:
            data = proc.stdout.read()
            if data:
                chunks.append(data)
            else:
                break
    sel.close()
    cpu = read_proc_cpu(proc.pid)
    proc.wait()
    return b"".join(chunks).decode("utf-8", errors="replace"), cpu


def capture_process(binary: str, *args: str, timeout: int = 15,
                    env: dict | None = None) -> str:
    merged = {**os.environ, **(env or {})} if env else None
    proc = subprocess.Popen(
        [binary, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=merged,
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
        return stdout
    except subprocess.TimeoutExpired:
        print(f"WARNING: timeout: {binary} {' '.join(args)}", file=sys.stderr)
        proc.kill()
        proc.wait()
        return ""


def cleanup_ipc_socket(addr: str):
    if addr.startswith("ipc://") and not addr.startswith("ipc://@"):
        path = addr[len("ipc://"):]
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def kill_process(proc: subprocess.Popen):
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass


# ── measurement parsing ──────────────────────────────────────────

def read_proc_cpu(pid: int) -> float:
    """Read user+sys CPU time in seconds from /proc/[pid]/stat."""
    try:
        fields = open(f"/proc/{pid}/stat").read().split()
        utime = int(fields[13])
        stime = int(fields[14])
        return (utime + stime) / os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError):
        return 0.0


def parse_throughput(output: str, size: int) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 2:
        return None
    count = float(parts[0])
    elapsed = float(parts[1])
    if elapsed <= 0:
        return None
    msgs_s = count / elapsed
    mbps = (count * size) / elapsed / 1e6
    result = {"msgs_s": msgs_s, "mbps": mbps, "elapsed": elapsed}
    if len(parts) >= 4:
        try:
            result["pull_cpu"] = float(parts[3])
        except ValueError:
            pass
    return result


def parse_latency(output: str) -> dict | None:
    parts = output.strip().split()
    if len(parts) < 5:
        return None
    result = {
        "p50_us": float(parts[0]),
        "p99_us": float(parts[1]),
        "p999_us": float(parts[2]),
        "max_us": float(parts[3]),
        "iterations": int(parts[4]),
    }
    if len(parts) >= 6:
        try:
            result["req_cpu"] = float(parts[5])
        except ValueError:
            pass
    if len(parts) >= 7:
        try:
            result["elapsed"] = float(parts[6])
        except ValueError:
            pass
    return result


# ── benchmark cells ──────────────────────────────────────────────

def run_throughput_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc",
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    env: dict | None = None,
) -> dict | None:
    best = None
    # Extra retries for inproc: compio cross-thread waker bug causes
    # intermittent hangs. With 5s timeout per attempt, retries are cheap.
    effective_rounds = max(rounds, 5) if transport == "inproc" else rounds
    for _ in range(effective_rounds):
        result = _run_throughput_once(binary, transport, addr, size,
                                      inproc_subcmd, duration, env=env)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _fresh_addr(addr: str) -> str:
    """Return a unique variant of an IPC address to avoid kernel cleanup races."""
    if addr.startswith("ipc://"):
        return f"{addr}-{next_addr_id()}"
    return addr


def _run_throughput_once(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str, duration: float, env: dict | None = None,
) -> dict | None:
    dur = str(duration)
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        timeout_s = max(int(duration) + 5, 8)
        output, cpu = capture_with_cpu(binary, inproc_subcmd, fresh_name,
                                       str(size), dur,
                                       timeout=timeout_s, env=env)
        result = parse_throughput(output, size)
        if result and cpu > 0:
            result["cpu_time"] = cpu
        return result

    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    push = spawn_process(binary, "push", addr, str(size), env=env)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(push)
        if port is None:
            kill_process(push)
            return None
        connect_addr = str(port)
    try:
        output = capture_process(binary, "pull", connect_addr, str(size), dur,
                                 env=env)
        push_cpu = read_proc_cpu(push.pid)
    finally:
        kill_process(push)
        cleanup_ipc_socket(addr)
    result = parse_throughput(output, size)
    if result and push_cpu > 0:
        pull_cpu = result.get("pull_cpu", 0.0)
        result["cpu_time"] = push_cpu + pull_cpu
    return result


def run_pubsub_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str = "inproc-pubsub",
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_pubsub_once(binary, transport, addr, size, peers,
                                  inproc_subcmd, duration)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _run_pubsub_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    inproc_subcmd: str, duration: float,
) -> dict | None:
    dur = str(duration)
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        timeout_s = max(int(duration) + 5, 8)
        output = capture_process(binary, inproc_subcmd, fresh_name, str(size),
                                 dur, str(peers), timeout=timeout_s)
        result = parse_throughput(output, size)
    else:
        addr = _fresh_addr(addr)
        cleanup_ipc_socket(addr)
        pub_ = spawn_process(binary, "pub", addr, str(size))
        if transport in ("ipc", "ws"):
            time.sleep(0.2)
            connect_addr = addr
        else:
            port = read_bound_port(pub_)
            if port is None:
                kill_process(pub_)
                return None
            connect_addr = str(port)
        drain_subs = []
        try:
            for _ in range(peers - 1):
                drain_subs.append(spawn_process(binary, "sub", connect_addr,
                                                str(size), dur))
            time.sleep(0.05)
            output = capture_process(binary, "sub", connect_addr, str(size), dur)
            pub_cpu = read_proc_cpu(pub_.pid)
        finally:
            kill_process(pub_)
            for s in drain_subs:
                kill_process(s)
            cleanup_ipc_socket(addr)
        result = parse_throughput(output, size)
        if result and pub_cpu > 0:
            pull_cpu = result.get("pull_cpu", 0.0)
            result["cpu_time"] = pub_cpu + pull_cpu
    if result and peers > 1:
        result["mbps"] *= peers
    return result


def run_fanout_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_fanout_once(binary, transport, addr, size, peers, duration)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _run_fanout_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float,
) -> dict | None:
    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    push = spawn_process(binary, "push", addr, str(size))
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(push)
        if port is None:
            kill_process(push)
            return None
        connect_addr = str(port)
    drains = []
    try:
        for _ in range(peers - 1):
            drains.append(spawn_process(binary, "pull", connect_addr,
                                        str(size), str(duration)))
        time.sleep(0.05)
        output = capture_process(binary, "pull", connect_addr, str(size),
                                 str(duration))
        push_cpu = read_proc_cpu(push.pid)
    finally:
        kill_process(push)
        for d in drains:
            kill_process(d)
        cleanup_ipc_socket(addr)
    result = parse_throughput(output, size)
    if result and push_cpu > 0:
        pull_cpu = result.get("pull_cpu", 0.0)
        result["cpu_time"] = push_cpu + pull_cpu
    if result and peers > 1:
        result["mbps"] *= peers
    return result


def run_fanin_cell(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
) -> dict | None:
    best = None
    for _ in range(rounds):
        result = _run_fanin_once(binary, transport, addr, size, peers, duration)
        if result and (best is None or result["msgs_s"] > best["msgs_s"]):
            best = result
    return best


def _run_fanin_once(
    binary: str, transport: str, addr: str, size: int, peers: int,
    duration: float,
) -> dict | None:
    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    dur = str(duration)
    pull = spawn_process(binary, "pull-bind", addr, str(size), dur)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(pull)
        if port is None:
            kill_process(pull)
            return None
        connect_addr = str(port)
    pushers = []
    try:
        for _ in range(peers):
            pushers.append(spawn_process(binary, "push-connect", connect_addr,
                                         str(size)))
        stdout, _ = pull.communicate(timeout=max(int(duration) + 10, 15))
        pushers_cpu = sum(read_proc_cpu(p.pid) for p in pushers)
    except subprocess.TimeoutExpired:
        kill_process(pull)
        stdout = ""
        pushers_cpu = 0.0
    finally:
        for p in pushers:
            kill_process(p)
        cleanup_ipc_socket(addr)
    result = parse_throughput(stdout, size)
    if result and pushers_cpu > 0:
        pull_cpu = result.get("pull_cpu", 0.0)
        result["cpu_time"] = pushers_cpu + pull_cpu
    return result


def run_latency_cell(
    binary: str, transport: str, addr: str, size: int,
    inproc_subcmd: str = "inproc-latency",
    iterations: int = LATENCY_ITERATIONS,
    warmup: int = LATENCY_WARMUP,
    timeout: int = LATENCY_TIMEOUT,
    env: dict | None = None,
) -> dict | None:
    if transport == "inproc":
        fresh_name = f"{addr}-{next_addr_id()}"
        output, cpu = capture_with_cpu(
            binary, inproc_subcmd, fresh_name, str(size),
            str(iterations), str(warmup),
            timeout=timeout, env=env,
        )
        result = parse_latency(output)
        if result and cpu > 0:
            result["cpu_time"] = cpu
        return result

    addr = _fresh_addr(addr)
    cleanup_ipc_socket(addr)
    rep = spawn_process(binary, "rep", addr, str(size), env=env)
    if transport in ("ipc", "ws"):
        time.sleep(0.2)
        connect_addr = addr
    else:
        port = read_bound_port(rep)
        if port is None:
            kill_process(rep)
            return None
        connect_addr = str(port)
    try:
        output = capture_process(
            binary, "req", connect_addr, str(size),
            str(iterations), str(warmup),
            timeout=timeout, env=env,
        )
        rep_cpu = read_proc_cpu(rep.pid)
    finally:
        kill_process(rep)
        cleanup_ipc_socket(addr)
    result = parse_latency(output)
    if result and rep_cpu > 0:
        req_cpu = result.get("req_cpu", 0.0)
        result["cpu_time"] = rep_cpu + req_cpu
    return result


# ── address generation ────────────────────────────────────────────

_addr_counter = 0

def next_addr_id() -> int:
    global _addr_counter
    _addr_counter += 1
    return _addr_counter

def addr_for(transport: str, prefix: str, idx: int, base_port: int,
             *, impl_name: str = "") -> str:
    uid = next_addr_id()
    if transport == "tcp":
        return "0"
    if transport == "ws":
        offsets = {"c": 500, "t": 600, "z": 700, "q": 800, "s": 900, "r": 1100, "m": 1300}
        return f"ws://127.0.0.1:{base_port + offsets.get(prefix, 500) + idx}/"
    if transport == "ipc":
        if impl_name in ("zmq.rs", "rzmq", "rust-zmq"):
            return f"ipc:///tmp/omq-bench-cmp-{prefix}-{uid}"
        return f"ipc://@omq-bench-cmp-{prefix}-{uid}"
    if transport == "inproc":
        return f"bench-cmp-{prefix}-{uid}"
    return "0"


# ── JSONL I/O ─────────────────────────────────────────────────────

def append_jsonl(row: dict):
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSONL_PATH, "a") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


# ── impl definitions ─────────────────────────────────────────────

IMPLS = {
    "omq-compio": {
        "crate": "omq-compio",
        "bin": "bench_peer_compio",
        "prefix": "c",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "omq-compio-st": {
        "binary_from": "omq-compio",
        "prefix": "s",
        "transports": ["inproc"],
        "inproc_tput_subcmd": "inproc-st",
        "inproc_lat_subcmd": "inproc-st-latency",
    },
    "omq-tokio": {
        "crate": "omq-tokio",
        "bin": "bench_peer_tokio",
        "prefix": "t",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "omq-tokio-mt": {
        "binary_from": "omq-tokio",
        "prefix": "u",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
        "env": {"OMQ_BENCH_RUNTIME": "multi_thread"},
    },
    "libzmq": {
        "prefix": "z",
        "transports": ["tcp", "inproc", "ipc", "ws"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
    "zmq.rs": {
        "prefix": "q",
        "transports": ["tcp", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "rzmq": {
        "prefix": "r",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "supports_pubsub": True,
    },
    "omq-libzmq": {
        "prefix": "m",
        "transports": ["tcp", "inproc", "ipc"],
        "inproc_tput_subcmd": "inproc",
        "inproc_lat_subcmd": "inproc-latency",
        "inproc_pubsub_subcmd": "inproc-pubsub",
        "supports_pubsub": True,
    },
}

PUBSUB_PEER_COUNTS = [1, 8, 64]
FANOUT_PEER_COUNTS = [2, 4, 8]
FANIN_PEER_COUNTS = [2, 4, 8]


def build_peers(impl_names: set[str], ws_needed: bool):
    binaries = {}
    features = ["ws"] if ws_needed else []

    if "omq-compio" in impl_names or "omq-compio-st" in impl_names:
        print("==> building omq-compio bench_peer...", file=sys.stderr)
        cargo_build("omq-compio", "bench_peer_compio", features=features or None)
        compio_bin = str(ROOT / "target" / "release" / "bench_peer_compio")
        if "omq-compio" in impl_names:
            binaries["omq-compio"] = compio_bin
        if "omq-compio-st" in impl_names:
            binaries["omq-compio-st"] = compio_bin

    if impl_names & {"omq-tokio", "omq-tokio-mt"}:
        print("==> building omq-tokio bench_peer...", file=sys.stderr)
        tokio_features = list(features) if features else []
        tokio_features.append("rt-multi-thread")
        cargo_build("omq-tokio", "bench_peer_tokio", features=tokio_features)
        tokio_bin = str(ROOT / "target" / "release" / "bench_peer_tokio")
        if "omq-tokio" in impl_names:
            binaries["omq-tokio"] = tokio_bin
        if "omq-tokio-mt" in impl_names:
            binaries["omq-tokio-mt"] = tokio_bin

    if "libzmq" in impl_names:
        print("==> building libzmq bench_peer...", file=sys.stderr)
        src = ROOT / "scripts" / "libzmq_bench_peer.c"
        out = ROOT / "scripts" / "libzmq_bench_peer"
        gcc_build(src, out)
        binaries["libzmq"] = str(out)

    if "zmq.rs" in impl_names:
        print("==> building zmq.rs bench_peer...", file=sys.stderr)
        zmqrs_dir = ROOT / "scripts" / "zmqrs_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=zmqrs_dir, check=True,
        )
        binaries["zmq.rs"] = str(zmqrs_dir / "target" / "release" / "zmqrs_bench_peer")

    if "rzmq" in impl_names:
        print("==> building rzmq bench_peer...", file=sys.stderr)
        rzmq_dir = ROOT / "scripts" / "rzmq_bench_peer"
        subprocess.run(
            ["cargo", "build", "--release", "-q"],
            cwd=rzmq_dir, check=True,
        )
        binaries["rzmq"] = str(rzmq_dir / "target" / "release" / "rzmq_bench_peer")

    if "omq-libzmq" in impl_names:
        print("==> building omq-libzmq bench_peer...", file=sys.stderr)
        subprocess.run(
            ["cargo", "build", "--release", "-p", "omq-libzmq", "-q"],
            cwd=ROOT, check=True,
        )
        src = ROOT / "scripts" / "libzmq_bench_peer.c"
        out = ROOT / "scripts" / "omq_libzmq_bench_peer"
        inc = ROOT / "omq-libzmq" / "include"
        lib_dir = ROOT / "target" / "release"
        subprocess.run(
            ["gcc", "-O2", "-o", str(out), str(src),
             f"-I{inc}", f"-L{lib_dir}", "-lomq_zmq", "-lpthread",
             f"-Wl,-rpath,{lib_dir}"],
            check=True,
        )
        binaries["omq-libzmq"] = str(out)

    return binaries


def run_benchmarks(
    binaries: dict[str, str],
    transports: list[str],
    sizes: list[int],
    run_latency: bool,
    run_pubsub: bool,
    pubsub_peers: list[int],
    base_port: int,
    run_id: str,
    duration: float = DEFAULT_DURATION,
    rounds: int = DEFAULT_ROUNDS,
    latency_iterations: int = LATENCY_ITERATIONS,
    latency_warmup: int = LATENCY_WARMUP,
    latency_timeout: int = LATENCY_TIMEOUT,
    run_fanout: bool = False,
    fanout_peers: list[int] | None = None,
    run_fanin: bool = False,
    fanin_peers: list[int] | None = None,
):
    _cleanup_ipc_sockets()
    atexit.register(_cleanup_ipc_sockets)
    for transport in transports:
        active = {
            name: path for name, path in binaries.items()
            if transport in IMPLS[name]["transports"]
        }
        if not active:
            continue

        # throughput
        print(f"\n── throughput: {transport} ──", file=sys.stderr)
        header = "".join(f"  {name:>22s}" for name in active)
        print(f"{'size':>10s}{header}", file=sys.stderr)

        for idx, size in enumerate(sizes):
            cells = {}
            for name, binary in active.items():
                impl_def = IMPLS[name]
                prefix = impl_def["prefix"]
                addr = addr_for(transport, prefix, idx, base_port,
                               impl_name=name)
                subcmd = impl_def.get("inproc_tput_subcmd", "inproc")
                impl_env = impl_def.get("env")
                result = run_throughput_cell(binary, transport, addr, size,
                                            inproc_subcmd=subcmd,
                                            duration=duration, rounds=rounds,
                                            env=impl_env)
                cells[name] = result
                if result:
                    row = {
                        "run_id": run_id,
                        "impl": name,
                        "kind": "throughput",
                        "transport": transport,
                        "msg_size": size,
                        "msgs_s": round(result["msgs_s"], 1),
                        "mbps": round(result["mbps"], 1),
                    }
                    if "elapsed" in result:
                        row["elapsed"] = round(result["elapsed"], 6)
                    if "cpu_time" in result:
                        row["cpu_time"] = round(result["cpu_time"], 6)
                    append_jsonl(row)

            line = f"{size_label(size):>10s}"
            for name in active:
                r = cells.get(name)
                if r:
                    line += f"  {r['msgs_s']:>9.0f} msg/s {r['mbps']:>6.1f} MB/s"
                else:
                    line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
            print(line, file=sys.stderr)

        # latency
        if run_latency:
            print(f"\n── latency: {transport} ──", file=sys.stderr)
            header = "".join(f"  {name:>24s}" for name in active)
            print(f"{'size':>10s}{header}", file=sys.stderr)

            for idx, size in enumerate(sizes):
                cells = {}
                for name, binary in active.items():
                    impl_def = IMPLS[name]
                    prefix = impl_def["prefix"]
                    addr = addr_for(transport, prefix, idx + len(sizes), base_port,
                                   impl_name=name)
                    subcmd = impl_def.get("inproc_lat_subcmd", "inproc-latency")
                    impl_env = impl_def.get("env")
                    result = run_latency_cell(binary, transport, addr, size,
                                             inproc_subcmd=subcmd,
                                             iterations=latency_iterations,
                                             warmup=latency_warmup,
                                             timeout=latency_timeout,
                                             env=impl_env)
                    cells[name] = result
                    if result:
                        row = {
                            "run_id": run_id,
                            "impl": name,
                            "kind": "latency",
                            "transport": transport,
                            "msg_size": size,
                            "p50_us": round(result["p50_us"], 3),
                            "p99_us": round(result["p99_us"], 3),
                            "p999_us": round(result["p999_us"], 3),
                            "max_us": round(result["max_us"], 3),
                            "iterations": result["iterations"],
                        }
                        if "cpu_time" in result:
                            row["cpu_time"] = round(result["cpu_time"], 6)
                        if "elapsed" in result:
                            row["elapsed"] = round(result["elapsed"], 6)
                        append_jsonl(row)

                line = f"{size_label(size):>10s}"
                for name in active:
                    r = cells.get(name)
                    if r:
                        line += f"    p50={r['p50_us']:>7.1f} µs  p99={r['p99_us']:>7.1f} µs"
                    else:
                        line += f"    {'—':>24s}"
                print(line, file=sys.stderr)

        # pub/sub throughput
        if run_pubsub:
            pubsub_active = {
                name: path for name, path in active.items()
                if IMPLS[name].get("supports_pubsub")
            }
        else:
            pubsub_active = {}
        if pubsub_active:
            for peers in pubsub_peers:
                print(f"\n── pub/sub {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in pubsub_active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in pubsub_active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 200 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset, base_port)
                        subcmd = impl_def.get("inproc_pubsub_subcmd",
                                              "inproc-pubsub")
                        result = run_pubsub_cell(
                            binary, transport, addr, size, peers,
                            inproc_subcmd=subcmd,
                            duration=duration, rounds=rounds,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "pub_sub",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            append_jsonl(row)

                    line = f"{size_label(size):>10s}"
                    for name in pubsub_active:
                        r = cells.get(name)
                        if r:
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
                    print(line, file=sys.stderr)

        # fan-out (1 PUSH → N PULL)
        if run_fanout and transport == "tcp":
            for peers in (fanout_peers or FANOUT_PEER_COUNTS):
                print(f"\n── fan-out {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 300 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset,
                                        base_port, impl_name=name)
                        result = run_fanout_cell(
                            binary, transport, addr, size, peers,
                            duration=duration, rounds=rounds,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "fan_out",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            append_jsonl(row)

                    line = f"{size_label(size):>10s}"
                    for name in active:
                        r = cells.get(name)
                        if r:
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
                    print(line, file=sys.stderr)

        # fan-in (N PUSH → 1 PULL)
        if run_fanin and transport == "tcp":
            for peers in (fanin_peers or FANIN_PEER_COUNTS):
                print(f"\n── fan-in {peers}p: {transport} ──", file=sys.stderr)
                header = "".join(f"  {name:>22s}" for name in active)
                print(f"{'size':>10s}{header}", file=sys.stderr)

                for idx, size in enumerate(sizes):
                    cells = {}
                    for name, binary in active.items():
                        impl_def = IMPLS[name]
                        prefix = impl_def["prefix"]
                        port_offset = 400 + peers * 50 + idx
                        addr = addr_for(transport, prefix, port_offset,
                                        base_port, impl_name=name)
                        result = run_fanin_cell(
                            binary, transport, addr, size, peers,
                            duration=duration, rounds=rounds,
                        )
                        cells[name] = result
                        if result:
                            row = {
                                "run_id": run_id,
                                "impl": name,
                                "kind": "fan_in",
                                "transport": transport,
                                "peers": peers,
                                "msg_size": size,
                                "msgs_s": round(result["msgs_s"], 1),
                                "mbps": round(result["mbps"], 1),
                            }
                            if "elapsed" in result:
                                row["elapsed"] = round(result["elapsed"], 6)
                            if "cpu_time" in result:
                                row["cpu_time"] = round(result["cpu_time"], 6)
                            append_jsonl(row)

                    line = f"{size_label(size):>10s}"
                    for name in active:
                        r = cells.get(name)
                        if r:
                            line += (f"  {r['msgs_s']:>9.0f} msg/s"
                                     f" {r['mbps']:>6.1f} MB/s")
                        else:
                            line += f"  {'—':>9s} msg/s {'—':>6s} MB/s"
                    print(line, file=sys.stderr)

    print(file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Run comparison benchmarks")
    parser.add_argument(
        "--impl", action="append", dest="impls",
        choices=list(IMPLS.keys()),
        help="implementation(s) to benchmark (default: all)",
    )
    parser.add_argument(
        "--transport", action="append",
        choices=["tcp", "inproc", "ipc", "ws"],
        help="transport(s) to benchmark (default: tcp + inproc + ipc)",
    )
    parser.add_argument(
        "--quick-run", action="store_true",
        help=f"3 sizes, {QUICK_ROUNDS} round of {QUICK_DURATION}s (unless overridden)",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help=f"seconds per throughput round (default: {DEFAULT_DURATION}, quick: {QUICK_DURATION})",
    )
    parser.add_argument(
        "--rounds", type=int, default=None,
        help=f"throughput rounds per cell, best-of-N (default: {DEFAULT_ROUNDS}, quick: {QUICK_ROUNDS})",
    )
    parser.add_argument(
        "--no-latency", action="store_true",
        help="skip REQ/REP latency benchmarks (on by default)",
    )
    parser.add_argument(
        "--no-pubsub", action="store_true",
        help="skip PUB/SUB throughput benchmarks",
    )
    parser.add_argument(
        "--pubsub-peers", type=str, default=None,
        help=f"comma-separated peer counts for PUB/SUB (default: {','.join(str(p) for p in PUBSUB_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--latency-iterations", type=int, default=LATENCY_ITERATIONS,
        help=f"measured round-trips per latency cell (default: {LATENCY_ITERATIONS})",
    )
    parser.add_argument(
        "--latency-warmup", type=int, default=LATENCY_WARMUP,
        help=f"warmup round-trips before measuring (default: {LATENCY_WARMUP})",
    )
    parser.add_argument(
        "--latency-timeout", type=int, default=LATENCY_TIMEOUT,
        help=f"timeout in seconds for latency subprocess (default: {LATENCY_TIMEOUT})",
    )
    parser.add_argument(
        "--fanout", action="store_true",
        help="run PUSH fan-out benchmarks (1 PUSH → N PULL, TCP only)",
    )
    parser.add_argument(
        "--fanout-peers", type=str, default=None,
        help=f"comma-separated peer counts for fan-out (default: {','.join(str(p) for p in FANOUT_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--fanin", action="store_true",
        help="run PUSH fan-in benchmarks (N PUSH → 1 PULL, TCP only)",
    )
    parser.add_argument(
        "--fanin-peers", type=str, default=None,
        help=f"comma-separated peer counts for fan-in (default: {','.join(str(p) for p in FANIN_PEER_COUNTS)})",
    )
    parser.add_argument(
        "--base-port", type=int, default=0,
        help="base TCP port (default: random ephemeral)",
    )
    parser.add_argument(
        "--id", type=str, default=None,
        help="override run_id (default: ISO timestamp)",
    )
    args = parser.parse_args()

    transports = args.transport or ["tcp", "inproc", "ipc"]
    sizes = QUICK_SIZES if args.quick_run else FULL_SIZES
    if args.quick_run:
        duration = args.duration if args.duration is not None else QUICK_DURATION
        rounds = args.rounds if args.rounds is not None else QUICK_ROUNDS
    else:
        duration = args.duration if args.duration is not None else DEFAULT_DURATION
        rounds = args.rounds if args.rounds is not None else DEFAULT_ROUNDS
    run_id = args.id or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    run_latency = not args.no_latency
    run_pubsub = not args.no_pubsub
    pubsub_peers = (
        [int(x) for x in args.pubsub_peers.split(",")]
        if args.pubsub_peers else PUBSUB_PEER_COUNTS
    )
    ws_needed = "ws" in transports

    impl_names = set(args.impls) if args.impls else set(IMPLS.keys())

    binaries = build_peers(impl_names, ws_needed)

    versions = []
    if impl_names & {"omq-compio", "omq-compio-st", "omq-tokio"}:
        versions.append(f"omq {cargo_version('omq-compio')}")
    if "libzmq" in impl_names:
        versions.append(f"libzmq {libzmq_version()}")
    if "zmq.rs" in impl_names:
        versions.append(f"zmq.rs {cargo_version('zeromq', manifest=ROOT / 'scripts' / 'zmqrs_bench_peer' / 'Cargo.toml')}")
    if "rzmq" in impl_names:
        versions.append(f"rzmq {cargo_version('rzmq', manifest=ROOT / 'scripts' / 'rzmq_bench_peer' / 'Cargo.toml')}")
    if "omq-libzmq" in impl_names:
        versions.append(f"omq-libzmq {cargo_version('omq-libzmq')}")
    print(" vs ".join(versions), file=sys.stderr)

    base_port = args.base_port or random.randint(20_000, 40_000)
    fanout_peers = (
        [int(x) for x in args.fanout_peers.split(",")]
        if args.fanout_peers else None
    )
    fanin_peers = (
        [int(x) for x in args.fanin_peers.split(",")]
        if args.fanin_peers else None
    )
    run_benchmarks(binaries, transports, sizes, run_latency,
                   run_pubsub, pubsub_peers, base_port, run_id,
                   duration=duration, rounds=rounds,
                   latency_iterations=args.latency_iterations,
                   latency_warmup=args.latency_warmup,
                   latency_timeout=args.latency_timeout,
                   run_fanout=args.fanout,
                   fanout_peers=fanout_peers,
                   run_fanin=args.fanin,
                   fanin_peers=fanin_peers)


if __name__ == "__main__":
    main()
