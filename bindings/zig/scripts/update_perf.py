#!/usr/bin/env python3
"""Measure Zig ZeroMQ binding throughput and latency.

Adapted from bindings/pyomq/scripts/update_perf.py. Full runs append to
~/.cache/omq.zig/bindings.jsonl and generate doc/charts/bindings.svg.
"""

import argparse
import json
import math
import os
import select
import shutil
import subprocess
import sys
import tempfile
import time

DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
THROUGHPUT_MSG_MAX = 12_000_000
THROUGHPUT_MSG_STEP = 2_000_000
LATENCY_US_MAX = 120
LATENCY_US_STEP = 20
SIZES = DEFAULT_SIZES.copy()
TARGET_RUNTIME_S = 2.5
THROUGHPUT_WARMUP_S = 0.5
N_ROUNDS = 3
LATENCY_WARMUP_S = 0.5
LATENCY_RUNTIME_S = 1.5
SUBPROCESS_TIMEOUT_S = 45.0

SCRIPT_DIR = os.path.dirname(__file__)
BINDING_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
REPO_ROOT = os.path.abspath(os.path.join(BINDING_DIR, "..", ".."))
CHART_DIR = os.path.join(BINDING_DIR, "doc", "charts")
CACHE_DIR = os.path.join(
    os.environ.get("OMQ_ZIG_CACHE_DIR")
    or os.path.join(os.environ.get("XDG_CACHE_HOME", os.path.expanduser("~/.cache")), "omq.zig")
)
JSONL_FILE = os.path.join(CACHE_DIR, "bindings.jsonl")
SRC_DIR = os.path.join(CACHE_DIR, "src")
BIN_DIR = os.path.join(CACHE_DIR, "bin")

IMPLS = {
    "omq.zig": {
        "repo": None,
        "bench": os.path.join(BINDING_DIR, "zig-out", "bin", "omq-zig-bench"),
    },
    "zzmq": {
        "repo": "https://github.com/nine-lives-later/zzmq",
        "bench": os.path.join(BIN_DIR, "zzmq-bench"),
    },
    "zimq": {
        "repo": "https://github.com/uyha/zimq",
        "tag": "zig-0.16",
        "bench": os.path.join(BIN_DIR, "zimq-bench"),
    },
}


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
    global LATENCY_WARMUP_S
    global LATENCY_RUNTIME_S
    global SUBPROCESS_TIMEOUT_S

    if args.quick:
        SIZES = QUICK_SIZES.copy()
        TARGET_RUNTIME_S = 0.5
        THROUGHPUT_WARMUP_S = 0.1
        N_ROUNDS = 1
        LATENCY_WARMUP_S = 0.1
        LATENCY_RUNTIME_S = 0.5
        SUBPROCESS_TIMEOUT_S = 20.0

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


def run(cmd, *, cwd=None, timeout=None):
    r = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{r.stdout}{r.stderr}")
    if "warning:" in r.stdout.lower() or "warning:" in r.stderr.lower():
        raise RuntimeError(f"{' '.join(cmd)} printed warning:\n{r.stdout}{r.stderr}")
    return r


def ensure_clone(name):
    repo = IMPLS[name]["repo"]
    dest = os.path.join(SRC_DIR, name)
    if not os.path.isdir(os.path.join(dest, ".git")):
        os.makedirs(SRC_DIR, exist_ok=True)
        run(["git", "-c", "init.templateDir=", "clone", "--depth", "1", repo, dest], timeout=60)
    if "tag" in IMPLS[name]:
        run(["git", "fetch", "--depth", "1", "origin", "tag", IMPLS[name]["tag"]], cwd=dest)
        run(["git", "checkout", f"tags/{IMPLS[name]['tag']}"], cwd=dest)
    return dest


def build_omq():
    run(["cargo", "build", "--release", "-p", "omq-libzmq"], cwd=REPO_ROOT, timeout=180)
    run(["zig", "build", "-Doptimize=ReleaseFast"], cwd=BINDING_DIR, timeout=120)


def build_zzmq():
    src = ensure_clone("zzmq")
    os.makedirs(CACHE_DIR, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="zzmq-", dir=CACHE_DIR) as work:
        shutil.copytree(src, os.path.join(work, "zzmq"), symlinks=True)
        patched = os.path.join(work, "zzmq", "src", "classes", "zmessage.zig")
        with open(patched) as f:
            content = f.read()
        content = content.replace("callconv(.C)", "callconv(.c)")
        with open(patched, "w") as f:
            f.write(content)
        out = IMPLS["zzmq"]["bench"]
        os.makedirs(os.path.dirname(out), exist_ok=True)
        run(
            [
                "zig",
                "build-exe",
                "-O",
                "ReleaseFast",
                "--dep",
                "zzmq",
                f"-Mroot={os.path.join(BINDING_DIR, 'scripts', 'bench', 'zzmq_bench.zig')}",
                f"-Mzzmq={os.path.join(work, 'zzmq', 'src', 'zzmq.zig')}",
                "-lc",
                "-lzmq",
                f"-femit-bin={out}",
            ],
            timeout=120,
        )


def build_zimq():
    src = ensure_clone("zimq")
    os.makedirs(CACHE_DIR, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="zimq-build-", dir=CACHE_DIR) as work:
        deps_dir = os.path.join(work, "deps")
        os.makedirs(deps_dir, exist_ok=True)
        os.symlink(src, os.path.join(deps_dir, "zimq"))
        build_zig = f"""
const std = @import("std");
pub fn build(b: *std.Build) void {{
    const target = b.standardTargetOptions(.{{}});
    const optimize = b.standardOptimizeOption(.{{}});
    const zimq_dep = b.dependency("zimq", .{{
        .target = target,
        .optimize = optimize,
        .curve = false,
    }});
    const exe = b.addExecutable(.{{
        .name = "zimq-bench",
        .root_module = b.createModule(.{{
            .root_source_file = .{{ .cwd_relative = "{os.path.join(BINDING_DIR, 'scripts', 'bench', 'zimq_bench.zig')}" }},
            .target = target,
            .optimize = optimize,
            .imports = &.{{ .{{ .name = "zimq", .module = zimq_dep.module("zimq") }} }},
        }}),
    }});
    b.installArtifact(exe);
}}
"""
        build_zon = f""".{{
    .name = .zimq_bench,
    .version = "0.0.0",
    .fingerprint = 0xaef28a37f715d243,
    .minimum_zig_version = "0.16.0",
    .dependencies = .{{
        .zimq = .{{ .path = "deps/zimq" }},
    }},
    .paths = .{{ "build.zig" }},
}}
"""
        with open(os.path.join(work, "build.zig"), "w") as f:
            f.write(build_zig)
        with open(os.path.join(work, "build.zig.zon"), "w") as f:
            f.write(build_zon)
        run(["zig", "build", "-Doptimize=ReleaseFast", "--prefix", CACHE_DIR], cwd=work, timeout=240)


def build_impls(impls):
    for impl in impls:
        print(f"building {impl}...")
        if impl == "omq.zig":
            build_omq()
        elif impl == "zzmq":
            build_zzmq()
        elif impl == "zimq":
            build_zimq()


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


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def median(values, key=lambda value: value):
    ordered = sorted(values, key=key)
    return ordered[len(ordered) // 2]


def fmt_rate(rate):
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f} M/s"
    return f"{rate / 1_000:.0f} k/s"


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def run_bench(exe, args, timeout):
    r = run([exe, *args], timeout=timeout)
    return float(r.stdout.strip())


def _fail_process(proc):
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def run_throughput_tcp(exe, size, duration):
    push_proc = subprocess.Popen(
        [exe, "throughput-push", str(size), str(duration)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        text=True,
    )
    assert push_proc.stdout is not None
    assert push_proc.stderr is not None
    assert push_proc.stdin is not None
    try:
        ready, _, _ = select.select([push_proc.stdout], [], [], SUBPROCESS_TIMEOUT_S)
        if not ready:
            _fail_process(push_proc)
            raise RuntimeError(f"{exe} throughput-push {size}B timed out before endpoint")
        endpoint = push_proc.stdout.readline().strip()
        if not endpoint:
            _, stderr = push_proc.communicate(timeout=5)
            raise RuntimeError(f"{exe} throughput-push {size}B failed:\n{stderr}")

        pull = run(
            [exe, "throughput-pull", endpoint, str(size)],
            timeout=SUBPROCESS_TIMEOUT_S,
        )
        return float(pull.stdout.strip())
    finally:
        try:
            push_proc.stdin.write("\n")
            push_proc.stdin.flush()
        except OSError:
            pass
        try:
            stdout, stderr = push_proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            _fail_process(push_proc)
            raise RuntimeError(f"{exe} throughput-push {size}B did not exit")
        if push_proc.returncode != 0:
            raise RuntimeError(f"{exe} throughput-push {size}B failed:\n{stdout}{stderr}")
        if "warning:" in stdout.lower() or "warning:" in stderr.lower():
            raise RuntimeError(f"{exe} throughput-push {size}B printed warning:\n{stdout}{stderr}")


def run_latency_tcp(exe, size, warmup_s, duration_s):
    rep_proc = subprocess.Popen(
        [exe, "latency-rep", str(size)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert rep_proc.stdout is not None
    assert rep_proc.stderr is not None
    try:
        ready, _, _ = select.select([rep_proc.stdout], [], [], SUBPROCESS_TIMEOUT_S)
        if not ready:
            _fail_process(rep_proc)
            raise RuntimeError(f"{exe} latency-rep {size}B timed out before endpoint")
        endpoint = rep_proc.stdout.readline().strip()
        if not endpoint:
            _, stderr = rep_proc.communicate(timeout=5)
            raise RuntimeError(f"{exe} latency-rep {size}B failed:\n{stderr}")

        req = run(
            [exe, "latency", str(size), str(warmup_s), str(duration_s), endpoint],
            timeout=SUBPROCESS_TIMEOUT_S,
        )
        return float(req.stdout.strip())
    finally:
        try:
            stdout, stderr = rep_proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            _fail_process(rep_proc)
            raise RuntimeError(f"{exe} latency-rep {size}B did not exit")
        if rep_proc.returncode != 0:
            raise RuntimeError(f"{exe} latency-rep {size}B failed:\n{stdout}{stderr}")
        if "warning:" in stdout.lower() or "warning:" in stderr.lower():
            raise RuntimeError(f"{exe} latency-rep {size}B printed warning:\n{stdout}{stderr}")


def run_throughput(impl):
    results = []
    exe = IMPLS[impl]["bench"]
    for size in SIZES:
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()
        runs = []
        for _ in range(N_ROUNDS):
            if THROUGHPUT_WARMUP_S > 0:
                run_throughput_tcp(exe, size, THROUGHPUT_WARMUP_S)
            runs.append(run_throughput_tcp(exe, size, TARGET_RUNTIME_S))
        rate = median(runs)
        results.append(rate)
        print(f" {fmt_rate(rate):>10}")
    return results


def run_latency(impl):
    results = []
    exe = IMPLS[impl]["bench"]
    for size in latency_sizes_from(SIZES):
        label = fmt_size(size)
        sys.stdout.write(f"  {label:>7} ...")
        sys.stdout.flush()
        runs = [
            run_latency_tcp(exe, size, LATENCY_WARMUP_S, LATENCY_RUNTIME_S)
            for _ in range(N_ROUNDS)
        ]
        p50 = median(runs)
        results.append(p50)
        print(f" p50 {p50:.1f} us")
    return results


def save_results(run_id, impl, throughput, latency):
    rows = []
    for i, size in enumerate(SIZES):
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "throughput",
                "mode": "sync",
                "transport": "tcp",
                "msg_size": size,
                "msgs_s": throughput[i],
            }
        )
    for i, size in enumerate(latency_sizes_from(SIZES)):
        rows.append(
            {
                "run_id": run_id,
                "impl": impl,
                "kind": "latency",
                "mode": "sync",
                "transport": "tcp",
                "msg_size": size,
                "p50_us": latency[i],
            }
        )
    append_jsonl(rows)
    print(f"  appended {len(rows)} rows to {JSONL_FILE}")


def chart_data_from_jsonl():
    rows = load_jsonl()
    latest = {}
    for r in rows:
        key = (r.get("impl"), r.get("kind"), r.get("mode"), r.get("transport"), r.get("msg_size"))
        prev = latest.get(key)
        if prev is None or r.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = r

    def get_tp(impl, size):
        r = latest.get((impl, "throughput", "sync", "tcp", size))
        return r["msgs_s"] if r else 0.0

    def get_lat(impl, size):
        r = latest.get((impl, "latency", "sync", "tcp", size))
        return r["p50_us"] if r else 0.0

    latency_sizes = latency_sizes_from(SIZES)
    return {
        "throughput": {impl: [get_tp(impl, size) for size in SIZES] for impl in IMPLS},
        "latency": {impl: [get_lat(impl, size) for size in latency_sizes] for impl in IMPLS},
    }


# SVG chart generation. Layout copied from bindings/pyomq/scripts/update_perf.py;
# only series names, title text, and input data keys differ.

C_OMQ = "#ef4444"
C_ZZMQ = "#60a5fa"
C_ZIMQ = "#a855f7"
CHART_SERIES = [
    ("omq.zig", C_OMQ),
    ("zzmq", C_ZZMQ),
    ("zimq", C_ZIMQ),
]


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

    tp_series = [(label, color, data["throughput"][label]) for label, color in CHART_SERIES]
    msg_max = THROUGHPUT_MSG_MAX
    gbs_values = [
        vals[i] * SIZES[i] / 1_000_000_000
        for _, _, vals in tp_series
        for i in large_indices
    ]
    gbs_max = max(1, math.ceil(max(gbs_values, default=0)))

    def y_msg(v):
        frac = v / msg_max if msg_max > 0 else 0
        return t1_bot - frac * t1_h

    def y_gbs(v):
        frac = v / gbs_max if gbs_max > 0 else 0
        return t1_bot - frac * t1_h

    lat_max = LATENCY_US_MAX
    lat_step = LATENCY_US_STEP

    def y_lat(v):
        return t2_bot - (min(v, lat_max) / lat_max) * t2_h

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

    left_ticks = list(range(THROUGHPUT_MSG_STEP, THROUGHPUT_MSG_MAX + 1, THROUGHPUT_MSG_STEP))
    right_ticks = [i / 2 for i in range(1, int(gbs_max * 2) + 1)]
    for panel_left, panel_right, panel_xs, ticks, panel_max, formatter, label_x in (
        (top_left, top_mid, small_xs, left_ticks, msg_max, _fmt_y_rate, top_left - 8),
        (
            top_right_left,
            top_right,
            large_xs,
            right_ticks,
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
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate - solid = bandwidth</text>'
    )

    # BOTTOM PANEL: LATENCY

    L.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: TCP loopback, p50 μs (lower is better)</text>"
    )

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

    L.append(f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}" stroke="#9ca3af" stroke-width="1.5"/>')
    L.append(f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}" stroke="#9ca3af" stroke-width="1.5"/>')

    for label, color in CHART_SERIES:
        vals = data["latency"][label]
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
    item_w = 120
    total_w = len(CHART_SERIES) * item_w
    start_x = mid_x - total_w / 2

    for idx, (label, color) in enumerate(CHART_SERIES):
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
        f"dashed = msg/s (left) - solid = throughput (right)</text>"
    )

    L.append("</svg>")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(L))
        f.write("\n")
    print(f"  wrote {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--impl", action="append", dest="impls", choices=list(IMPLS), help="implementation(s) to benchmark")
    parser.add_argument("--quick", action="store_true", help="run a short local-only benchmark")
    parser.add_argument("--sizes", type=parse_sizes, help="comma-separated message sizes, e.g. 8,128,1k,32k")
    parser.add_argument("--rounds", type=int, help="measured rounds per cell")
    parser.add_argument("--target-runtime", type=float, help="throughput runtime per round in seconds")
    parser.add_argument("--warmup-duration", type=float, help="throughput warmup duration per round in seconds")
    parser.add_argument("--latency-warmup-duration", type=float, help="REQ/REP latency warmup duration in seconds")
    parser.add_argument("--latency-duration", type=float, help="REQ/REP latency duration in seconds")
    parser.add_argument("--timeout", type=float, help="subprocess timeout in seconds")
    parser.add_argument("--no-save", action="store_true", help="print results without appending JSONL")
    parser.add_argument("--no-chart", action="store_true", help="skip SVG generation")
    parser.add_argument("--no-build", action="store_true", help="skip harness builds")
    parser.add_argument("--chart-only", action="store_true", help="regenerate SVG from existing JSONL")
    args = parser.parse_args()

    if args.chart_only:
        data = chart_data_from_jsonl()
        gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))
        return

    configure_benchmark(args)
    impls = args.impls or list(IMPLS)
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
        )
    )
    save_enabled = not args.no_save and not args.quick and not diagnostic_knobs

    if not args.no_build:
        build_impls(impls)

    run_id = time.strftime("%Y-%m-%dT%H:%M:%S")
    for impl in impls:
        print(f"\n{'=' * 40}")
        print(f"Benchmarking {impl}")
        print(f"{'=' * 40}")
        print(f"\n{impl} TCP PUSH/PULL throughput...")
        throughput = run_throughput(impl)
        print(f"\n{impl} TCP REQ/REP latency...")
        latency = run_latency(impl)
        if save_enabled:
            save_results(run_id, impl, throughput, latency)

    if not save_enabled:
        print("\nSkipping JSONL and chart updates.")
        return
    if not args.no_chart:
        print("\nGenerating chart...")
        data = chart_data_from_jsonl()
        gen_combined_chart(data, os.path.join(CHART_DIR, "bindings.svg"))


if __name__ == "__main__":
    main()
