#!/usr/bin/env python3
"""Measure OMQ.Net vs NetMQ throughput and latency (sync + async).

Run from the repository root after building the .NET benchmark peer.
Full runs append to doc/charts/bindings.jsonl (latest run_id wins per impl).
They also generate doc/charts/bindings.svg and update the README proxy table.
"""

import argparse
import json
import math
import os
import socket
import subprocess
import time

DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
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
    "omq.net",
)
JSONL_FILE = os.path.join(_CACHE_DIR, "bindings.jsonl")

C_OMQ = "#ef4444"
C_OMQ_ASYNC = "#fb923c"
C_NETMQ = "#60a5fa"
C_NETMQ_ASYNC = "#a855f7"


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


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


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
    small_indices = [SIZES.index(s) for s in small_sizes]
    large_indices = [SIZES.index(s) for s in large_sizes]
    msg_max = 3_200_000
    gbs_values = [
        v * SIZES[i] / 1_000_000_000
        for values in tp_values
        for i in large_indices
        for v in [values[i]]
    ]
    gbs_max = max(1, math.ceil(max(gbs_values, default=0)))

    def y_msg(v):
        frac = v / msg_max if msg_max > 0 else 0
        return t1_bot - frac * t1_h

    def y_gbs(v):
        frac = v / gbs_max if gbs_max > 0 else 0
        return t1_bot - frac * t1_h

    lat_min = 0.0
    lat_max = 350.0
    lat_step = 50

    def y_lat(v):
        frac = (v - lat_min) / (lat_max - lat_min)
        return t2_bot - frac * t2_h

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>')

    # ── TOP PANEL: THROUGHPUT ──────────────────────────────────────

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

    for panel_left, panel_right, panel_sizes, panel_xs, ticks, formatter, label_x in (
        (
            top_left,
            top_mid,
            small_sizes,
            small_xs,
            list(range(400_000, int(msg_max), 400_000)) + [msg_max],
            _fmt_y_rate,
            top_left - 8,
        ),
        (
            top_right_left,
            top_right,
            large_sizes,
            large_xs,
            [i / 2 for i in range(1, int(gbs_max * 2) + 1)],
            lambda v: f"{v:g} GB/s",
            top_right + 8,
        ),
    ):
        for tick in ticks:
            panel_max = msg_max if panel_left == top_left else gbs_max
            frac = tick / panel_max
            yy = t1_bot - frac * t1_h
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
        ("OMQ.Net", C_OMQ, sync_omq_tp),
        ("OMQ.Net async", C_OMQ_ASYNC, async_omq_tp),
        ("NetMQ", C_NETMQ, sync_pz_tp),
        ("NetMQ async", C_NETMQ_ASYNC, async_pz_tp),
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
            f'  <text x="{small_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle" fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    for i, size in enumerate(large_sizes):
        L.append(
            f'  <text x="{large_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle" fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    L.append(
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate · solid = bandwidth</text>'
    )

    # ── BOTTOM PANEL: LATENCY ─────────────────────────────────────

    L.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 μs (lower is better)</text>"
    )

    sync_omq_lat = data["sync_omq_lat"]
    sync_pz_lat = data["sync_pz_lat"]
    async_omq_lat = data["async_omq_lat"]
    async_pz_lat = data["async_pz_lat"]

    for v in list(range(int(lat_min), int(lat_max), int(lat_step))) + [int(lat_max)]:
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
        ("OMQ.Net", C_OMQ, sync_omq_lat),
        ("OMQ.Net async", C_OMQ_ASYNC, async_omq_lat),
        ("NetMQ", C_NETMQ, sync_pz_lat),
        ("NetMQ async", C_NETMQ_ASYNC, async_pz_lat),
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

    # ── LEGEND ────────────────────────────────────────────────────

    leg_y = t2_bot + 40
    legend_items = [
        ("OMQ.Net", C_OMQ),
        ("OMQ.Net async", C_OMQ_ASYNC),
        ("NetMQ", C_NETMQ),
        ("NetMQ async", C_NETMQ_ASYNC),
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
        f"solid = p50 latency</text>"
    )
    L.append("</svg>")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(L))
        f.write("\n")
    print(f"  wrote {path}")


def chart_data_from_jsonl():
    latest = {}
    for row in load_jsonl():
        key = (row.get("impl"), row.get("pattern"), row.get("size"))
        latest[key] = row

    def tp(impl, size):
        return latest.get((impl, "pushpull", size), {}).get("messages_per_second", 0.0)

    def lat(impl, size):
        return latest.get((impl, "reqrep", size), {}).get("p50_us", 0.0)

    return {
        "sync_omq_tp": [tp("omq", s) for s in SIZES],
        "sync_pz_tp": [tp("netmq", s) for s in SIZES],
        "async_omq_tp": [tp("omq-async", s) for s in SIZES],
        "async_pz_tp": [tp("netmq-async", s) for s in SIZES],
        "sync_omq_lat": [lat("omq", s) for s in latency_sizes_from(SIZES)],
        "sync_pz_lat": [lat("netmq", s) for s in latency_sizes_from(SIZES)],
        "async_omq_lat": [lat("omq-async", s) for s in latency_sizes_from(SIZES)],
        "async_pz_lat": [lat("netmq-async", s) for s in latency_sizes_from(SIZES)],
    }


def _peer(impl, role, pattern, endpoint, size, duration, warmup):
    project = os.path.join(
        os.path.dirname(__file__), "..", "bench", "Omq.Net.Bench.csproj"
    )
    return [
        "dotnet",
        "run",
        "--no-build",
        "--configuration",
        "Release",
        "--project",
        project,
        "--",
        pattern,
        impl,
        role,
        endpoint,
        str(size),
        str(duration),
        str(warmup),
    ]


def _read_ready(stream, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = stream.readline()
        if line:
            return line.strip()
        time.sleep(0.01)
    raise RuntimeError("benchmark peer not ready")


def run_cell(impl, pattern, size, duration, warmup):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        endpoint = f"tcp://127.0.0.1:{sock.getsockname()[1]}"
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = (
        f"{REPO_ROOT}/target/release:{env.get('LD_LIBRARY_PATH', '')}"
    )
    server_role, client_role = (
        ("pull", "push") if pattern == "pushpull" else ("rep", "req")
    )
    server = subprocess.Popen(
        _peer(impl, server_role, pattern, endpoint, size, duration, warmup),
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    client = None
    try:
        _read_ready(server.stdout)
        client = subprocess.Popen(
            _peer(impl, client_role, pattern, endpoint, size, duration, warmup),
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _read_ready(client.stdout)
        source = client.stdout if pattern == "reqrep" else server.stdout
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            line = source.readline()
            if line.startswith("RESULT "):
                return json.loads(line[7:])
            if not line:
                time.sleep(0.01)
        raise RuntimeError("benchmark result timeout")
    finally:
        for child in (client, server):
            if child and child.poll() is None:
                child.terminate()
                try:
                    child.wait(2)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()


def append_row(row):
    os.makedirs(os.path.dirname(JSONL_FILE), exist_ok=True)
    row["run_id"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open(JSONL_FILE, "a") as stream:
        stream.write(json.dumps(row) + "\n")


def main():
    global SIZES
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--sizes")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--impl", action="append", choices=["omq", "omq-async", "netmq", "netmq-async"]
    )
    args = parser.parse_args()
    SIZES = (
        [int(x) for x in args.sizes.split(",")]
        if args.sizes
        else ([128, 1024, 4096] if args.quick else DEFAULT_SIZES)
    )
    if args.chart_only:
        gen_combined_chart(
            chart_data_from_jsonl(), os.path.join(CHART_DIR, "bindings.svg")
        )
        return
    if not args.no_build:
        subprocess.run(
            [
                "dotnet",
                "build",
                "--configuration",
                "Release",
                "bindings/dotnet/bench/Omq.Net.Bench.csproj",
            ],
            cwd=REPO_ROOT,
            check=True,
        )
    rounds = 1 if args.quick else args.rounds
    duration = 0.5 if args.quick else TARGET_RUNTIME_S
    warmup = 0.1 if args.quick else THROUGHPUT_WARMUP_S
    for impl in args.impl or ["omq", "omq-async", "netmq", "netmq-async"]:
        for pattern in ("pushpull", "reqrep"):
            for size in SIZES:
                if pattern == "reqrep" and size > LATENCY_MAX_SIZE:
                    continue
                d, w = (
                    (0.5, 0.1)
                    if args.quick and pattern == "reqrep"
                    else (LATENCY_RUNTIME_S, LATENCY_WARMUP_S)
                    if pattern == "reqrep"
                    else (duration, warmup)
                )
                rows = [run_cell(impl, pattern, size, d, w) for _ in range(rounds)]
                rows.sort(
                    key=lambda r: (
                        r.get("p50_us", 0)
                        if pattern == "reqrep"
                        else r.get("messages_per_second", 0)
                    )
                )
                median = rows[len(rows) // 2]
                if not args.quick:
                    append_row(median)
                print(impl, pattern, size, median, flush=True)
    if not args.quick:
        gen_combined_chart(
            chart_data_from_jsonl(), os.path.join(CHART_DIR, "bindings.svg")
        )


if __name__ == "__main__":
    main()
