#!/usr/bin/env python3
"""Measure OMQ.java vs JeroMQ TCP throughput and latency."""

import argparse
import datetime as dt
import json
import math
import os
import selectors
import socket
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
PUSHPULL_CLASS = "io.omq.perf.PushPullTcpPeer"
REQREP_CLASS = "io.omq.perf.ReqRepTcpPeer"
DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
DEFAULT_IMPLS = ["omq", "omq-into", "jeromq", "jeromq-into"]
DEFAULT_THROUGHPUT_DURATION = 2.5
QUICK_THROUGHPUT_DURATION = 0.5
DEFAULT_THROUGHPUT_WARMUP = 0.5
QUICK_THROUGHPUT_WARMUP = 0.1
DEFAULT_LATENCY_DURATION = 1.5
QUICK_LATENCY_DURATION = 0.5
DEFAULT_LATENCY_WARMUP = 0.5
QUICK_LATENCY_WARMUP = 0.1
CHART_DIR = ROOT / "doc" / "charts"
JSONL = (
    Path(os.environ.get("OMQ_JAVA_CACHE_DIR", Path.home() / ".cache" / "omq.java"))
    / "pushpull-tcp.jsonl"
)

C_OMQ = "#ef4444"
C_OMQ_INTO = "#fb923c"
C_JEROMQ = "#60a5fa"
C_JEROMQ_INTO = "#a855f7"
LATENCY_MIN_US = 0.0
LATENCY_MAX_US = 120.0


def parse_csv_ints(value):
    return [int(part) for part in value.split(",") if part]


def parse_csv_strings(value):
    return [part for part in value.split(",") if part]


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def load_jsonl():
    rows = []
    try:
        with JSONL.open() as file:
            for line in file:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


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


def java_cmd(class_name, cp, impl, role, endpoint, size, measure, warmup, batch=None):
    args = [
        str(size),
        str(measure),
        str(warmup),
    ]
    if batch is not None:
        args.append(str(batch))
    return [
        "java",
        "--enable-native-access=ALL-UNNAMED",
        "-Djava.library.path=" + str(ROOT / "native" / "target" / "release"),
        "-cp",
        cp,
        class_name,
        impl,
        role,
        endpoint,
        *args,
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
    return proc.communicate(timeout=5)


def run_cell_once(cp, impl, size, duration, warmup, batch, timeout):
    endpoint = free_endpoint()
    pull = subprocess.Popen(
        java_cmd(PUSHPULL_CLASS, cp, impl, "pull", endpoint, size, duration, warmup, batch),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    push = None
    try:
        ready = read_line_timeout(pull, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = pull.communicate(timeout=1) if pull.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        push = subprocess.Popen(
            java_cmd(PUSHPULL_CLASS, cp, impl, "push", endpoint, size, duration, warmup, batch),
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        deadline = time.monotonic() + timeout
        while pull.poll() is None:
            if push.poll() is not None:
                push_out, push_err = push.communicate(timeout=1)
                pull_out, pull_err = kill(pull)
                raise RuntimeError(
                    "sender exited before receiver:\n"
                    + push_out
                    + push_err
                    + "\nreceiver output:\n"
                    + ready
                    + pull_out
                    + pull_err
                )
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(pull.args, timeout)
            time.sleep(0.05)

        out, err = pull.communicate(timeout=1)
        out = ready + out
        push_out, push_err = kill(push)
        push = None
        fail_on_noise("sender", push_out, push_err)
        fail_on_noise("receiver", out, err)
        if pull.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        return parse_result(out)
    except Exception as exc:
        kill(pull)
        if push is not None:
            push_out, push_err = kill(push)
            if push_out or push_err:
                raise RuntimeError(f"sender output before failure:\n{push_out}{push_err}") from exc
        raise


def run_cell(cp, impl, size, args):
    runs = []
    total = args.warmup_rounds + args.rounds
    for round_index in range(total):
        result = run_cell_once(
            cp,
            impl,
            size,
            args.throughput_duration,
            args.throughput_warmup,
            args.batch_size,
            args.timeout,
        )
        result["target_seconds"] = args.throughput_duration
        result["warmup_seconds"] = args.throughput_warmup
        if round_index >= args.warmup_rounds:
            runs.append(result)
        print(
            f"  {impl:8s} size={size:6d} round={round_index + 1}/{total} "
            f"{result['seconds']:5.2f}s {result['msgs_s']:12.0f} msg/s "
            f"{result['gb_s']:7.3f} GB/s",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["msgs_s"])[len(runs) // 2]


def run_latency_cell_once(cp, impl, size, duration, warmup, timeout):
    endpoint = free_endpoint()
    rep = subprocess.Popen(
        java_cmd(REQREP_CLASS, cp, impl, "rep", endpoint, size, duration, warmup),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        ready = read_line_timeout(rep, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = rep.communicate(timeout=1) if rep.poll() is not None else ("", "")
            raise RuntimeError(f"REP did not become ready:\n{ready or ''}{out}{err}")

        req = subprocess.run(
            java_cmd(REQREP_CLASS, cp, impl, "req", endpoint, size, duration, warmup),
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        fail_on_noise("REQ", req.stdout, req.stderr)
        if req.returncode != 0:
            raise RuntimeError(f"REQ failed:\n{req.stdout}{req.stderr}")

        out, err = kill(rep)
        fail_on_noise("REP", ready + out, err)
        return parse_result(req.stdout)
    except Exception:
        kill(rep)
        raise


def run_latency_cell(cp, impl, size, args):
    runs = []
    total = args.warmup_rounds + args.rounds
    for round_index in range(total):
        result = run_latency_cell_once(
            cp,
            impl,
            size,
            args.latency_duration,
            args.latency_warmup_duration,
            args.timeout + args.latency_warmup_duration + args.latency_duration,
        )
        result["target_seconds"] = args.latency_duration
        result["warmup_seconds"] = args.latency_warmup_duration
        if round_index >= args.warmup_rounds:
            runs.append(result)
        print(
            f"  {impl:8s} size={size:6d} round={round_index + 1}/{total} "
            f"p50 {result['p50_us']:8.1f} μs p99 {result['p99_us']:8.1f} μs "
            f"n={result['iterations']}",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["p50_us"])[len(runs) // 2]


def append_jsonl(rows):
    JSONL.parent.mkdir(parents=True, exist_ok=True)
    with JSONL.open("a") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")


def latest_rows(kind, sizes, impls, fallback):
    latest = {}
    for row in load_jsonl():
        if row.get("kind") != kind:
            continue
        impl = row.get("impl")
        if impl not in impls:
            continue
        size = row.get("msg_size")
        if size not in sizes:
            continue
        key = (impl, size)
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row
    return {
        impl: [latest.get((impl, size), fallback.copy()) for size in sizes]
        for impl in impls
    }


def chart_data_from_jsonl(sizes, latency_sizes):
    return {
        "throughput": latest_rows(
            "pushpull_tcp", sizes, DEFAULT_IMPLS, {"msgs_s": 0.0, "gb_s": 0.0}
        ),
        "latency": latest_rows(
            "reqrep_tcp_latency",
            latency_sizes,
            DEFAULT_IMPLS,
            {"p50_us": 0.0, "p99_us": 0.0},
        ),
    }


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def nice_ceil(value):
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for multiplier in [1, 2, 5, 10]:
        candidate = multiplier * base
        if candidate >= value:
            return candidate
    return 10 * base


def fmt_y_rate(value):
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def fmt_y_gbs(value):
    if value >= 1:
        return f"{value:g} GB/s"
    return f"{value * 1000:g} MB/s"


def fmt_y_us(value):
    if value >= 1000:
        return f"{value / 1000:g} ms"
    return f"{value:g} μs"


def read_chart_hw():
    config = {}
    path = REPO / ".chart_hw"
    try:
        with path.open() as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if sep:
                    config[key.strip()] = value.strip()
    except OSError:
        pass
    return config


def escape_svg(value):
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def detect_hardware():
    config = read_chart_hw()
    prefix = os.environ.get("OMQ_HW_PREFIX") or config.get("prefix")
    postfix = os.environ.get("OMQ_HW_POSTFIX") or config.get("postfix")
    if prefix and postfix:
        return f"{prefix}, {postfix}"
    if prefix:
        return prefix
    if postfix:
        return postfix
    return None


def gen_chart(sizes, latency_sizes, path):
    data = chart_data_from_jsonl(sizes, latency_sizes)
    throughput = data["throughput"]
    latency = data["latency"]
    series = [
        ("OMQ.java", C_OMQ, "omq"),
        ("OMQ.java receiveInto", C_OMQ_INTO, "omq-into"),
        ("JeroMQ", C_JEROMQ, "jeromq"),
        ("JeroMQ recvByteBuffer", C_JEROMQ_INTO, "jeromq-into"),
    ]
    hw_label = detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 810 + hw_offset
    x_left, x_right = 60, 790
    top_left, top_mid, top_right = 60, 395, 790
    top_right_left = 455
    t1_top = 95 + hw_offset
    t1_bot = 430 + hw_offset
    t1_h = t1_bot - t1_top
    t2_top = t1_bot + 105
    t2_bot = t2_top + 200
    t2_h = t2_bot - t2_top
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2
    small_sizes = [size for size in sizes if size <= 1024]
    large_sizes = [size for size in sizes if size >= 256]
    small_indices = [sizes.index(size) for size in small_sizes]
    large_indices = [sizes.index(size) for size in large_sizes]
    small_xs = [
        top_left + i * (top_mid - top_left) / max(len(small_sizes) - 1, 1)
        for i in range(len(small_sizes))
    ]
    large_xs = [
        top_right_left + i * (top_right - top_right_left) / max(len(large_sizes) - 1, 1)
        for i in range(len(large_sizes))
    ]
    lat_xs = [
        x_left + i * plot_w / max(len(latency_sizes) - 1, 1)
        for i in range(len(latency_sizes))
    ]
    max_msgs = max(
        (
            rows[index]["msgs_s"]
            for rows in throughput.values()
            for index in small_indices
        ),
        default=0.0,
    )
    max_gbs = max(
        (
            rows[index]["gb_s"]
            for rows in throughput.values()
            for index in large_indices
        ),
        default=0.0,
    )
    msg_max = nice_ceil(max_msgs)
    gbs_max = max(1, math.ceil(max_gbs))

    def y_msg(value):
        return t1_bot - (value / msg_max) * t1_h

    def y_gbs(value):
        return t1_bot - (value / gbs_max) * t1_h

    def y_lat(value):
        bounded = min(max(value, LATENCY_MIN_US), LATENCY_MAX_US)
        frac = (bounded - LATENCY_MIN_US) / (LATENCY_MAX_US - LATENCY_MIN_US)
        return t2_bot - frac * t2_h

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>',
        f'  <text x="{mid_x}" y="{t1_top - 65}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        "JIT-warmed PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>",
    ]
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{t1_top - 51}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{escape_svg(hw_label)}</text>'
        )

    for panel_left, panel_right, panel_xs, ticks, panel_max, formatter, label_x in (
        (
            top_left,
            top_mid,
            small_xs,
            [msg_max * i / 10 for i in range(1, 11)],
            msg_max,
            fmt_y_rate,
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
            lines.append(
                f'  <line x1="{panel_left}" y1="{yy:.1f}" x2="{panel_right}"'
                f' y2="{yy:.1f}" stroke="#374151" stroke-width="1"/>'
            )
            anchor = "end" if label_x < panel_left else "start"
            lines.append(
                f'  <text x="{label_x}" y="{yy:.1f}" text-anchor="{anchor}"'
                f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
                f"{formatter(tick)}</text>"
            )
        for x in panel_xs:
            lines.append(
                f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}"'
                f' stroke="#374151" stroke-width="1"/>'
            )
        if panel_left == top_left:
            lines.append(
                f'  <line x1="{panel_left}" y1="{t1_top}" x2="{panel_left}"'
                f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        if panel_right == top_right:
            lines.append(
                f'  <line x1="{panel_right}" y1="{t1_top}" x2="{panel_right}"'
                f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
            )
        lines.append(
            f'  <line x1="{panel_left}" y1="{t1_bot}" x2="{panel_right}"'
            f' y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>'
        )

    lines.append(
        f'  <text x="{(top_left + top_mid) / 2:.1f}" y="{t1_top - 17}"'
        f' text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">'
        "small messages</text>"
    )
    lines.append(
        f'  <text x="{(top_right_left + top_right) / 2:.1f}" y="{t1_top - 17}"'
        f' text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">'
        "medium/large messages</text>"
    )

    for _, color, impl in series:
        rows = throughput[impl]
        points = " ".join(
            f"{small_xs[j]:.1f},{y_msg(rows[index]['msgs_s']):.1f}"
            for j, index in enumerate(small_indices)
        )
        lines.append(
            f'  <polyline points="{points}" fill="none" stroke="{color}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    for _, color, impl in series:
        rows = throughput[impl]
        points = " ".join(
            f"{large_xs[j]:.1f},{y_gbs(rows[index]['gb_s']):.1f}"
            for j, index in enumerate(large_indices)
        )
        lines.append(
            f'  <polyline points="{points}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for j, index in enumerate(large_indices):
            yy = y_gbs(rows[index]["gb_s"])
            lines.append(
                f'  <circle cx="{large_xs[j]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )

    for i, size in enumerate(small_sizes):
        lines.append(
            f'  <text x="{small_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    for i, size in enumerate(large_sizes):
        lines.append(
            f'  <text x="{large_xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">dashed = message rate · solid = bandwidth</text>'
    )

    lines.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        "JIT-warmed REQ/REP latency: 2-process, TCP loopback, p50 μs (lower is better)</text>"
    )

    for value in range(int(LATENCY_MIN_US), int(LATENCY_MAX_US) + 1, 20):
        yy = y_lat(value)
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
            f"{fmt_y_us(value)}</text>"
        )

    for x in lat_xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#374151" stroke-width="1"/>'
        )

    lines.extend(
        [
            f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
        ]
    )

    for _, color, impl in series:
        rows = latency[impl]
        points = " ".join(
            f"{lat_xs[i]:.1f},{y_lat(row['p50_us']):.1f}"
            for i, row in enumerate(rows)
        )
        lines.append(
            f'  <polyline points="{points}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, row in enumerate(rows):
            yy = y_lat(row["p50_us"])
            lines.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )

    for i, size in enumerate(latency_sizes):
        lines.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )

    legend_y = t2_bot + 40
    legend_items = [(label, color) for label, color, _ in series]
    item_w = 180
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2
    for idx, (label, color) in enumerate(legend_items):
        lx = start_x + idx * item_w
        lines.append(
            f'  <line x1="{lx:.0f}" y1="{legend_y}" x2="{lx + 14:.0f}"'
            f' y2="{legend_y}" stroke="{color}" stroke-width="2.5"/>'
        )
        lines.append(f'  <circle cx="{lx + 7:.0f}" cy="{legend_y}" r="2.5" fill="{color}"/>')
        lines.append(
            f'  <text x="{lx + 20:.0f}" y="{legend_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{escape_svg(label)}</text>'
        )

    footer_y = legend_y + 22
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"top dashed = msg/s (left) · top solid = GB/s (right) · bottom solid = p50 latency</text>"
    )
    lines.append("</svg>")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    print(f"wrote {path}")


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


def print_latency_table(rows, sizes, impls):
    by_key = {(row["msg_size"], row["impl"]): row for row in rows}
    print()
    print("size  impl          p50 μs    p99 μs   jeromq/impl")
    for size in sizes:
        base = by_key.get((size, "jeromq"))
        base_p50 = base["p50_us"] if base else 0.0
        for impl in impls:
            row = by_key[(size, impl)]
            ratio = base_p50 / row["p50_us"] if base_p50 and row["p50_us"] else 0.0
            print(
                f"{size:5d} {impl:8s} {row['p50_us']:9.1f} "
                f"{row['p99_us']:9.1f} {ratio:11.2f}x"
            )
        print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes", type=parse_csv_ints)
    parser.add_argument("--impls", type=parse_csv_strings, default=DEFAULT_IMPLS)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=0)
    parser.add_argument("--throughput-duration", type=float)
    parser.add_argument(
        "--throughput-warmup",
        dest="throughput_warmup",
        type=float,
    )
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--latency-warmup-duration",
        dest="latency_warmup_duration",
        type=float,
    )
    parser.add_argument(
        "--latency-duration",
        dest="latency_duration",
        type=float,
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--no-chart", action="store_true")
    parser.add_argument("--throughput-only", action="store_true")
    parser.add_argument("--latency-only", action="store_true")
    args = parser.parse_args()

    if args.throughput_only and args.latency_only:
        parser.error("--throughput-only and --latency-only are mutually exclusive")
    if args.chart_only and (args.throughput_only or args.latency_only):
        parser.error("--chart-only cannot be combined with benchmark selection")
    if args.quick:
        args.rounds = min(args.rounds, 1)
        args.warmup_rounds = 0
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.warmup_rounds < 0:
        parser.error("--warmup-rounds cannot be negative")
    if args.throughput_duration is None:
        args.throughput_duration = (
            QUICK_THROUGHPUT_DURATION if args.quick else DEFAULT_THROUGHPUT_DURATION
        )
    if args.throughput_warmup is None:
        args.throughput_warmup = (
            QUICK_THROUGHPUT_WARMUP if args.quick else DEFAULT_THROUGHPUT_WARMUP
        )
    if args.throughput_duration <= 0:
        parser.error("--throughput-duration must be greater than zero")
    if args.throughput_warmup < 0:
        parser.error("--throughput-warmup cannot be negative")
    if args.latency_warmup_duration is None:
        args.latency_warmup_duration = (
            QUICK_LATENCY_WARMUP if args.quick else DEFAULT_LATENCY_WARMUP
        )
    if args.latency_duration is None:
        args.latency_duration = QUICK_LATENCY_DURATION if args.quick else DEFAULT_LATENCY_DURATION
    if args.latency_warmup_duration < 0:
        parser.error("--latency-warmup-duration cannot be negative")
    if args.latency_duration <= 0:
        parser.error("--latency-duration must be greater than zero")

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    latency_sizes = latency_sizes_from(sizes)
    chart_path = CHART_DIR / "bindings.svg"
    if args.chart_only:
        gen_chart(sizes, latency_sizes, chart_path)
        return

    build(args)
    cp = classpath()
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rows = []
    throughput_rows = []
    latency_rows = []

    print(f"run_id={run_id}")
    if not args.latency_only:
        print("\nPUSH/PULL throughput (TCP)...")
        for size in sizes:
            for impl in args.impls:
                row = run_cell(cp, impl, size, args)
                row["run_id"] = run_id
                row["kind"] = "pushpull_tcp"
                throughput_rows.append(row)
                rows.append(row)

    if not args.throughput_only:
        print("\nREQ/REP latency (TCP)...")
        for size in latency_sizes:
            for impl in args.impls:
                row = run_latency_cell(cp, impl, size, args)
                row["run_id"] = run_id
                row["kind"] = "reqrep_tcp_latency"
                latency_rows.append(row)
                rows.append(row)

    append_jsonl(rows)
    if throughput_rows:
        print_table(throughput_rows, sizes, args.impls)
    if latency_rows:
        print_latency_table(latency_rows, latency_sizes, args.impls)
    print(f"appended {len(rows)} rows to {JSONL}")
    if not args.no_chart:
        gen_chart(sizes, latency_sizes, chart_path)


if __name__ == "__main__":
    main()
