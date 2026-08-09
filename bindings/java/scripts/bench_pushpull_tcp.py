#!/usr/bin/env python3
"""Measure OMQ.java vs JeroMQ PUSH/PULL throughput over TCP loopback."""

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
CLASS = "io.omq.perf.PushPullTcpPeer"
DEFAULT_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [8, 128, 1024, 4096, 32768]
DEFAULT_IMPLS = ["omq", "omq-into", "jeromq", "jeromq-into"]
CHART_DIR = ROOT / "doc" / "charts"
JSONL = (
    Path(os.environ.get("OMQ_JAVA_CACHE_DIR", Path.home() / ".cache" / "omq.java"))
    / "pushpull-tcp.jsonl"
)

C_OMQ = "#dc2626"
C_OMQ_INTO = "#f97316"
C_JEROMQ = "#2563eb"
C_JEROMQ_INTO = "#8b5cf6"


def parse_csv_ints(value):
    return [int(part) for part in value.split(",") if part]


def parse_csv_strings(value):
    return [part for part in value.split(",") if part]


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


def message_count(size, args):
    by_bytes = args.target_bytes // max(size, 1)
    return max(args.min_messages, min(args.max_messages, by_bytes))


def java_cmd(cp, impl, role, endpoint, size, messages, warmup, batch):
    return [
        "java",
        "--enable-native-access=ALL-UNNAMED",
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


def chart_data_from_jsonl(sizes):
    latest = {}
    for row in load_jsonl():
        if row.get("kind") != "pushpull_tcp":
            continue
        impl = row.get("impl")
        size = row.get("msg_size")
        if size not in sizes:
            continue
        key = (impl, size)
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row
    return {
        impl: [latest.get((impl, size), {"msgs_s": 0.0, "gb_s": 0.0}) for size in sizes]
        for impl in DEFAULT_IMPLS
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


def read_chart_hw():
    config = {}
    path = ROOT / ".chart_hw"
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
    try:
        cpu = None
        with open("/proc/cpuinfo") as file:
            for line in file:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                    break
        cores = os.cpu_count()
        if cpu and cores:
            label = f"{cpu}, {cores} cores"
            prefix = os.environ.get("OMQ_HW_PREFIX") or config.get("prefix")
            postfix = os.environ.get("OMQ_HW_POSTFIX") or config.get("postfix")
            extras = [item.strip() for item in postfix.split(",")] if postfix else []
            hw_extras = os.environ.get("OMQ_HW_EXTRAS")
            if hw_extras:
                extras.extend(hw_extras.split(","))
            extras = [item for item in (item.strip() for item in extras) if item]
            if extras:
                label += ", " + ", ".join(extras)
            if prefix:
                label = f"{prefix}, {label}"
            return label
    except OSError:
        pass
    return None


def gen_chart(sizes, path):
    data = chart_data_from_jsonl(sizes)
    series = [
        ("OMQ.java", C_OMQ, data["omq"]),
        ("OMQ.java receiveInto", C_OMQ_INTO, data["omq-into"]),
        ("JeroMQ", C_JEROMQ, data["jeromq"]),
        ("JeroMQ recvByteBuffer", C_JEROMQ_INTO, data["jeromq-into"]),
    ]
    max_msgs = max((row["msgs_s"] for rows in data.values() for row in rows), default=0.0)
    max_gbs = max((row["gb_s"] for rows in data.values() for row in rows), default=0.0)
    msg_max = nice_ceil(max_msgs)
    gbs_max = nice_ceil(max_gbs)
    hw_label = detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 500 + hw_offset
    x_left, x_right = 90, 760
    y_top = 35 + hw_offset
    y_bot = 370 + hw_offset
    plot_w = x_right - x_left
    plot_h = y_bot - y_top
    mid_x = (x_left + x_right) / 2
    xs = [x_left + i * plot_w / max(len(sizes) - 1, 1) for i in range(len(sizes))]

    def y_msg(value):
        return y_bot - (value / msg_max) * plot_h

    def y_gbs(value):
        return y_bot - (value / gbs_max) * plot_h

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>',
        f'  <text x="{mid_x}" y="{y_top - 17}" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        "PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>",
    ]
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{escape_svg(hw_label)}</text>'
        )

    for i in range(1, 11):
        frac = i / 10
        yy = y_bot - frac * plot_h
        msg_val = int(msg_max * frac)
        gbs_val = gbs_max * frac
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">'
            f"{fmt_y_rate(msg_val)}</text>"
        )
        lines.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#6b7280" font-size="10">'
            f"{fmt_y_gbs(gbs_val)}</text>"
        )

    for x in xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{y_top}" x2="{x:.1f}" y2="{y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    lines.extend(
        [
            f'  <line x1="{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_right}" y1="{y_top}" x2="{x_right}" y2="{y_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
        ]
    )

    y_mid = (y_top + y_bot) / 2
    lines.append(
        f'  <text x="40" y="{y_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{y_mid:.0f})">msg/s</text>'
    )

    for _, color, rows in series:
        points = " ".join(
            f"{xs[i]:.1f},{y_msg(row['msgs_s']):.1f}" for i, row in enumerate(rows)
        )
        lines.append(
            f'  <polyline points="{points}" fill="none" stroke="{color}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    for _, color, rows in series:
        points = " ".join(
            f"{xs[i]:.1f},{y_gbs(row['gb_s']):.1f}" for i, row in enumerate(rows)
        )
        lines.append(
            f'  <polyline points="{points}" fill="none" stroke="{color}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i, row in enumerate(rows):
            yy = y_gbs(row["gb_s"])
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="white" stroke-width="1"/>'
            )

    for i, size in enumerate(sizes):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{y_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(size)}</text>'
        )

    legend_y = y_bot + 48
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
            f'  <text x="{lx + 20:.0f}" y="{legend_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{escape_svg(label)}</text>'
        )

    footer_y = legend_y + 22
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"dashed = msg/s (left) · solid = GB/s (right)</text>"
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
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--no-chart", action="store_true")
    args = parser.parse_args()

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    chart_path = CHART_DIR / "pushpull_tcp.svg"
    if args.chart_only:
        gen_chart(sizes, chart_path)
        return

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
    if not args.no_chart:
        gen_chart(sizes, chart_path)


if __name__ == "__main__":
    main()
