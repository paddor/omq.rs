#!/usr/bin/env python3
"""Measure OMQ.cr vs zeromq-crystal over 2-process TCP loopback.

Benchmark rows are append-only in ~/.cache/omq.cr/bindings.jsonl by default.
The chart is regenerated from the latest cached row per implementation, kind,
and size.
"""

import argparse
import datetime as dt
import html
import json
import math
import os
import selectors
import socket
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
CACHE_DIR = Path(
    os.environ.get(
        "OMQ_CRYSTAL_CACHE_DIR",
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq.cr",
    )
)
JSONL = CACHE_DIR / "bindings.jsonl"
PEERS = {
    "omq.cr": CACHE_DIR / "omq-crystal-bench-peer",
    "zeromq-crystal": CACHE_DIR / "zeromq-crystal-bench-peer",
}
CHART_DIR = ROOT / "doc" / "charts"
CHART = CHART_DIR / "bindings.svg"

DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
THROUGHPUT_MSG_MAX = 6_000_000
DEFAULT_IMPLS = ["omq.cr", "zeromq-crystal"]
DEFAULT_LATENCY_IMPLS = ["omq.cr", "zeromq-crystal"]
IMPL_LABELS = {
    "omq.cr": "OMQ.cr",
    "zeromq-crystal": "zeromq-crystal",
}
COLORS = {
    "omq.cr": "#ef4444",
    "zeromq-crystal": "#60a5fa",
}


def parse_csv_ints(value):
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
        size = int(raw) * multiplier
        if size <= 0:
            raise argparse.ArgumentTypeError("sizes must be positive")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one size is required")
    return sizes


def parse_csv_strings(value):
    return [part.strip() for part in value.split(",") if part.strip()]


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def dylib_env(profile):
    lib_dir = REPO / "target" / profile
    env = os.environ.copy()
    env["LIBRARY_PATH"] = f"{lib_dir}{os.pathsep}{env.get('LIBRARY_PATH', '')}"
    env["CRYSTAL_LIBRARY_PATH"] = (
        f"{lib_dir}{os.pathsep}{env.get('CRYSTAL_LIBRARY_PATH', '')}"
    )
    if sys.platform == "darwin":
        key = "DYLD_LIBRARY_PATH"
    elif os.name == "nt":
        key = "PATH"
    else:
        key = "LD_LIBRARY_PATH"
    env[key] = f"{lib_dir}{os.pathsep}{env.get(key, '')}"
    return env


def has_noise(text):
    lowered = (text or "").lower()
    return "warning" in lowered or "timeout" in lowered


def run(cmd, cwd=REPO, env=None, timeout=None, fail_on_warning=True):
    print("+ " + " ".join(str(part) for part in cmd), flush=True)
    result = subprocess.run(
        [str(part) for part in cmd],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    if fail_on_warning and has_noise(result.stdout):
        raise RuntimeError("command printed warning/timeout")


def active_impls(args):
    ordered = []
    for impl in [*args.impls, *args.latency_impls]:
        if impl not in ordered:
            ordered.append(impl)
    return ordered


def validate_impls(impls):
    unknown = [impl for impl in impls if impl not in PEERS]
    if unknown:
        raise SystemExit(f"unknown implementation(s): {', '.join(unknown)}")


def build(args, profile, impls):
    if args.no_build:
        return

    for path in PEERS.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    if "omq.cr" in impls:
        cargo_cmd = ["cargo", "build", "-p", "omq-libzmq"]
        if profile == "release":
            cargo_cmd.append("--release")
        run(cargo_cmd)

        lib_dir = REPO / "target" / profile
        link_flags = f"-L{lib_dir} -Wl,-rpath,{lib_dir}"
        run(
            [
                args.crystal,
                "build",
                ROOT / "scripts" / "bench_peer.cr",
                "-o",
                PEERS["omq.cr"],
                "--link-flags",
                link_flags,
                *([] if args.debug else ["--release"]),
            ],
            env=dylib_env(profile),
        )

    if "zeromq-crystal" in impls:
        run([args.shards, "install"], cwd=ROOT)
        run(
            [
                args.crystal,
                "build",
                ROOT / "scripts" / "bench_peer_zeromq.cr",
                "-o",
                PEERS["zeromq-crystal"],
                *([] if args.debug else ["--release"]),
            ],
            cwd=ROOT,
        )


def free_endpoint():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    finally:
        sock.close()
    return f"tcp://127.0.0.1:{port}"


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


def peer_env(profile, impl):
    if impl == "omq.cr":
        return dylib_env(profile)
    return os.environ.copy()


def peer_cmd(impl, bench, role, endpoint, size, duration, warmup):
    return [
        str(PEERS[impl]),
        bench,
        impl,
        role,
        endpoint,
        str(size),
        f"{duration:.6f}",
        f"{warmup:.6f}",
    ]


def parse_result(output):
    for line in output.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT ") :])
    raise RuntimeError("missing RESULT line:\n" + output)


def fail_on_noise(name, stdout, stderr):
    text = (stdout or "") + "\n" + (stderr or "")
    if has_noise(text):
        raise RuntimeError(f"{name} printed warning/timeout:\n{text}")


def kill(proc):
    if proc.poll() is not None:
        return proc.communicate(timeout=5)
    proc.kill()
    return proc.communicate(timeout=5)


def run_throughput_pair_once(impl, size, duration, warmup, timeout, profile):
    endpoint = free_endpoint()
    env = peer_env(profile, impl)
    receiver = subprocess.Popen(
        peer_cmd(impl, "pushpull", "pull", endpoint, size, duration, warmup),
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    sender = None
    ready = ""
    try:
        line = read_line_timeout(receiver, 5)
        if line is None:
            out, err = kill(receiver)
            raise RuntimeError(f"receiver did not become ready:\n{out}{err}")
        ready += line
        if not line.startswith("READY "):
            out, err = kill(receiver)
            raise RuntimeError(f"bad receiver ready line: {line!r}\n{out}{err}")
        endpoint = line[len("READY ") :].strip()

        sender = subprocess.Popen(
            peer_cmd(impl, "pushpull", "push", endpoint, size, duration, warmup),
            cwd=REPO,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = receiver.communicate(timeout=timeout)
        out = ready + out
        fail_on_noise("receiver", out, err)
        if receiver.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        sender_out, sender_err = kill(sender)
        fail_on_noise("sender", sender_out, sender_err)
        return parse_result(out)
    except Exception:
        kill(receiver)
        if sender is not None:
            kill(sender)
        raise


def run_pair_once(
    impl, bench, receiver_role, sender_role, size, duration, warmup, timeout, profile
):
    endpoint = free_endpoint()
    env = peer_env(profile, impl)
    receiver = subprocess.Popen(
        peer_cmd(impl, bench, receiver_role, endpoint, size, duration, warmup),
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    ready = ""
    try:
        line = read_line_timeout(receiver, 5)
        if line is None:
            out, err = kill(receiver)
            raise RuntimeError(f"receiver did not become ready:\n{out}{err}")
        ready += line
        if not line.startswith("READY "):
            out, err = kill(receiver)
            raise RuntimeError(f"bad receiver ready line: {line!r}\n{out}{err}")
        endpoint = line[len("READY ") :].strip()
        sender = subprocess.run(
            peer_cmd(impl, bench, sender_role, endpoint, size, duration, warmup),
            cwd=REPO,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        fail_on_noise("sender", sender.stdout, sender.stderr)
        if sender.returncode != 0:
            raise RuntimeError(f"sender failed:\n{sender.stdout}{sender.stderr}")

        if bench == "reqrep":
            out, err = kill(receiver)
            fail_on_noise("receiver", ready + out, err)
            return parse_result(sender.stdout)

        out, err = receiver.communicate(timeout=timeout)
        out = ready + out
        fail_on_noise("receiver", out, err)
        if receiver.returncode != 0:
            raise RuntimeError(f"receiver failed:\n{out}{err}")
        return parse_result(out)
    except Exception:
        kill(receiver)
        raise


def run_throughput_cell(impl, size, args, profile):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.warmup_duration + args.duration
    for round_index in range(total):
        row = run_throughput_pair_once(
            impl,
            size,
            args.duration,
            args.warmup_duration,
            timeout,
            profile,
        )
        if round_index >= args.warmup_rounds:
            runs.append(row)
        print(
            f"  {impl:15s} size={size:6d} round={round_index + 1}/{total} "
            f"{row['msgs_s']:12.0f} msg/s {row['gb_s']:7.3f} GB/s "
            f"n={row['messages']} t={row['seconds']:.3f}s",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["msgs_s"])[len(runs) // 2]


def run_latency_cell(impl, size, args, profile):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.latency_warmup_duration + args.latency_duration
    for round_index in range(total):
        row = run_pair_once(
            impl,
            "reqrep",
            "rep",
            "req",
            size,
            args.latency_duration,
            args.latency_warmup_duration,
            timeout,
            profile,
        )
        if round_index >= args.warmup_rounds:
            runs.append(row)
        print(
            f"  {impl:15s} size={size:6d} round={round_index + 1}/{total} "
            f"p50 {row['p50_us']:8.1f} us p99 {row['p99_us']:8.1f} us "
            f"n={row['messages']}",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["p50_us"])[len(runs) // 2]


def append_jsonl(rows):
    JSONL.parent.mkdir(parents=True, exist_ok=True)
    with JSONL.open("a") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")


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


def latest_chart_data(sizes, latency_sizes, impls, latency_impls):
    latest = {}
    for row in load_jsonl():
        key = (row.get("impl"), row.get("kind"), row.get("msg_size"))
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row
    throughput = {
        impl: [
            latest.get((impl, "pushpull_tcp", size), {}).get("msgs_s", 0.0)
            for size in sizes
        ]
        for impl in impls
    }
    latency = {
        impl: [
            latest.get((impl, "reqrep_tcp_latency", size), {}).get("p50_us", 0.0)
            for size in latency_sizes
        ]
        for impl in latency_impls
    }
    return {"throughput": throughput, "latency": latency}


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


def fmt_y_mbps(value):
    if value >= 1000:
        return f"{value / 1000:g} GB/s"
    if value >= 10:
        return f"{value:.0f} MB/s"
    return f"{value:.1f} MB/s"


def fmt_y_us(value):
    if value >= 1000:
        return f"{value / 1000:g} ms"
    return f"{value:g} us"


def read_chart_hw():
    config = {}
    try:
        with (REPO / ".chart_hw").open() as file:
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


def detect_hardware():
    config = read_chart_hw()
    label = os.environ.get("OMQ_HW_LABEL") or config.get("label")
    if label:
        return label
    prefix = os.environ.get("OMQ_HW_PREFIX") or config.get("prefix")
    postfix = os.environ.get("OMQ_HW_POSTFIX") or config.get("postfix")
    if prefix and postfix:
        return f"{prefix}, {postfix}"
    return prefix or postfix


def svg_line(points, color, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (
        f'  <polyline points="{points}" fill="none" stroke="{color}"'
        f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"{dash}/>'
    )


def add_legend(lines, mid_x, leg_y, impls):
    item_w = 165
    start_x = mid_x - (item_w * len(impls)) / 2
    for index, impl in enumerate(impls):
        x = start_x + index * item_w
        color = COLORS[impl]
        label = html.escape(IMPL_LABELS[impl])
        lines.append(
            f'  <line x1="{x:.0f}" y1="{leg_y}" x2="{x + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        lines.append(f'  <circle cx="{x + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>')
        lines.append(
            f'  <text x="{x + 22:.0f}" y="{leg_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{label}</text>'
        )


def gen_chart(data, path, sizes, latency_sizes, impls, latency_impls):
    hw_label = detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 670 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    t1_top = 35 + hw_offset
    t1_bot = 370 + hw_offset
    t1_h = t1_bot - t1_top
    t1_leg_y = t1_bot + 40
    t2_top = t1_bot + 105
    t2_bot = t2_top + 120
    t2_h = t2_bot - t2_top
    mid_x = (x_left + x_right) / 2
    xs = [x_left + i * plot_w / max(len(sizes) - 1, 1) for i in range(len(sizes))]
    lat_xs = [
        x_left + i * plot_w / max(len(latency_sizes) - 1, 1)
        for i in range(len(latency_sizes))
    ]

    mbps = {
        impl: [
            rate * sizes[index] / 1_000_000.0
            for index, rate in enumerate(data["throughput"][impl])
        ]
        for impl in impls
    }
    msg_max = THROUGHPUT_MSG_MAX
    all_mbps = [value for values in mbps.values() for value in values]
    all_latency = [value for values in data["latency"].values() for value in values]
    mbps_max = max(5_000, nice_ceil(max(all_mbps or [0]) * 1.05))
    lat_max = max(200, nice_ceil(max(all_latency or [0]) * 1.05))

    def y_msg(value):
        return t1_bot - (value / msg_max) * t1_h

    def y_mbps(value):
        return t1_bot - (value / mbps_max) * t1_h

    def y_lat(value):
        return t2_bot - (min(value, lat_max) / lat_max) * t2_h

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">',
        f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>',
        f'  <text x="{mid_x}" y="{t1_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>",
    ]
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{t1_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{html.escape(hw_label)}</text>'
        )

    for i in range(1, 11):
        frac = i / 10
        yy = t1_bot - frac * t1_h
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
            f"{fmt_y_rate(msg_max * frac)}</text>"
        )
        lines.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#9ca3af" font-size="10">'
            f"{fmt_y_mbps(mbps_max * frac)}</text>"
        )
    for x in xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
    lines.extend(
        [
            f'  <line x1="{x_left}" y1="{t1_top}" x2="{x_left}" y2="{t1_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_right}" y1="{t1_top}" x2="{x_right}" y2="{t1_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
            f'  <line x1="{x_left}" y1="{t1_bot}" x2="{x_right}" y2="{t1_bot}"'
            f' stroke="#9ca3af" stroke-width="1.5"/>',
        ]
    )
    t1_mid = (t1_top + t1_bot) / 2
    lines.append(
        f'  <text x="40" y="{t1_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#e5e7eb" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t1_mid:.0f})">msg/s</text>'
    )

    for impl in impls:
        color = COLORS[impl]
        points = " ".join(
            f"{xs[i]:.1f},{y_msg(value):.1f}"
            for i, value in enumerate(data["throughput"][impl])
        )
        lines.append(svg_line(points, color, dashed=True))
        points = " ".join(f"{xs[i]:.1f},{y_mbps(value):.1f}" for i, value in enumerate(mbps[impl]))
        lines.append(svg_line(points, color))
        for i, value in enumerate(mbps[impl]):
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{y_mbps(value):.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )
    for i, size in enumerate(sizes):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    add_legend(lines, mid_x, t1_leg_y, impls)
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{t1_leg_y + 18}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">dashed = msg/s (left), solid = throughput (right)</text>'
    )

    lines.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 us (lower is better)</text>"
    )
    for i in range(1, 11):
        value = lat_max * i / 10
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
    t2_mid = (t2_top + t2_bot) / 2
    lines.append(
        f'  <text x="40" y="{t2_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#e5e7eb" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t2_mid:.0f})">p50 latency (us)</text>'
    )
    for impl in latency_impls:
        color = COLORS[impl]
        points = " ".join(
            f"{lat_xs[i]:.1f},{y_lat(value):.1f}"
            for i, value in enumerate(data["latency"][impl])
        )
        lines.append(svg_line(points, color))
        for i, value in enumerate(data["latency"][impl]):
            lines.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{y_lat(value):.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )
    for i, size in enumerate(latency_sizes):
        lines.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )
    add_legend(lines, mid_x, t2_bot + 40, latency_impls)
    lines.append("</svg>")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    print(f"wrote {path}")


def print_table(rows, sizes, latency_sizes, impls, latency_impls):
    by_key = {(row["impl"], row["kind"], row["msg_size"]): row for row in rows}
    print()
    print("PUSH/PULL TCP throughput")
    print("impl            size      msg/s      GB/s")
    for impl in impls:
        for size in sizes:
            row = by_key.get((impl, "pushpull_tcp", size))
            if row:
                print(f"{impl:15s} {size:6d} {row['msgs_s']:11.0f} {row['gb_s']:8.3f}")
    print()
    print("REQ/REP TCP latency")
    print("impl            size      p50 us    p99 us")
    for impl in latency_impls:
        for size in latency_sizes:
            row = by_key.get((impl, "reqrep_tcp_latency", size))
            if row:
                print(f"{impl:15s} {size:6d} {row['p50_us']:9.1f} {row['p99_us']:9.1f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes", type=parse_csv_ints)
    parser.add_argument("--impls", type=parse_csv_strings, default=DEFAULT_IMPLS)
    parser.add_argument("--latency-impls", type=parse_csv_strings)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=0)
    parser.add_argument("--duration", type=float, default=2.5)
    parser.add_argument("--warmup-duration", type=float, default=0.5)
    parser.add_argument("--latency-duration", type=float, default=1.5)
    parser.add_argument("--latency-warmup-duration", type=float)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--throughput-only", action="store_true")
    parser.add_argument("--latency-only", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--crystal", default=os.environ.get("OMQ_CRYSTAL", "crystal"))
    parser.add_argument("--shards", default=os.environ.get("OMQ_SHARDS", "shards"))
    args = parser.parse_args()

    if args.throughput_only and args.latency_only:
        parser.error("--throughput-only and --latency-only are mutually exclusive")
    if args.quick:
        args.rounds = min(args.rounds, 1)
        args.warmup_rounds = 0
        args.duration = min(args.duration, 0.5)
        args.latency_duration = min(args.latency_duration, 0.5)
        args.warmup_duration = min(args.warmup_duration, 0.1)
    if args.latency_warmup_duration is None:
        args.latency_warmup_duration = args.warmup_duration
    if args.latency_impls is None:
        args.latency_impls = list(args.impls)
    validate_impls(args.impls)
    validate_impls(args.latency_impls)
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.warmup_rounds < 0:
        parser.error("--warmup-rounds cannot be negative")
    if args.duration <= 0 or args.latency_duration <= 0:
        parser.error("durations must be positive")
    if args.warmup_duration < 0 or args.latency_warmup_duration < 0:
        parser.error("warmup durations cannot be negative")

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    latency_sizes = latency_sizes_from(sizes)
    profile = "debug" if args.debug else "release"
    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rows = []

    if not args.chart_only:
        build(args, profile, active_impls(args))
        if not args.latency_only:
            print("PUSH/PULL TCP throughput")
            for impl in args.impls:
                for size in sizes:
                    row = run_throughput_cell(impl, size, args, profile)
                    row.update(
                        {
                            "kind": "pushpull_tcp",
                            "transport": "tcp",
                            "run_id": run_id,
                            "target_seconds": args.duration,
                            "warmup_seconds": args.warmup_duration,
                        }
                    )
                    rows.append(row)
        if not args.throughput_only:
            print("REQ/REP TCP latency")
            for impl in args.latency_impls:
                for size in latency_sizes:
                    row = run_latency_cell(impl, size, args, profile)
                    row.update(
                        {
                            "kind": "reqrep_tcp_latency",
                            "transport": "tcp",
                            "run_id": run_id,
                            "target_seconds": args.latency_duration,
                            "warmup_seconds": args.latency_warmup_duration,
                        }
                    )
                    rows.append(row)
        append_jsonl(rows)
        print(f"appended {len(rows)} rows to {JSONL}")

    chart_sizes = DEFAULT_SIZES if (args.throughput_only or args.latency_only) else sizes
    chart_latency_sizes = latency_sizes_from(chart_sizes)
    data = latest_chart_data(
        chart_sizes, chart_latency_sizes, args.impls, args.latency_impls
    )
    gen_chart(data, CHART, chart_sizes, chart_latency_sizes, args.impls, args.latency_impls)
    latest_rows = [
        *(
            r
            for r in load_jsonl()
            if r.get("impl") in args.impls
            and r.get("kind") == "pushpull_tcp"
            and r.get("msg_size") in chart_sizes
        ),
        *(
            r
            for r in load_jsonl()
            if r.get("impl") in args.latency_impls
            and r.get("kind") == "reqrep_tcp_latency"
            and r.get("msg_size") in chart_latency_sizes
        ),
    ]
    print_table(latest_rows, chart_sizes, chart_latency_sizes, args.impls, args.latency_impls)


if __name__ == "__main__":
    main()
