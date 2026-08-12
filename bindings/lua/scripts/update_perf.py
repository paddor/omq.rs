#!/usr/bin/env python3
"""Measure OMQ.lua vs lzmq over 2-process TCP loopback.

Adapted from bindings/go/scripts/update_perf.py. Benchmark rows are
append-only in ~/.cache/omq.lua/bindings.jsonl by default. The chart is
regenerated from the latest cached row per implementation, kind, and size.
"""

import argparse
import datetime as dt
import html
import json
import math
import os
import selectors
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
CACHE_DIR = Path(
    os.environ.get(
        "OMQ_LUA_CACHE_DIR",
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq.lua",
    )
)
JSONL = CACHE_DIR / "bindings.jsonl"
CHART_DIR = ROOT / "doc" / "charts"
CHART = CHART_DIR / "bindings.svg"
DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
THROUGHPUT_MSG_MAX = 6_000_000
DEFAULT_IMPLS = ["omq.lua", "lzmq"]
DEFAULT_LATENCY_IMPLS = ["omq.lua", "lzmq"]
IMPL_LABELS = {
    "omq.lua": "OMQ.lua",
    "lzmq": "lzmq",
}
COLORS = {
    "omq.lua": "#ef4444",
    "lzmq": "#60a5fa",
}
_LUAROCKS_ENV = None


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


def luarocks_env():
    global _LUAROCKS_ENV
    if _LUAROCKS_ENV is not None:
        return _LUAROCKS_ENV
    _LUAROCKS_ENV = {}
    candidates = [
        shutil.which("luarocks"),
        str(Path.home() / ".local" / "bin" / "luarocks"),
    ]
    for candidate in [path for path in candidates if path]:
        if not Path(candidate).exists():
            continue
        result = subprocess.run(
            [candidate, "path"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            continue
        for line in result.stdout.splitlines():
            parts = shlex.split(line)
            if len(parts) != 2 or parts[0] != "export" or "=" not in parts[1]:
                continue
            key, value = parts[1].split("=", 1)
            if key in {"LUA_PATH", "LUA_CPATH", "PATH"}:
                _LUAROCKS_ENV[key] = value
        break
    return _LUAROCKS_ENV


def lua_env(profile):
    lib_dir = ROOT / "native" / "target" / profile
    lib_pattern = "?.dylib" if sys.platform == "darwin" else "lib?.so"
    env = os.environ.copy()
    rock_env = luarocks_env()
    env["LUA_PATH"] = f"{ROOT / 'lua'}/?.lua;{rock_env.get('LUA_PATH', env.get('LUA_PATH', ';;'))}"
    env["LUA_CPATH"] = f"{lib_dir}/{lib_pattern};{rock_env.get('LUA_CPATH', env.get('LUA_CPATH', ';;'))}"
    if "PATH" in rock_env:
        env["PATH"] = rock_env["PATH"]
    return env


def run(cmd, cwd=ROOT, timeout=None, fail_on_warning=True):
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
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    if fail_on_warning and has_noise(result.stdout):
        raise RuntimeError("command printed warning/timeout")


def has_noise(text):
    lowered = (text or "").lower()
    return "warning" in lowered or "timeout" in lowered


def build_native(args):
    if args.no_build:
        return
    cmd = ["cargo", "build", "--manifest-path", str(ROOT / "native" / "Cargo.toml")]
    if not args.debug:
        cmd.append("--release")
    run(cmd, cwd=REPO)


def lua_module_available(lua_bin, module, profile):
    result = subprocess.run(
        [lua_bin, "-e", f"require({module!r})"],
        cwd=REPO,
        env=lua_env(profile),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return result.returncode == 0


def check_impl_availability(lua_bin, profile, impls):
    available = {}
    for impl in impls:
        if impl == "lzmq":
            available[impl] = lua_module_available(lua_bin, "lzmq", profile)
            if not available[impl]:
                print(f"skip: lzmq module not installed for {lua_bin}")
        else:
            available[impl] = True
    return available


def filter_available_impls(impls, availability):
    available = []
    for impl in impls:
        if not availability.get(impl, False):
            continue
        available.append(impl)
    return available


def free_endpoint():
    import socket

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


def peer_cmd(lua_bin, bench, impl, role, endpoint, size, amount, warmup):
    return [
        lua_bin,
        str(ROOT / "scripts" / "bench_peer.lua"),
        bench,
        impl,
        role,
        endpoint,
        str(size),
        str(amount),
        str(warmup),
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


def run_throughput_pair_once(lua_bin, impl, size, duration, warmup, timeout, profile):
    endpoint = free_endpoint()
    env = lua_env(profile)
    receiver = subprocess.Popen(
        peer_cmd(
            lua_bin,
            "pushpull",
            impl,
            "pull",
            endpoint,
            size,
            f"{duration:.6f}",
            f"{warmup:.6f}",
        ),
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    sender = None
    try:
        ready = read_line_timeout(receiver, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = receiver.communicate(timeout=1) if receiver.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        sender = subprocess.Popen(
            peer_cmd(
                lua_bin,
                "pushpull",
                impl,
                "push",
                endpoint,
                size,
                f"{duration:.6f}",
                f"{warmup:.6f}",
            ),
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
    lua_bin,
    bench,
    impl,
    receiver_role,
    sender_role,
    size,
    duration,
    warmup,
    timeout,
    profile,
):
    endpoint = free_endpoint()
    env = lua_env(profile)
    receiver = subprocess.Popen(
        peer_cmd(lua_bin, bench, impl, receiver_role, endpoint, size, duration, warmup),
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        ready = read_line_timeout(receiver, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = receiver.communicate(timeout=1) if receiver.poll() is not None else ("", "")
            raise RuntimeError(f"receiver did not become ready:\n{ready or ''}{out}{err}")

        sender = subprocess.run(
            peer_cmd(lua_bin, bench, impl, sender_role, endpoint, size, duration, warmup),
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


def run_throughput_cell(lua_bin, impl, size, args, profile):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.warmup_duration + args.duration
    for round_index in range(total):
        row = run_throughput_pair_once(
            lua_bin,
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
            f"  {impl:12s} size={size:6d} round={round_index + 1}/{total} "
            f"{row['msgs_s']:12.0f} msg/s {row['gb_s']:7.3f} GB/s "
            f"n={row['messages']} t={row['seconds']:.3f}s",
            flush=True,
        )
    return sorted(runs, key=lambda row: row["msgs_s"])[len(runs) // 2]


def run_latency_cell(lua_bin, impl, size, args, profile):
    runs = []
    total = args.warmup_rounds + args.rounds
    timeout = args.timeout + args.latency_warmup_duration + args.latency_duration
    for round_index in range(total):
        row = run_pair_once(
            lua_bin,
            "reqrep",
            impl,
            "rep",
            "req",
            size,
            f"{args.latency_duration:.6f}",
            f"{args.latency_warmup_duration:.6f}",
            timeout,
            profile,
        )
        row["target_seconds"] = args.latency_duration
        row["warmup_seconds"] = args.latency_warmup_duration
        if round_index >= args.warmup_rounds:
            runs.append(row)
        print(
            f"  {impl:12s} size={size:6d} round={round_index + 1}/{total} "
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


def normalized_kind(row):
    kind = row.get("kind")
    if kind == "throughput" and row.get("transport") == "tcp":
        return "pushpull_tcp"
    return kind


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def print_table(rows, sizes, latency_sizes, impls, latency_impls):
    by_key = {(row["kind"], row["msg_size"], row["impl"]): row for row in rows}
    print()
    if impls:
        print("PUSH/PULL TCP throughput")
        print("size    impl             msg/s      GB/s   vs lzmq")
        for size in sizes:
            base = by_key.get(("pushpull_tcp", size, "lzmq"))
            base_msgs = base["msgs_s"] if base else 0.0
            for impl in impls:
                row = by_key.get(("pushpull_tcp", size, impl))
                if row is None:
                    continue
                ratio = row["msgs_s"] / base_msgs if base_msgs else 0.0
                print(
                    f"{size:6d} {impl:12s} {row['msgs_s']:11.0f} "
                    f"{row['gb_s']:8.3f} {ratio:9.2f}x"
                )
            print()
    if latency_impls:
        print("REQ/REP TCP latency")
        print("size    impl             p50 us    p99 us   vs lzmq")
        for size in latency_sizes:
            base = by_key.get(("reqrep_tcp_latency", size, "lzmq"))
            base_p50 = base["p50_us"] if base else 0.0
            for impl in latency_impls:
                row = by_key.get(("reqrep_tcp_latency", size, impl))
                if row is None:
                    continue
                ratio = base_p50 / row["p50_us"] if row["p50_us"] else 0.0
                print(
                    f"{size:6d} {impl:12s} {row['p50_us']:9.1f} "
                    f"{row['p99_us']:9.1f} {ratio:9.2f}x"
                )
            print()


def latest_chart_data(sizes, latency_sizes, impls, latency_impls):
    latest = {}
    for row in load_jsonl():
        kind = normalized_kind(row)
        impl = row.get("impl")
        size = row.get("msg_size")
        key = (kind, impl, size)
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row

    throughput = {
        impl: [
            latest.get(("pushpull_tcp", impl, size), {}).get("msgs_s", 0.0)
            for size in sizes
        ]
        for impl in impls
    }
    latency = {
        impl: [
            latest.get(("reqrep_tcp_latency", impl, size), {}).get("p50_us", 0.0)
            for size in latency_sizes
        ]
        for impl in latency_impls
    }
    return {"throughput": throughput, "latency": latency}


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


def detect_hardware():
    config = read_chart_hw()
    label = os.environ.get("OMQ_HW_LABEL") or config.get("label")
    if label:
        return label
    prefix = os.environ.get("OMQ_HW_PREFIX") or config.get("prefix")
    postfix = os.environ.get("OMQ_HW_POSTFIX") or config.get("postfix")
    if prefix and postfix:
        return f"{prefix}, {postfix}"
    if prefix:
        return prefix
    if postfix:
        return postfix
    return None


def chart_selection(args, sizes):
    if args.latency_only or args.throughput_only:
        return (
            DEFAULT_SIZES,
            latency_sizes_from(DEFAULT_SIZES),
            DEFAULT_IMPLS,
            DEFAULT_LATENCY_IMPLS,
        )
    return sizes, latency_sizes_from(sizes), args.impls, args.latency_impls


def svg_line(points, color, dashed=False):
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    return (
        f'  <polyline points="{points}" fill="none" stroke="{color}"'
        f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"{dash}/>'
    )


def visible_impls(data, key, impls):
    return [impl for impl in impls if any(data[key].get(impl, []))]


def gen_chart(data, path, sizes, latency_sizes, impls, latency_impls):
    impls = visible_impls(data, "throughput", impls)
    latency_impls = visible_impls(data, "latency", latency_impls)
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

    all_rates = [rate for values in data["throughput"].values() for rate in values]
    all_mbps = [
        rate * sizes[index] / 1_000_000.0
        for values in data["throughput"].values()
        for index, rate in enumerate(values)
    ]
    msg_max = THROUGHPUT_MSG_MAX
    mbps_max = max(5_000, nice_ceil(max(all_mbps or [0]) * 1.05))
    lat_values = [v for values in data["latency"].values() for v in values]
    lat_max = max(200, nice_ceil(max(lat_values or [0]) * 1.05))

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
        values = data["throughput"].get(impl, [])
        points = " ".join(f"{xs[i]:.1f},{y_msg(value):.1f}" for i, value in enumerate(values))
        lines.append(svg_line(points, COLORS[impl], dashed=True))
    for impl in impls:
        values = data["throughput"].get(impl, [])
        mbps = [value * sizes[index] / 1_000_000.0 for index, value in enumerate(values)]
        points = " ".join(f"{xs[i]:.1f},{y_mbps(value):.1f}" for i, value in enumerate(mbps))
        lines.append(svg_line(points, COLORS[impl]))
        for i, value in enumerate(mbps):
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{y_mbps(value):.1f}" r="3"'
                f' fill="{COLORS[impl]}" stroke="#000000" stroke-width="1"/>'
            )

    for i, size in enumerate(sizes):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )

    add_legend(lines, [(IMPL_LABELS[impl], COLORS[impl]) for impl in impls], mid_x, t1_leg_y)
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{t1_leg_y + 18}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"dashed = msg/s (left), solid = throughput (right)</text>"
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
        values = data["latency"].get(impl, [])
        points = " ".join(
            f"{lat_xs[i]:.1f},{y_lat(value):.1f}" for i, value in enumerate(values)
        )
        lines.append(svg_line(points, COLORS[impl]))
        for i, value in enumerate(values):
            lines.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{y_lat(value):.1f}" r="3"'
                f' fill="{COLORS[impl]}" stroke="#000000" stroke-width="1"/>'
            )
    for i, size in enumerate(latency_sizes):
        lines.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(size)}</text>'
        )

    add_legend(
        lines,
        [(IMPL_LABELS[impl], COLORS[impl]) for impl in latency_impls],
        mid_x,
        t2_bot + 40,
    )
    lines.append("</svg>")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    print(f"wrote {path}")


def add_legend(lines, legend_items, mid_x, leg_y):
    marker_w = 14
    marker_gap = 6
    item_gap = 30
    text_px = 6.2
    item_widths = [
        marker_w + marker_gap + len(label) * text_px for label, _ in legend_items
    ]
    total_w = sum(item_widths) + item_gap * max(0, len(legend_items) - 1)
    start_x = mid_x - total_w / 2
    lx = start_x
    for index, (label, color) in enumerate(legend_items):
        lines.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        lines.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>')
        lines.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{html.escape(label)}</text>'
        )
        lx += item_widths[index] + item_gap


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes", type=parse_csv_ints)
    parser.add_argument("--impls", type=parse_csv_strings, default=DEFAULT_IMPLS)
    parser.add_argument("--latency-impls", type=parse_csv_strings, default=DEFAULT_LATENCY_IMPLS)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=0)
    parser.add_argument("--duration", type=float, default=2.5)
    parser.add_argument("--warmup-duration", type=float, default=0.5)
    parser.add_argument("--latency-duration", type=float, default=1.5)
    parser.add_argument("--latency-warmup-duration", type=float)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--lua", default=os.environ.get("OMQ_LUA", "/usr/bin/lua"))
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--no-chart", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--throughput-only", action="store_true")
    parser.add_argument("--latency-only", action="store_true")
    parser.add_argument("--debug", action="store_true", help="use debug native build")
    args = parser.parse_args()

    if args.throughput_only and args.latency_only:
        parser.error("--throughput-only and --latency-only are mutually exclusive")

    sizes = args.sizes or (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    if args.quick:
        args.rounds = min(args.rounds, 1)
        args.warmup_rounds = 0
        args.duration = min(args.duration, 0.5)
        args.latency_duration = min(args.latency_duration, 0.5)
        args.warmup_duration = min(args.warmup_duration, 0.1)
    if args.latency_warmup_duration is None:
        args.latency_warmup_duration = args.warmup_duration
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.warmup_rounds < 0:
        parser.error("--warmup-rounds cannot be negative")
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.warmup_duration < 0:
        parser.error("--warmup-duration cannot be negative")
    if args.latency_duration <= 0:
        parser.error("--latency-duration must be positive")
    if args.latency_warmup_duration < 0:
        parser.error("--latency-warmup-duration cannot be negative")
    for impl in args.impls:
        if impl not in IMPL_LABELS:
            parser.error(f"unknown throughput impl: {impl}")
    for impl in args.latency_impls:
        if impl not in IMPL_LABELS:
            parser.error(f"unknown latency impl: {impl}")
    latency_sizes = latency_sizes_from(sizes)

    if args.chart_only:
        data = latest_chart_data(sizes, latency_sizes, args.impls, args.latency_impls)
        gen_chart(data, CHART, sizes, latency_sizes, args.impls, args.latency_impls)
        return

    if shutil.which(args.lua) is None and not Path(args.lua).exists():
        raise SystemExit(f"lua not found: {args.lua}")

    profile = "debug" if args.debug else "release"
    build_native(args)
    if not lua_module_available(args.lua, "omq", profile):
        raise RuntimeError(f"OMQ.lua module not loadable for {args.lua}")

    requested_impls = sorted(set(args.impls + args.latency_impls))
    availability = check_impl_availability(args.lua, profile, requested_impls)
    throughput_impls = [] if args.latency_only else filter_available_impls(
        args.impls,
        availability,
    )
    latency_impls = [] if args.throughput_only else filter_available_impls(
        args.latency_impls,
        availability,
    )
    if not throughput_impls and not latency_impls:
        raise RuntimeError("no requested Lua benchmark implementations available")

    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    arena_threshold = os.environ.get("OMQ_BENCH_ARENA_THRESHOLD")
    rows = []
    print(f"run_id={run_id}")
    if throughput_impls:
        print("PUSH/PULL TCP throughput")
        for size in sizes:
            for impl in throughput_impls:
                row = run_throughput_cell(args.lua, impl, size, args, profile)
                row["run_id"] = run_id
                row["kind"] = "pushpull_tcp"
                row["transport"] = "tcp"
                if impl == "omq.lua" and arena_threshold:
                    row["arena_threshold"] = arena_threshold
                rows.append(row)
    if latency_impls:
        print("REQ/REP TCP latency")
        for size in latency_sizes:
            for impl in latency_impls:
                row = run_latency_cell(args.lua, impl, size, args, profile)
                row["run_id"] = run_id
                row["kind"] = "reqrep_tcp_latency"
                row["transport"] = "tcp"
                if impl == "omq.lua" and arena_threshold:
                    row["arena_threshold"] = arena_threshold
                rows.append(row)

    if not args.no_save:
        append_jsonl(rows)
        print(f"appended {len(rows)} rows to {JSONL}")
    print_table(rows, sizes, latency_sizes, throughput_impls, latency_impls)

    if not args.no_chart and not args.no_save:
        chart_sizes, chart_latency_sizes, chart_impls, chart_latency_impls = chart_selection(
            args, sizes
        )
        data = latest_chart_data(
            chart_sizes, chart_latency_sizes, chart_impls, chart_latency_impls
        )
        gen_chart(
            data,
            CHART,
            chart_sizes,
            chart_latency_sizes,
            chart_impls,
            chart_latency_impls,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (RuntimeError, subprocess.TimeoutExpired, OSError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
