#!/usr/bin/env python3
"""Measure OMQ BEAM bindings over two-process TCP loopback.

Rows append to ~/.cache/omq.beam/<lang>/bindings.jsonl. Chart generation uses
latest cached row per implementation, kind, and size. Hardware subtitle reads
repo root .chart_hw when present.
"""

import argparse
import datetime as dt
import json
import math
import os
import selectors
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
CACHE_ROOT = Path(os.environ.get("OMQ_BEAM_CACHE_DIR", Path.home() / ".cache" / "omq.beam"))
CHART = ROOT / "doc" / "charts" / "bindings.svg"
DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
QUICK_SIZES = [16, 128, 1024, 4096, 32768]
LATENCY_MAX_SIZE = 4096
SIZES = DEFAULT_SIZES.copy()
TARGET_RUNTIME_S = 2.5
THROUGHPUT_WARMUP_S = 0.5
N_ROUNDS = 3
LATENCY_WARMUP_S = 0.5
LATENCY_RUNTIME_S = 1.5
SUBPROCESS_TIMEOUT_S = 60.0
DEFAULT_IMPLS = ["omq-erlang", "omq-elixir", "omq-gleam", "erlzmq", "exzmq"]
C_OMQ_ERLANG = "#ef4444"
C_OMQ_ELIXIR = "#fb923c"
C_OMQ_GLEAM = "#22c55e"
C_ERLZMQ = "#60a5fa"


def impl_lang(impl):
    return {
        "omq-erlang": "erlang",
        "erlzmq": "erlang",
        "omq-elixir": "elixir",
        "exzmq": "elixir",
        "omq-gleam": "gleam",
    }.get(impl, "unknown")


def jsonl_for_impl(impl):
    return CACHE_ROOT / impl_lang(impl) / "bindings.jsonl"


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


def build(no_build):
    if no_build:
        return
    run(["rebar3", "compile"], cwd=ROOT)
    run(["cargo", "build", "--release"], cwd=ROOT / "native")
    release_nif = ROOT / "native" / "target" / "release" / "libomq_beam_native.so"
    priv_nif = ROOT / "priv" / "omq_beam_native.so"
    priv_nif.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(release_nif, priv_nif)
    run(["mix", "compile", "--warnings-as-errors"], cwd=ROOT / "elixir")
    gleam = gleam_bin()
    if gleam:
        run([gleam, "build"], cwd=ROOT / "gleam")


def gleam_bin():
    for path in [
        shutil.which("gleam"),
        str(Path.home() / "src" / "gleam" / "target" / "release" / "gleam"),
    ]:
        if path and Path(path).exists():
            return path
    return None


def impl_available(impl):
    if impl == "omq-gleam":
        return gleam_bin() is not None
    if impl == "erlzmq":
        return shutil.which("elixir") is not None
    if impl == "exzmq":
        return False
    return True


def erl_module_available(module):
    code = f"case code:ensure_loaded({module}) of {{module,{module}}}->halt(0); _->halt(1) end."
    return subprocess.run(["erl", "-noshell", "-eval", code], stdout=subprocess.DEVNULL).returncode == 0


def elixir_module_available(module):
    code = f"if Code.ensure_loaded?({module}), do: System.halt(0), else: System.halt(1)"
    return subprocess.run(["elixir", "-e", code], stdout=subprocess.DEVNULL).returncode == 0


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


def read_ready(proc, seconds):
    deadline = time.monotonic() + seconds
    lines = []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None, "".join(lines)
        line = read_line_timeout(proc, remaining)
        if line is None:
            return None, "".join(lines)
        lines.append(line)
        if has_noise(line):
            return None, "".join(lines)
        if line.startswith("READY "):
            return line, "".join(lines)


def peer_cmd(impl, bench, role, endpoint, size, duration, warmup):
    if impl in {"omq-erlang", "omq-gleam"}:
        return [
            "escript",
            str(ROOT / "scripts" / "bench_peer.erl"),
            bench,
            impl,
            role,
            endpoint,
            str(size),
            f"{duration:.6f}",
            f"{warmup:.6f}",
        ]
    if impl in {"omq-elixir", "erlzmq"}:
        return [
            "elixir",
            str(ROOT / "scripts" / "bench_peer.exs"),
            bench,
            impl,
            role,
            endpoint,
            str(size),
            f"{duration:.6f}",
            f"{warmup:.6f}",
        ]
    raise RuntimeError(f"{impl} benchmark peer not implemented yet")


def parse_result(output):
    for line in output.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:])
    raise RuntimeError("missing RESULT line:\n" + output)


def kill(proc):
    if proc.poll() is None:
        proc.kill()
    return proc.communicate(timeout=5)


def run_pair(impl, bench, receiver_role, sender_role, size, duration, warmup, timeout):
    endpoint = free_endpoint()
    receiver = subprocess.Popen(
        peer_cmd(impl, bench, receiver_role, endpoint, size, duration, warmup),
        cwd=REPO,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    sender = None
    try:
        ready, ready_output = read_ready(receiver, 10)
        if ready is None or not ready.startswith("READY "):
            out, err = kill(receiver)
            raise RuntimeError(f"receiver not ready:\n{ready_output}{out}{err}")
        sender = subprocess.Popen(
            peer_cmd(impl, bench, sender_role, endpoint, size, duration, warmup),
            cwd=REPO,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if bench == "reqrep":
            out, err = sender.communicate(timeout=timeout)
            if sender.returncode != 0 or has_noise(out + err):
                raise RuntimeError(f"sender failed:\n{out}{err}")
            kill(receiver)
            return parse_result(out)
        out, err = receiver.communicate(timeout=timeout)
        if receiver.returncode != 0 or has_noise(out + err):
            raise RuntimeError(f"receiver failed:\n{ready_output}{out}{err}")
        kill(sender)
        return parse_result(ready_output + out)
    except Exception:
        kill(receiver)
        if sender is not None:
            kill(sender)
        raise


def run_bench(args):
    for impl in args.impl:
        if not impl_available(impl):
            print(f"skip: {impl} unavailable", flush=True)
            continue
        for size in args.sizes:
            for round_index in range(args.rounds):
                row = run_pair(
                    impl,
                    "pushpull",
                    "pull",
                    "push",
                    size,
                    args.duration,
                    args.warmup_duration,
                    args.timeout,
                )
                row["run_id"] = args.run_id
                append_jsonl([row])
                print(
                    f"  {impl:10s} pushpull {size:6d} {row['msgs_s']:12.0f} msg/s",
                    flush=True,
                )
                if size <= 4096:
                    row = run_pair(
                        impl,
                        "reqrep",
                        "rep",
                        "req",
                        size,
                        args.latency_duration,
                        args.latency_warmup_duration,
                        args.timeout,
                    )
                    row["run_id"] = args.run_id
                    append_jsonl([row])
                    print(
                        f"  {impl:10s} reqrep   {size:6d} p50 {row['p50_us']:8.1f} us",
                        flush=True,
                    )


def append_jsonl(rows):
    if not rows:
        return
    grouped = {}
    for row in rows:
        grouped.setdefault(impl_lang(row["impl"]), []).append(row)
    for lang, lang_rows in grouped.items():
        path = CACHE_ROOT / lang / "bindings.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as file:
            for row in lang_rows:
                file.write(json.dumps(row, sort_keys=True) + "\n")


def load_rows():
    rows = []
    for path in sorted(CACHE_ROOT.glob("*/bindings.jsonl")):
        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return rows


def latency_sizes_from(sizes):
    return [size for size in sizes if size <= LATENCY_MAX_SIZE]


def fmt_size(size):
    if size >= 1024:
        return f"{size // 1024} KiB"
    return f"{size} B"


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def _fmt_y_us(val):
    if val >= 1000:
        return f"{val / 1000:g} ms"
    return f"{val:g} us"


def _nice_ceil(value):
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for multiple in (1, 2, 5, 10):
        candidate = multiple * base
        if candidate >= value:
            return candidate
    return 10 * base


def _nice_ticks(max_value, target_ticks=5):
    step = _nice_ceil(max_value / target_ticks)
    ticks = [0]
    tick = step
    while tick < max_value:
        ticks.append(tick)
        tick += step
    ticks.append(max_value)
    return ticks


def _read_chart_hw():
    config = {}
    path = REPO / ".chart_hw"
    try:
        for line in path.read_text().splitlines():
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

    erlang_tp = data["erlang_tp"]
    elixir_tp = data["elixir_tp"]
    gleam_tp = data["gleam_tp"]
    erlzmq_tp = data["erlzmq_tp"]

    tp_values = [erlang_tp, elixir_tp, gleam_tp, erlzmq_tp]
    small_indices = [SIZES.index(s) for s in small_sizes]
    large_indices = [SIZES.index(s) for s in large_sizes]
    msg_values = [
        values[i]
        for values in tp_values
        for i in small_indices
        if i < len(values) and values[i] > 0
    ]
    msg_max = _nice_ceil(max(msg_values, default=1))
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

    # -- TOP PANEL: THROUGHPUT --------------------------------------

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
            _nice_ticks(msg_max),
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
            lambda v: f"{v:g} GB/s",
            top_right + 8,
        ),
    ):
        for tick in ticks:
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
        ("OMQ Erlang", C_OMQ_ERLANG, erlang_tp),
        ("OMQ Elixir", C_OMQ_ELIXIR, elixir_tp),
        ("OMQ Gleam", C_OMQ_GLEAM, gleam_tp),
        ("erlzmq", C_ERLZMQ, erlzmq_tp),
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
        f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate / solid = bandwidth</text>'
    )

    # -- BOTTOM PANEL: LATENCY --------------------------------------

    L.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 us (lower is better)</text>"
    )

    erlang_lat = data["erlang_lat"]
    elixir_lat = data["elixir_lat"]
    gleam_lat = data["gleam_lat"]
    erlzmq_lat = data["erlzmq_lat"]

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
        ("OMQ Erlang", C_OMQ_ERLANG, erlang_lat),
        ("OMQ Elixir", C_OMQ_ELIXIR, elixir_lat),
        ("OMQ Gleam", C_OMQ_GLEAM, gleam_lat),
        ("erlzmq", C_ERLZMQ, erlzmq_lat),
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

    # -- LEGEND ------------------------------------------------------

    leg_y = t2_bot + 40
    legend_items = [
        ("OMQ Erlang", C_OMQ_ERLANG),
        ("OMQ Elixir", C_OMQ_ELIXIR),
        ("OMQ Gleam", C_OMQ_GLEAM),
        ("erlzmq", C_ERLZMQ),
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

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(L) + "\n")
    print(f"  wrote {path}")


def chart_data_from_jsonl():
    latest = {}
    for row in load_rows():
        key = (row["impl"], row["kind"], row["msg_size"])
        if key not in latest or row.get("run_id", "") >= latest[key].get("run_id", ""):
            latest[key] = row

    def tp(impl, size):
        return latest.get((impl, "throughput", size), {}).get("msgs_s", 0.0)

    def lat(impl, size):
        return latest.get((impl, "latency", size), {}).get("p50_us", 0.0)

    return {
        "erlang_tp": [tp("omq-erlang", s) for s in SIZES],
        "elixir_tp": [tp("omq-elixir", s) for s in SIZES],
        "gleam_tp": [tp("omq-gleam", s) for s in SIZES],
        "erlzmq_tp": [tp("erlzmq", s) for s in SIZES],
        "erlang_lat": [lat("omq-erlang", s) for s in latency_sizes_from(SIZES)],
        "elixir_lat": [lat("omq-elixir", s) for s in latency_sizes_from(SIZES)],
        "gleam_lat": [lat("omq-gleam", s) for s in latency_sizes_from(SIZES)],
        "erlzmq_lat": [lat("erlzmq", s) for s in latency_sizes_from(SIZES)],
    }


def parse_args():
    global SIZES
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--impl", default=",".join(DEFAULT_IMPLS))
    parser.add_argument("--sizes")
    parser.add_argument("--rounds", type=int, default=N_ROUNDS)
    parser.add_argument("--duration", type=float, default=TARGET_RUNTIME_S)
    parser.add_argument("--warmup-duration", type=float, default=THROUGHPUT_WARMUP_S)
    parser.add_argument("--latency-duration", type=float, default=LATENCY_RUNTIME_S)
    parser.add_argument("--latency-warmup-duration", type=float, default=LATENCY_WARMUP_S)
    parser.add_argument("--timeout", type=float, default=SUBPROCESS_TIMEOUT_S)
    args = parser.parse_args()
    if args.quick and not args.sizes:
        args.sizes = ",".join(str(size) for size in QUICK_SIZES)
        args.duration = 0.2
        args.latency_duration = 0.2
        args.rounds = 1
    args.impl = [part.strip() for part in args.impl.split(",") if part.strip()]
    if args.sizes:
        args.sizes = [int(part.strip()) for part in args.sizes.split(",") if part.strip()]
    else:
        args.sizes = DEFAULT_SIZES.copy()
    SIZES = args.sizes
    args.run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return args


def main():
    args = parse_args()
    if not args.chart_only:
        build(args.no_build)
        run_bench(args)
    gen_combined_chart(chart_data_from_jsonl(), CHART)


if __name__ == "__main__":
    main()
