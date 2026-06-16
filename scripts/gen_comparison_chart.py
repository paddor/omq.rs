#!/usr/bin/env python3
"""Generate comparison SVG charts from benchmarks/comparisons.jsonl.

Produces:
  doc/charts/pushpull/omq_tcp.svg  — TCP: throughput + CPU% (omq backends + libzmq)
  doc/charts/pushpull/omq_ipc.svg  — IPC: throughput + CPU% (omq backends + libzmq)
  doc/charts/pushpull/omq_inproc.svg — inproc: throughput + CPU% (omq backends + libzmq)
"""

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"

COLORS = {
    "libzmq": "#eab308",
    "omq-compio": "#7c3aed",
    "omq-compio-st": "#ff69b4",
    "omq-tokio": "#f97316",
    "omq-tokio-mt": "#dc2626",
    "zmq.rs": "#2563eb",
    "rzmq": "#16a34a",
    "omq-libzmq": "#06b6d4",
}

LABELS = {
    "libzmq": "libzmq v4.3.5",
    "omq-compio": "omq-compio",
    "omq-compio-st": "omq-compio (ST)",
    "omq-tokio": "omq-tokio",
    "omq-tokio-mt": "omq-tokio (MT)",
    "zmq.rs": "zmq.rs v0.6.0",
    "rzmq": "rzmq v0.5.18",
    "omq-libzmq": "omq-libzmq",
}


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024 * 1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


# ── data loading ──────────────────────────────────────────────────

def load_jsonl() -> list[dict]:
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found", file=sys.stderr)
        sys.exit(1)
    rows = []
    for line in JSONL_PATH.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def load_data(transport: str, impls: list[str]) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows if r.get("transport") == transport]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    lat: dict[int, dict[str, float]] = {}
    lat_cpu: dict[int, dict[str, float]] = {}

    seen_tput: dict[tuple, str] = {}
    seen_lat: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        kind = r.get("kind")

        if kind == "throughput":
            key = (impl_name, size)
            if key not in seen_tput or run_id >= seen_tput[key]:
                seen_tput[key] = run_id
                msgs_s = r.get("msgs_s", 0)
                mbps = r.get("mbps", 0)
                gbs = mbps / 1000.0
                tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)
                cpu_time = r.get("cpu_time", 0)
                elapsed = r.get("elapsed", 0)
                if elapsed > 0 and cpu_time > 0:
                    tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

        elif kind == "latency":
            key = (impl_name, size)
            if key not in seen_lat or run_id >= seen_lat[key]:
                seen_lat[key] = run_id
                lat.setdefault(size, {})[impl_name] = r.get("p50_us", 0)
                cpu_time = r.get("cpu_time", 0)
                elapsed = r.get("elapsed", 0)
                if elapsed > 0 and cpu_time > 0:
                    lat_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s <= 32768)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu,
            "lat": lat, "lat_cpu": lat_cpu}


# ── SVG helpers ───────────────────────────────────────────────────

def svg_line(x1, y1, x2, y2, stroke="#e5e7eb", width=1, dash=None) -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (
        f'  <line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}"'
        f' stroke="{stroke}" stroke-width="{width}"{d}/>'
    )


def svg_text(x, y, text, anchor="middle", fill="#374151", size=10, weight=None,
             baseline=None, rotate=None) -> str:
    parts = [f'  <text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}"']
    if baseline:
        parts[0] += f' dominant-baseline="{baseline}"'
    parts[0] += f' fill="{fill}" font-size="{size}"'
    if weight:
        parts[0] += f' font-weight="{weight}"'
    if rotate:
        parts[0] += f' transform="rotate({rotate},{x:.1f},{y:.1f})"'
    parts[0] += f">{text}</text>"
    return parts[0]


def svg_polyline(points: list[tuple[float, float]], color: str, width=2.5,
                 dash=None) -> str:
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    d = f' stroke-dasharray="{dash}"' if dash else ""
    cap = ' stroke-linecap="round" stroke-linejoin="round"' if not dash else ""
    return (
        f'  <polyline points="{pts}" fill="none" stroke="{color}"'
        f' stroke-width="{width}"{cap}{d}/>'
    )


def svg_dots(points: list[tuple[float, float]], color: str) -> list[str]:
    return [
        f'  <circle cx="{x:.1f}" cy="{y:.1f}" r="3"'
        f' fill="{color}" stroke="white" stroke-width="1"/>'
        for x, y in points
    ]


# ── chart panels ─────────────────────────────────────────────────

def draw_throughput_panel(
    L: list[str], sizes: list[int], xs: list[float], tput: dict,
    impls: list[str], x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str, log_gbs: bool = False,
    fixed_msg_max: float | None = None,
    fixed_gbs_max: float | None = None,
    msg_break: tuple[float, float] | None = None,
):
    import math

    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_msgs = [
        tput[s][name][0]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    msg_max = fixed_msg_max if fixed_msg_max else (max(all_msgs) * 1.15 if all_msgs else 16e6)

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    gbs_max = max(all_gbs) if all_gbs else 10.0
    gbs_min = min(all_gbs) if all_gbs else 0.01
    if log_gbs:
        gbs_min = max(gbs_min, 0.01)
        log_lo = math.floor(math.log10(gbs_min * 0.8))
        log_hi = math.ceil(math.log10((fixed_gbs_max or gbs_max) * 1.15))
    else:
        tput_max = fixed_gbs_max if fixed_gbs_max else gbs_max * 1.15

    if msg_break:
        break_val, bottom_frac = msg_break
        y_break = y_bot - bottom_frac * h

        def y_msg(v):
            if v <= break_val:
                return y_bot - (v / break_val) * bottom_frac * h
            return y_break - ((v - break_val) / (msg_max - break_val)) * (1 - bottom_frac) * h
    else:
        def y_msg(v):
            return y_bot - (v / msg_max) * h

    def y_tput(v):
        if log_gbs:
            if v <= 0:
                return y_bot
            frac = (math.log10(v) - log_lo) / (log_hi - log_lo)
            return y_bot - frac * h
        return y_bot - (v / tput_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # msg/s gridlines (left axis)
    if msg_break:
        _bv, _ = msg_break
        step_lo = nice_step(_bv, 4)
        v = step_lo
        while v < _bv:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            label = f"{v / 1e3:.0f}k" if v < 1e6 else f"{int(v / 1e6)}M"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_lo
        step_hi = nice_step(msg_max - _bv, 10)
        v = math.ceil(_bv / step_hi) * step_hi
        while v <= msg_max:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            millions = v / 1e6
            if millions >= 1 and millions == int(millions):
                label = f"{int(millions)}M"
            elif v >= 1e6:
                label = f"{millions:.1f}M"
            else:
                label = f"{v / 1e3:.0f}k"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_hi
    else:
        step_msg = nice_step(msg_max, 12)
        v = step_msg
        while v <= msg_max:
            yy = y_msg(v)
            L.append(svg_line(x_left, yy, x_right, yy))
            millions = v / 1e6
            if millions >= 1 and millions == int(millions):
                label = f"{int(millions)}M"
            elif v >= 1e6:
                label = f"{millions:.1f}M"
            else:
                label = f"{v / 1e3:.0f}k"
            L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle"))
            v += step_msg

    # GB/s gridlines (right axis, dashed)
    if log_gbs:
        for decade in range(log_lo, log_hi + 1):
            base = 10 ** decade
            for mult in [1, 2, 5]:
                v = base * mult
                if v < 10 ** log_lo or v > 10 ** log_hi:
                    continue
                yy = y_tput(v)
                if mult == 1:
                    L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
                    label = f"{v:.0f}" if v >= 1 else f"{v:g}"
                    L.append(svg_text(x_right + 8, yy, f"{label} GB/s",
                                      anchor="start", baseline="middle",
                                      fill="#6b7280"))
                else:
                    L.append(svg_line(x_left, yy, x_right, yy,
                                      dash="2,8", stroke="#e5e7eb"))
    else:
        step_gbs = nice_step(tput_max, 5)
        v = step_gbs
        while v <= tput_max:
            yy = y_tput(v)
            L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
            L.append(svg_text(x_right + 8, yy, f"{v:.0f} GB/s",
                              anchor="start", baseline="middle", fill="#6b7280"))
            v += step_gbs

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    if msg_break:
        _, _bf = msg_break
        yb = y_bot - _bf * h
        gap = 6
        L.append(
            f'  <rect x="{x_left - 1:.1f}" y="{yb - gap:.1f}"'
            f' width="3" height="{2 * gap}" fill="white"/>'
        )
        L.append(
            f'  <path d="M {x_left - 5:.1f},{yb + gap:.1f}'
            f' L {x_left + 5:.1f},{yb + 1:.1f}'
            f' M {x_left - 5:.1f},{yb - 1:.1f}'
            f' L {x_left + 5:.1f},{yb - gap:.1f}"'
            f' stroke="#9ca3af" stroke-width="1.5" fill="none"/>'
        )

    # axis labels
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "msg/s", weight="600", rotate=-90))

    # dashed msg/s lines
    draw_order = [name for name in
                  ["rzmq", "zmq.rs", "libzmq", "omq-tokio-mt", "omq-tokio",
                   "omq-compio-st", "omq-compio"]
                  if name in impls]
    for name in draw_order:
        pts = [
            (xs[i], y_msg(tput[sizes[i]][name][0]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2, dash="6,4"))

    # solid throughput lines with dots
    for name in draw_order:
        pts = [
            (xs[i], y_tput(tput[sizes[i]][name][1]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def draw_latency_panel(
    L: list[str], sizes: list[int], xs: list[float], lat: dict,
    impls: list[str], x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str, fixed_lat_max: float | None = None,
):
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_vals = [
        lat[s][name]
        for s in sizes for name in impls if name in lat.get(s, {})
    ]
    lat_max = fixed_lat_max if fixed_lat_max else (max(all_vals) * 1.2 if all_vals else 150.0)

    def y_lat(v):
        return y_bot - (v / lat_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # gridlines
    step = nice_step(lat_max, 10)
    v = step
    while v <= lat_max:
        yy = y_lat(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{v:.0f}", anchor="end", baseline="middle"))
        v += step

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    # axis label
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "p50 latency (µs)", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["libzmq", "omq-tokio-mt", "omq-tokio", "rzmq", "zmq.rs",
                   "omq-compio-st", "omq-compio"]
                  if name in impls]
    for name in draw_order:
        pts = [
            (xs[i], y_lat(lat[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def _cpu_ticks(data_max):
    """Return (axis_max, tick_values) for a linear 0-based CPU% axis."""
    if data_max <= 0:
        data_max = 100
    candidates = [50, 100, 200, 400, 500, 800, 1000]
    ceil = data_max
    for c in candidates:
        if c >= data_max:
            ceil = c
            break
    else:
        import math
        ceil = math.ceil(data_max / 100) * 100
    step = 50 if ceil <= 400 else 100
    ticks = list(range(step, int(ceil) + 1, step))
    return ceil, ticks


def draw_throughput_cpu_panel(
    L: list[str], sizes: list[int], xs: list[float], tput: dict,
    tput_cpu: dict, impls: list[str],
    x_left: float, x_right: float, x_right2: float,
    y_top: float, y_bot: float, title: str,
    fixed_gbs_max: float | None = None,
    fixed_msg_max: float | None = None,
    log_gbs: bool = False,
):
    """Three-axis throughput panel: CPU% (left, dotted), GB/s (inner right,
    solid+dots), msg/s (outer right, dashed)."""
    import math

    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_cpu = [
        tput_cpu[s][name]
        for s in sizes for name in impls if name in tput_cpu.get(s, {})
    ]
    cpu_ceil, cpu_ticks = _cpu_ticks(max(all_cpu) * 1.1 if all_cpu else 200)

    all_gbs = [
        tput[s][name][1]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    gbs_max = max(all_gbs) if all_gbs else 10.0
    gbs_min = min(all_gbs) if all_gbs else 0.01
    if log_gbs:
        gbs_min = max(gbs_min, 0.01)
        log_lo = math.floor(math.log10(gbs_min * 0.8))
        log_hi = math.ceil(math.log10((fixed_gbs_max or gbs_max) * 1.15))
    else:
        gbs_max = fixed_gbs_max if fixed_gbs_max else (gbs_max * 1.15)

    all_msgs = [
        tput[s][name][0]
        for s in sizes for name in impls if name in tput.get(s, {})
    ]
    msg_max = fixed_msg_max if fixed_msg_max else (max(all_msgs) * 1.15 if all_msgs else 16e6)

    def y_cpu(v):
        frac = max(0, min(1, v / cpu_ceil))
        return y_bot - frac * h

    def y_gbs(v):
        if log_gbs:
            if v <= 0:
                return y_bot
            frac = (math.log10(v) - log_lo) / (log_hi - log_lo)
            return y_bot - frac * h
        return y_bot - (v / gbs_max) * h

    def y_msg(v):
        return y_bot - (v / msg_max) * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # CPU% gridlines (left axis)
    for val in cpu_ticks:
        yy = y_cpu(val)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{val:g}%",
                          anchor="end", baseline="middle"))

    # GB/s gridlines (inner right axis, dashed)
    if log_gbs:
        for decade in range(log_lo, log_hi + 1):
            base = 10 ** decade
            for mult in [1, 2, 5]:
                v = base * mult
                if v < 10 ** log_lo or v > 10 ** log_hi:
                    continue
                yy = y_gbs(v)
                if mult == 1:
                    L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
                    label = f"{v:.0f}" if v >= 1 else f"{v:g}"
                    L.append(svg_text(x_right + 8, yy, f"{label} GB/s",
                                      anchor="start", baseline="middle",
                                      fill="#6b7280"))
                else:
                    L.append(svg_line(x_left, yy, x_right, yy,
                                      dash="2,8", stroke="#e5e7eb"))
    else:
        step_gbs = nice_step(gbs_max, 5)
        v = step_gbs
        while v <= gbs_max:
            yy = y_gbs(v)
            L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
            L.append(svg_text(x_right + 8, yy, f"{v:.0f} GB/s",
                              anchor="start", baseline="middle", fill="#6b7280"))
            v += step_gbs

    # msg/s tick labels (outer right axis)
    step_msg = nice_step(msg_max, 8)
    v = step_msg
    while v <= msg_max:
        yy = y_msg(v)
        millions = v / 1e6
        if millions >= 1 and millions == int(millions):
            label = f"{int(millions)}M"
        elif v >= 1e6:
            label = f"{millions:.1f}M"
        else:
            label = f"{v / 1e3:.0f}k"
        L.append(svg_text(x_right2 + 8, yy, f"{label}/s",
                          anchor="start", baseline="middle", fill="#9ca3af"))
        v += step_msg

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right2, y_top, x_right2, y_bot, stroke="#d1d5db", width=1))

    # axis labels
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "CPU %", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["libzmq", "omq-libzmq", "omq-tokio-mt", "omq-tokio",
                   "omq-compio-st", "omq-compio", "rzmq", "zmq.rs"]
                  if name in impls]

    # dotted CPU% lines
    for name in draw_order:
        pts = [
            (xs[i], y_cpu(tput_cpu[sizes[i]][name]))
            for i in range(len(sizes)) if name in tput_cpu.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2, dash="2,3"))

    # dashed msg/s lines (outer right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_msg(tput[sizes[i]][name][0]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=1.5, dash="5,3"))

    # solid GB/s lines with dots (inner right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_gbs(tput[sizes[i]][name][1]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def draw_latency_cpu_panel(
    L: list[str], sizes: list[int], xs: list[float], lat: dict,
    lat_cpu: dict, impls: list[str],
    x_left: float, x_right: float,
    y_top: float, y_bot: float, title: str,
    fixed_lat_max: float | None = None,
):
    """Two-axis latency panel: p50 latency (left, solid+dots),
    CPU% (right, dotted)."""
    h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    all_vals = [
        lat[s][name]
        for s in sizes for name in impls if name in lat.get(s, {})
    ]
    lat_max = fixed_lat_max if fixed_lat_max else (max(all_vals) * 1.2 if all_vals else 150.0)

    all_cpu = [
        lat_cpu[s][name]
        for s in sizes for name in impls if name in lat_cpu.get(s, {})
    ]
    cpu_ceil, cpu_ticks = _cpu_ticks(max(all_cpu) * 1.1 if all_cpu else 200)

    def y_lat(v):
        return y_bot - (v / lat_max) * h

    def y_cpu(v):
        frac = max(0, min(1, v / cpu_ceil))
        return y_bot - frac * h

    L.append(svg_text(mid_x, y_top - 17, title, size=13, weight="700", fill="#111827"))

    # latency gridlines (left axis)
    step = nice_step(lat_max, 10)
    v = step
    while v <= lat_max:
        yy = y_lat(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{v:.0f}", anchor="end", baseline="middle"))
        v += step

    # CPU% gridlines (right axis, dashed)
    for val in cpu_ticks:
        yy = y_cpu(val)
        L.append(svg_line(x_left, yy, x_right, yy, dash="3,6"))
        L.append(svg_text(x_right + 8, yy, f"{val:g}%",
                          anchor="start", baseline="middle", fill="#6b7280"))

    # vertical gridlines
    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    # axes
    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_right, y_top, x_right, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    # axis label
    mid_y = (y_top + y_bot) / 2
    L.append(svg_text(40, mid_y, "p50 latency (µs)", weight="600", rotate=-90))

    draw_order = [name for name in
                  ["libzmq", "omq-libzmq", "omq-tokio-mt", "omq-tokio",
                   "rzmq", "zmq.rs", "omq-compio-st", "omq-compio"]
                  if name in impls]

    # dotted CPU% lines (right axis)
    for name in draw_order:
        pts = [
            (xs[i], y_cpu(lat_cpu[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat_cpu.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name], width=2, dash="2,3"))

    # solid latency lines with dots (left axis)
    for name in draw_order:
        pts = [
            (xs[i], y_lat(lat[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    # x-axis labels
    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 14, fmt_size(s), size=8.5))


def nice_step(max_val: float, target_lines: int) -> float:
    raw = max_val / target_lines
    mag = 10 ** int(f"{raw:.0e}".split("e")[1])
    for s in [1, 2, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


# ── chart generation ──────────────────────────────────────────────

def detect_hardware() -> str | None:
    from chart_hw import detect_hardware as _detect
    return _detect()


def _draw_impl_legend(L: list[str], impls: list[str], mid_x: float, leg_y: float,
                      label_overrides: dict | None = None,
                      show_st_mt: bool = False) -> float:
    """Draw impl legend. Returns extra vertical space consumed (0 or 18)."""
    legend_items = [(k, (label_overrides or {}).get(k, LABELS[k])) for k in impls if k in COLORS]
    item_w = 145 if show_st_mt else 125
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = COLORS[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

    if show_st_mt:
        L.append(
            f'  <text x="{mid_x}" y="{leg_y + 18}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="9">'
            f'ST = single-threaded   MT = multi-threaded</text>'
        )
        return 18
    return 0


def generate_chart(data: dict, impls: list[str], transport_label: str,
                   log_gbs: bool = False,
                   fixed_msg_max: float | None = None,
                   fixed_gbs_max: float | None = None,
                   msg_break: tuple[float, float] | None = None,
                   hw_label: str | None = None,
                   label_overrides: dict | None = None) -> str:
    sizes = data["sizes"]
    tput = data["tput"]
    n = len(sizes)
    if n < 2:
        print(f"WARNING: only {n} data points for {transport_label}", file=sys.stderr)
        if n == 0:
            return ""

    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 480 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    t1_y_top = 35 + hw_offset
    t1_y_bot = 385 + hw_offset

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    draw_throughput_panel(
        L, sizes, xs, tput, impls, x_left, x_right, t1_y_top, t1_y_bot,
        f"PUSH/PULL throughput: {transport_label} (higher is better)",
        log_gbs=log_gbs,
        fixed_msg_max=fixed_msg_max,
        fixed_gbs_max=fixed_gbs_max,
        msg_break=msg_break,
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{t1_y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    leg_y = t1_y_bot + 60
    _draw_impl_legend(L, impls, mid_x, leg_y, label_overrides=label_overrides)

    # line-type legend (dashed = msg/s, solid = GB/s)
    lt_y = leg_y + 22
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="6,4"/>'
    )
    L.append(
        f'  <text x="{lt_start + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (left axis)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2"/>'
    )
    L.append(f'  <circle cx="{lt_right + 10:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    gbs_label = "throughput / GB/s (right axis, log)" if log_gbs \
        else "throughput / GB/s (right axis)"
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">{gbs_label}</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_latency_chart(data: dict, impls: list[str], transport_label: str,
                           fixed_lat_max: float | None = None,
                           hw_label: str | None = None,
                           label_overrides: dict | None = None) -> str:
    sizes = data["sizes"]
    lat = data["lat"]
    n = len(sizes)
    if n < 2:
        return ""

    has_latency = any(s in lat and any(name in lat[s] for name in impls) for s in sizes)
    if not has_latency:
        return ""

    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 280 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    y_top = 35 + hw_offset
    y_bot = y_top + 150

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    draw_latency_panel(
        L, sizes, xs, lat, impls, x_left, x_right, y_top, y_bot,
        f"REQ/REP latency: {transport_label} (p50 µs, lower is better)",
        fixed_lat_max=fixed_lat_max,
    )

    leg_y = y_bot + 50
    _draw_impl_legend(L, impls, mid_x, leg_y, label_overrides=label_overrides)

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_chart_cpu(data: dict, impls: list[str], transport_label: str,
                       fixed_gbs_max: float | None = None,
                       fixed_msg_max: float | None = None,
                       log_gbs: bool = False,
                       hw_label: str | None = None,
                       label_overrides: dict | None = None,
                       show_st_mt: bool = False) -> str:
    """Throughput chart with three axes: CPU% (left), GB/s (inner right),
    msg/s (outer right)."""
    sizes = data["sizes"]
    tput = data["tput"]
    tput_cpu = data.get("tput_cpu", {})
    n = len(sizes)
    if n < 2:
        print(f"WARNING: only {n} data points for {transport_label}", file=sys.stderr)
        if n == 0:
            return ""

    hw_offset = 14 if hw_label else 0
    x_left = 90
    x_right = 700
    x_right2 = 780
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2
    right_pad = 15
    svg_w = x_right2 + 80 + right_pad
    st_mt_extra = 18 if show_st_mt else 0
    svg_h = 520 + hw_offset + st_mt_extra

    t1_y_top = 35 + hw_offset
    t1_y_bot = 400 + hw_offset

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    draw_throughput_cpu_panel(
        L, sizes, xs, tput, tput_cpu, impls,
        x_left, x_right, x_right2, t1_y_top, t1_y_bot,
        f"PUSH/PULL throughput: {transport_label}",
        fixed_gbs_max=fixed_gbs_max,
        fixed_msg_max=fixed_msg_max,
        log_gbs=log_gbs,
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{t1_y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    leg_y = t1_y_bot + 40
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    # line-type legend
    lt_y = leg_y + 22 + extra
    lt_total = 500
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="2,3" opacity="0.7"/>'
    )
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (left)</text>'
    )

    lt_mid = lt_start + 145
    L.append(
        f'  <line x1="{lt_mid:.0f}" y1="{lt_y}" x2="{lt_mid + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2.5"/>'
    )
    L.append(f'  <circle cx="{lt_mid + 7:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    L.append(
        f'  <text x="{lt_mid + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">GB/s (inner right{", log" if log_gbs else ""})</text>'
    )

    lt_right = lt_mid + 165
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (outer right)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def generate_latency_chart_cpu(data: dict, impls: list[str], transport_label: str,
                               fixed_lat_max: float | None = None,
                               hw_label: str | None = None,
                               label_overrides: dict | None = None,
                               show_st_mt: bool = False) -> str:
    """Latency chart with two axes: p50 latency (left), CPU% (right, dotted)."""
    sizes = data["sizes"]
    lat = data["lat"]
    lat_cpu = data.get("lat_cpu", {})
    n = len(sizes)
    if n < 2:
        return ""

    has_latency = any(s in lat and any(name in lat[s] for name in impls) for s in sizes)
    if not has_latency:
        return ""

    hw_offset = 14 if hw_label else 0
    st_mt_extra = 18 if show_st_mt else 0
    svg_w = 850
    svg_h = 320 + hw_offset + st_mt_extra
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    y_top = 35 + hw_offset
    y_bot = y_top + 180

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{y_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    draw_latency_cpu_panel(
        L, sizes, xs, lat, lat_cpu, impls, x_left, x_right, y_top, y_bot,
        f"REQ/REP latency: {transport_label} (p50 µs)",
        fixed_lat_max=fixed_lat_max,
    )

    leg_y = y_bot + 40
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    # line-type legend
    lt_y = leg_y + 22 + extra
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2.5"/>'
    )
    L.append(f'  <circle cx="{lt_start + 7:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">p50 latency (left)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="2,3" opacity="0.7"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (right)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def load_pubsub_data(transport: str, impls: list[str], peers: int) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows
              if r.get("transport") == transport
              and r.get("kind") == "pub_sub"
              and r.get("peers") == peers]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    seen: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        key = (impl_name, size)
        if key not in seen or run_id >= seen[key]:
            seen[key] = run_id
            msgs_s = r.get("msgs_s", 0)
            mbps = r.get("mbps", 0)
            # mbps is already aggregate (per-sub × peers) from run_comparisons.
            gbs = mbps / 1000.0
            tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)
            cpu_time = r.get("cpu_time", 0)
            elapsed = r.get("elapsed", 0)
            if elapsed > 0 and cpu_time > 0:
                tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s <= 32768)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu}


def generate_pubsub_chart(
    panels: list[tuple[int, dict]],
    impls: list[str], transport_label: str,
    log_gbs: bool = False,
    fixed_msg_max: float | None = None,
    fixed_gbs_max: float | None = None,
    scale_overrides: dict[int, tuple[float, float | None, bool | None]] | None = None,
    hw_label: str | None = None,
    title_fn: "Callable[[int, str], str] | None" = None,
) -> str:
    panels = [(p, d) for p, d in panels if d["sizes"]]
    if not panels:
        return ""
    sizes = panels[0][1]["sizes"]
    n = len(sizes)
    if n < 2:
        return ""

    panel_h = 240
    gap = 70
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = hw_offset + 35 + len(panels) * (panel_h + gap) + 20
    x_left, x_right = 90, 760
    plot_w = x_right - x_left
    mid_x = (x_left + x_right) / 2

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{hw_offset + 32}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    for idx, (peers, data) in enumerate(panels):
        p_sizes = data["sizes"]
        p_xs = [x_left + i * plot_w / max(len(p_sizes) - 1, 1)
                for i in range(len(p_sizes))]
        y_top = hw_offset + 35 + idx * (panel_h + gap)
        y_bot = y_top + panel_h
        if title_fn:
            panel_title = title_fn(peers, transport_label)
        else:
            sub_label = "1 subscriber" if peers == 1 else f"{peers} subscribers"
            panel_title = f"PUB/SUB throughput, {sub_label}: {transport_label}"
        p_msg_max = fixed_msg_max
        p_gbs_max = fixed_gbs_max
        p_log_gbs = log_gbs
        if scale_overrides and peers in scale_overrides:
            ovr = scale_overrides[peers]
            p_msg_max = ovr[0]
            p_gbs_max = ovr[1]
            if len(ovr) > 2 and ovr[2] is not None:
                p_log_gbs = ovr[2]
        draw_throughput_panel(
            L, p_sizes, p_xs, data["tput"], impls,
            x_left, x_right, y_top, y_bot,
            panel_title,
            log_gbs=p_log_gbs,
            fixed_msg_max=p_msg_max,
            fixed_gbs_max=p_gbs_max,
        )

    last_bot = hw_offset + 35 + (len(panels) - 1) * (panel_h + gap) + panel_h
    leg_y = last_bot + 40

    legend_items = [(k, LABELS[k]) for k in impls if k in COLORS]
    item_w = 125
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = COLORS[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

    lt_y = leg_y + 22
    lt_total = 320
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="6,4"/>'
    )
    L.append(
        f'  <text x="{lt_start + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (left axis)</text>'
    )

    lt_right = lt_start + 170
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2"/>'
    )
    L.append(f'  <circle cx="{lt_right + 10:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    gbs_label = "throughput / GB/s (right axis, log)" if log_gbs \
        else "throughput / GB/s (right axis)"
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">{gbs_label}</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def load_fanio_data(transport: str, impls: list[str], peers: int,
                     kind: str) -> dict:
    rows = load_jsonl()
    t_rows = [r for r in rows
              if r.get("transport") == transport
              and r.get("kind") == kind
              and r.get("peers") == peers]

    tput: dict[int, dict[str, tuple[float, float]]] = {}
    tput_cpu: dict[int, dict[str, float]] = {}
    seen: dict[tuple, str] = {}

    for r in t_rows:
        impl_name = r.get("impl")
        if impl_name not in impls:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        key = (impl_name, size)
        if key not in seen or run_id >= seen[key]:
            seen[key] = run_id
            msgs_s = r.get("msgs_s", 0)
            mbps = r.get("mbps", 0)
            gbs = mbps / 1000.0
            tput.setdefault(size, {})[impl_name] = (msgs_s, gbs)
            cpu_time = r.get("cpu_time", 0)
            elapsed = r.get("elapsed", 0)
            if elapsed > 0 and cpu_time > 0:
                tput_cpu.setdefault(size, {})[impl_name] = cpu_time / elapsed * 100

    sizes = sorted(s for s in tput if s <= 32768)
    return {"sizes": sizes, "tput": tput, "tput_cpu": tput_cpu}


def generate_multi_panel_cpu_chart(
    panels: list[tuple[int, dict]],
    impls: list[str], transport_label: str,
    hw_label: str | None = None,
    title_fn: "Callable[[int, str], str] | None" = None,
    label_overrides: dict | None = None,
    show_st_mt: bool = False,
) -> str:
    panels = [(p, d) for p, d in panels if d["sizes"]]
    if not panels:
        return ""
    sizes = panels[0][1]["sizes"]
    n = len(sizes)
    if n < 2:
        return ""

    x_left = 90
    x_right = 700
    x_right2 = 780
    plot_w = x_right - x_left
    panel_h = 260
    gap = 70
    hw_offset = 14 if hw_label else 0
    st_mt_extra = 18 if show_st_mt else 0
    right_pad = 15
    svg_w = x_right2 + 80 + right_pad
    svg_h = hw_offset + 35 + len(panels) * (panel_h + gap) + 20 + st_mt_extra
    mid_x = (x_left + x_right) / 2

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="{hw_offset + 32}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    for idx, (peers, data) in enumerate(panels):
        p_sizes = data["sizes"]
        p_xs = [x_left + i * plot_w / max(len(p_sizes) - 1, 1)
                for i in range(len(p_sizes))]
        y_top = hw_offset + 35 + idx * (panel_h + gap)
        y_bot = y_top + panel_h
        if title_fn:
            panel_title = title_fn(peers, transport_label)
        else:
            panel_title = f"throughput, {peers} peers: {transport_label}"
        draw_throughput_cpu_panel(
            L, p_sizes, p_xs, data["tput"],
            data.get("tput_cpu", {}), impls,
            x_left, x_right, x_right2, y_top, y_bot,
            panel_title,
        )

    last_bot = hw_offset + 35 + (len(panels) - 1) * (panel_h + gap) + panel_h
    leg_y = last_bot + 30
    extra = _draw_impl_legend(L, impls, mid_x, leg_y,
                              label_overrides=label_overrides,
                              show_st_mt=show_st_mt)

    lt_y = leg_y + 22 + extra
    lt_total = 500
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="2,3" opacity="0.7"/>'
    )
    L.append(
        f'  <text x="{lt_start + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">CPU % (left)</text>'
    )

    lt_mid = lt_start + 145
    L.append(
        f'  <line x1="{lt_mid:.0f}" y1="{lt_y}" x2="{lt_mid + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2.5"/>'
    )
    L.append(f'  <circle cx="{lt_mid + 7:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    L.append(
        f'  <text x="{lt_mid + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">GB/s (inner right)</text>'
    )

    lt_right = lt_mid + 165
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 14:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3"/>'
    )
    L.append(
        f'  <text x="{lt_right + 20:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (outer right)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    FIXED_GBS_MAX = 6.0
    FIXED_LAT_MAX = 150.0
    FIXED_INPROC_LAT_MAX = 40.0
    hw = detect_hardware()

    # ── Main charts (with CPU%): libzmq + omq backends ──────────
    main_impls = ["libzmq", "omq-compio", "omq-tokio", "omq-tokio-mt"]

    for transport, impls, label, log in [
        ("tcp", main_impls, "TCP loopback, 2-process", False),
        ("ipc", main_impls, "IPC, 2-process", False),
        ("inproc", ["libzmq", "omq-compio", "omq-compio-st", "omq-tokio", "omq-tokio-mt"], "inproc", True),
    ]:
        data = load_data(transport, impls)
        if not data["sizes"]:
            print(f"No {transport} data found", file=sys.stderr)
            continue

        inproc_overrides = {"omq-compio": "omq-compio (MT)"} if transport == "inproc" else None
        svg = generate_chart_cpu(data, impls, label,
                                 fixed_gbs_max=None if log else FIXED_GBS_MAX,
                                 log_gbs=log,
                                 hw_label=hw,
                                 label_overrides=inproc_overrides)
        if svg:
            out = REPO / "doc" / "charts" / "pushpull" / f"omq_{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

        lat_max = FIXED_INPROC_LAT_MAX if transport == "inproc" else FIXED_LAT_MAX
        svg = generate_latency_chart_cpu(data, impls, label,
                                         fixed_lat_max=lat_max, hw_label=hw,
                                         label_overrides=inproc_overrides)
        if svg:
            out = REPO / "doc" / "charts" / "reqrep" / f"omq_{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

    # ── Cross-impl charts (other impls vs slowest omq) ────────
    alt_impls = ["libzmq", "omq-tokio", "zmq.rs", "rzmq"]
    vs_overrides = {
        "omq-tokio": "omq-tokio (ST)",
        "zmq.rs": "zmq.rs v0.6.0 (MT)",
        "rzmq": "rzmq v0.5.18 (MT)",
    }

    for transport, impls, label, log in [
        ("tcp", alt_impls, "TCP loopback, 2-process", False),
        ("ipc", alt_impls, "IPC, 2-process", False),
        ("inproc", ["libzmq", "omq-tokio", "rzmq"], "inproc", True),
    ]:
        data = load_data(transport, impls)
        if not data["sizes"]:
            continue

        svg = generate_chart_cpu(data, impls, label,
                                 fixed_gbs_max=None if log else FIXED_GBS_MAX,
                                 log_gbs=log,
                                 hw_label=hw,
                                 label_overrides=vs_overrides,
                                 show_st_mt=True)
        if svg:
            out = REPO / "doc" / "charts" / "pushpull" / f"alt_{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

        lat_max = FIXED_INPROC_LAT_MAX if transport == "inproc" else FIXED_LAT_MAX
        svg = generate_latency_chart_cpu(data, impls, label,
                                         fixed_lat_max=lat_max, hw_label=hw,
                                         label_overrides=vs_overrides,
                                         show_st_mt=True)
        if svg:
            out = REPO / "doc" / "charts" / "reqrep" / f"alt_{transport}.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)

    # ── PUB/SUB charts ──────────────────────────────────────────
    pubsub_impls = ["libzmq", "omq-compio", "omq-tokio", "omq-tokio-mt"]
    pubsub_alt_impls = ["libzmq", "omq-tokio", "zmq.rs", "rzmq"]
    pubsub_peer_counts = [1, 8, 64]

    def pubsub_title(peers, tl):
        sub_label = "1 subscriber" if peers == 1 else f"{peers} subscribers"
        return f"PUB/SUB throughput, {sub_label}: {tl}"

    for transport, label in [
        ("tcp", "TCP loopback"),
        ("ipc", "IPC"),
    ]:
        panels = [
            (p, load_pubsub_data(transport, pubsub_impls, p))
            for p in pubsub_peer_counts
        ]
        if any(d["sizes"] for _, d in panels):
            svg = generate_multi_panel_cpu_chart(
                panels, pubsub_impls, label,
                hw_label=hw, title_fn=pubsub_title,
            )
            if svg:
                out = REPO / "doc" / "charts" / "pubsub" / f"omq_{transport}.svg"
                out.write_text(svg)
                print(f"Written: {out}", file=sys.stderr)

    # PUB/SUB cross-impl charts
    for transport, label in [
        ("tcp", "TCP loopback"),
    ]:
        panels = [
            (p, load_pubsub_data(transport, pubsub_alt_impls, p))
            for p in pubsub_peer_counts
        ]
        if any(d["sizes"] for _, d in panels):
            svg = generate_multi_panel_cpu_chart(
                panels, pubsub_alt_impls, label,
                hw_label=hw, title_fn=pubsub_title,
                label_overrides=vs_overrides, show_st_mt=True,
            )
            if svg:
                out = REPO / "doc" / "charts" / "pubsub" / f"alt_{transport}.svg"
                out.write_text(svg)
                print(f"Written: {out}", file=sys.stderr)

    # ── Fan-out / fan-in charts (TCP only) ──────────────────────
    fanio_impls = ["libzmq", "omq-compio", "omq-tokio", "omq-tokio-mt"]
    fanio_peers = [2, 4, 8]

    def fanout_title(peers, tl):
        return f"PUSH fan-out (1 PUSH → {peers} PULL): {tl}"

    def fanin_title(peers, tl):
        return f"PUSH fan-in ({peers} PUSH → 1 PULL): {tl}"

    for kind, tfn, dir_name in [
        ("fan_out", fanout_title, "pushpull"),
        ("fan_in", fanin_title, "pushpull"),
    ]:
        panels = [
            (p, load_fanio_data("tcp", fanio_impls, p, kind))
            for p in fanio_peers
        ]
        if not any(d["sizes"] for _, d in panels):
            continue
        svg = generate_multi_panel_cpu_chart(
            panels, fanio_impls, "TCP loopback",
            hw_label=hw,
            title_fn=tfn,
        )
        if svg:
            slug = kind.replace("_", "")
            out = REPO / "doc" / "charts" / dir_name / f"{slug}_tcp.svg"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(svg)
            print(f"Written: {out}", file=sys.stderr)


    # ── Main hero chart (all impls, throughput only) ─────────────
    from gen_main_chart import generate_main_chart, load_data as load_main_data
    tput, lat = load_main_data()
    svg = generate_main_chart(tput, lat, hw)
    if svg:
        out = REPO / "doc" / "charts" / "main_tcp.svg"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
