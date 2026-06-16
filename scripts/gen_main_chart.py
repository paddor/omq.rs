#!/usr/bin/env python3
"""Generate the 3-panel main chart: doc/charts/main_tcp.svg.

Panel 1: PUSH/PULL throughput (MB/s), small messages (8 B .. 256 B)
Panel 2: PUSH/PULL throughput (MB/s), medium/large messages (512 B .. 32 KiB)
Panel 3: REQ/REP latency (p50 µs), 8 B .. 8 KiB

All 6 implementations, one line per impl, TCP only.
"""

import json
import math
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "comparisons.jsonl"

sys.path.insert(0, str(REPO / "scripts"))
from chart_hw import detect_hardware

IMPLS = ["libzmq", "omq-compio", "omq-tokio", "omq-tokio-mt", "zmq.rs", "rzmq"]

COLORS = {
    "libzmq": "#eab308",
    "omq-compio": "#7c3aed",
    "omq-tokio": "#f97316",
    "omq-tokio-mt": "#dc2626",
    "zmq.rs": "#2563eb",
    "rzmq": "#16a34a",
}

LABELS = {
    "libzmq": "libzmq v4.3.5",
    "omq-compio": "omq-compio",
    "omq-tokio": "omq-tokio (ST)",
    "omq-tokio-mt": "omq-tokio (MT)",
    "zmq.rs": "zmq.rs v0.6.0 (MT)",
    "rzmq": "rzmq v0.5.18 (MT)",
}

DRAW_ORDER = ["rzmq", "zmq.rs", "libzmq", "omq-tokio-mt", "omq-tokio", "omq-compio"]


def fmt_size(b: int) -> str:
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def nice_step(max_val: float, target_lines: int) -> float:
    raw = max_val / target_lines
    mag = 10 ** int(f"{raw:.0e}".split("e")[1])
    for s in [1, 2, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


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


def load_data() -> tuple[dict, dict]:
    """Return (tput, lat) dicts keyed by size -> impl -> value.

    tput[size][impl] = mbps (float)
    lat[size][impl] = p50_us (float)
    """
    rows = load_jsonl()
    tcp = [r for r in rows if r.get("transport") == "tcp"]

    tput: dict[int, dict[str, float]] = {}
    lat: dict[int, dict[str, float]] = {}
    seen_tput: dict[tuple, str] = {}
    seen_lat: dict[tuple, str] = {}

    for r in tcp:
        impl_name = r.get("impl")
        if impl_name not in IMPLS:
            continue
        run_id = r.get("run_id", "")
        size = r.get("msg_size")
        kind = r.get("kind")

        if kind == "throughput":
            key = (impl_name, size)
            if key not in seen_tput or run_id >= seen_tput[key]:
                seen_tput[key] = run_id
                tput.setdefault(size, {})[impl_name] = r.get("mbps", 0)

        elif kind == "latency":
            key = (impl_name, size)
            if key not in seen_lat or run_id >= seen_lat[key]:
                seen_lat[key] = run_id
                lat.setdefault(size, {})[impl_name] = r.get("p50_us", 0)

    return tput, lat


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


# ── panel drawing ────────────────────────────────────────────────

def draw_throughput_panel(
    L: list[str], sizes: list[int], xs: list[float], tput: dict,
    x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str,
):
    h = y_bot - y_top

    all_vals = [
        tput[s][name]
        for s in sizes for name in IMPLS if name in tput.get(s, {})
    ]
    mbps_max = max(all_vals) * 1.15 if all_vals else 1000.0

    def y_mbps(v):
        return y_bot - (v / mbps_max) * h

    L.append(svg_text((x_left + x_right) / 2, y_top - 12, title,
                       size=12, weight="700", fill="#111827"))

    step = nice_step(mbps_max, 6)
    v = step
    while v <= mbps_max:
        yy = y_mbps(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        if v >= 1000:
            label = f"{v / 1000:.1f}" if v % 1000 else f"{int(v / 1000)}"
            label += " GB/s"
        else:
            label = f"{v:.0f} MB/s"
        L.append(svg_text(x_left - 8, yy, label, anchor="end", baseline="middle",
                          size=8.5))
        v += step

    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    for name in DRAW_ORDER:
        pts = [
            (xs[i], y_mbps(tput[sizes[i]][name]))
            for i in range(len(sizes)) if name in tput.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 13, fmt_size(s), size=8))


def draw_latency_panel(
    L: list[str], sizes: list[int], xs: list[float], lat: dict,
    x_left: float, x_right: float, y_top: float, y_bot: float,
    title: str, lat_min: float = 0,
):
    h = y_bot - y_top

    all_vals = [
        lat[s][name]
        for s in sizes for name in IMPLS if name in lat.get(s, {})
    ]
    lat_max = max(all_vals) * 1.15 if all_vals else 150.0
    lat_range = lat_max - lat_min

    def y_lat(v):
        return y_bot - ((v - lat_min) / lat_range) * h

    L.append(svg_text((x_left + x_right) / 2, y_top - 12, title,
                       size=12, weight="700", fill="#111827"))

    if lat_min > 0:
        L.append(svg_text(x_left - 8, y_bot, f"{lat_min:.0f} µs", anchor="end",
                          baseline="middle", size=8.5, fill="#9ca3af"))

    step = nice_step(lat_range, 5)
    v = math.ceil(lat_min / step) * step
    if v <= lat_min:
        v += step
    while v <= lat_max:
        yy = y_lat(v)
        L.append(svg_line(x_left, yy, x_right, yy))
        L.append(svg_text(x_left - 8, yy, f"{v:.0f} µs", anchor="end",
                          baseline="middle", size=8.5))
        v += step

    for x in xs:
        L.append(svg_line(x, y_top, x, y_bot))

    L.append(svg_line(x_left, y_top, x_left, y_bot, stroke="#9ca3af", width=1.5))
    L.append(svg_line(x_left, y_bot, x_right, y_bot, stroke="#9ca3af", width=1.5))

    lat_draw_order = ["libzmq", "omq-tokio-mt", "omq-tokio", "rzmq",
                      "zmq.rs", "omq-compio"]
    for name in lat_draw_order:
        pts = [
            (xs[i], y_lat(lat[sizes[i]][name]))
            for i in range(len(sizes)) if name in lat.get(sizes[i], {})
        ]
        if pts:
            L.append(svg_polyline(pts, COLORS[name]))
            L.extend(svg_dots(pts, COLORS[name]))

    for i, s in enumerate(sizes):
        L.append(svg_text(xs[i], y_bot + 13, fmt_size(s), size=8))


# ── main chart generation ────────────────────────────────────────

def generate_main_chart(tput: dict, lat: dict, hw_label: str | None) -> str:
    small_sizes = [8, 16, 32, 64, 128]
    large_sizes = [128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]

    hw_offset = 14 if hw_label else 0
    panel_h = 260
    x_pad_left = 70
    panel_gap_x = 50
    x_pad_right = 25
    legend_h = 60

    svg_w = 950
    total_w = svg_w - x_pad_left - x_pad_right - panel_gap_x
    p1_w = total_w / 3
    p2_w = total_w * 2 / 3

    header_y = 16
    row_top = hw_offset + header_y + 30
    row_bot = row_top + panel_h
    svg_h = row_bot + legend_h + 10

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w:.0f} {svg_h:.0f}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w:.0f}" height="{svg_h:.0f}" fill="white"/>')

    mid_x = svg_w / 2

    title_y = header_y
    L.append(svg_text(mid_x, title_y, "PUSH/PULL throughput, TCP loopback, 2-process",
                       size=14, weight="700", fill="#111827"))
    if hw_label:
        L.append(svg_text(mid_x, title_y + 14, hw_label, size=9, fill="#9ca3af"))

    def make_xs(sizes, xl, xr):
        n = len(sizes)
        return [xl + i * (xr - xl) / max(n - 1, 1) for i in range(n)]

    p1_xl = x_pad_left
    p1_xr = p1_xl + p1_w
    draw_throughput_panel(L, small_sizes, make_xs(small_sizes, p1_xl, p1_xr),
                          tput, p1_xl, p1_xr, row_top, row_bot,
                          "small messages (higher is better)")

    p2_xl = p1_xr + panel_gap_x
    p2_xr = p2_xl + p2_w
    draw_throughput_panel(L, large_sizes, make_xs(large_sizes, p2_xl, p2_xr),
                          tput, p2_xl, p2_xr, row_top, row_bot,
                          "medium/large messages (higher is better)")

    # Legend
    leg_y = row_bot + 30
    legend_items = [(k, LABELS[k]) for k in IMPLS if k in COLORS]
    item_w = (svg_w - 40) / len(legend_items)
    start_x = 20

    for i, (key, label) in enumerate(legend_items):
        lx = start_x + i * item_w
        c = COLORS[key]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5" stroke-linecap="round"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="10" font-weight="500">{label}</text>'
        )

    abbr_y = leg_y + 18
    L.append(svg_text(mid_x, abbr_y,
                       "ST = single-threaded   MT = multi-threaded",
                       size=9, fill="#9ca3af"))

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    hw = detect_hardware()
    tput, lat = load_data()
    svg = generate_main_chart(tput, lat, hw)
    out = REPO / "doc" / "charts" / "main_tcp.svg"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg)
    print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
