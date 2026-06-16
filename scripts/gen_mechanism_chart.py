#!/usr/bin/env python3
"""Generate doc/charts/mechanism/{compio,tokio}.svg from bench JSONL data."""

import json
import math
import os
import sys
from pathlib import Path


def fmt_size(b: int) -> str:
    if b >= 1024 * 1024:
        return f"{b // (1024*1024)} MiB"
    if b >= 1024:
        return f"{b // 1024} KiB"
    return f"{b} B"


def _fmt_y_rate(val):
    if val >= 1_000_000:
        return f"{val / 1_000_000:g}M"
    if val >= 1_000:
        return f"{val / 1_000:g}k"
    return f"{val:g}"


def _nice_ticks(max_val, target_count=6) -> list[float]:
    raw = max_val / target_count
    mag = 10 ** math.floor(math.log10(raw))
    for step in [1, 2, 5, 10]:
        s = step * mag
        if max_val / s <= target_count + 1:
            return [s * i for i in range(1, int(max_val / s) + 1)]
    return [max_val]


def detect_hardware() -> str | None:
    from chart_hw import detect_hardware as _detect
    return _detect()


def load_data(jsonl: Path) -> dict:
    rows = []
    for line in jsonl.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("pattern") == "mechanism":
            rows.append(r)

    if not rows:
        return {"sizes": [], "series": {}}

    latest: dict[tuple[str, int], dict] = {}
    for r in rows:
        key = (r["transport"], r["msg_size"])
        latest[key] = r

    mechanisms = ["PLAIN", "CURVE", "BLAKE3ZMQ"]
    all_sizes = sorted({k[1] for k in latest})
    sizes = [s for s in all_sizes if all((m, s) in latest for m in mechanisms)]

    size_filter = os.environ.get("OMQ_CHART_SIZES")
    if size_filter:
        allowed = {int(x) for x in size_filter.split(",") if x.strip()}
        sizes = [s for s in sizes if s in allowed]

    series: dict[str, list[tuple[float, float]]] = {m: [] for m in mechanisms}
    for s in sizes:
        for m in mechanisms:
            r = latest[(m, s)]
            gbs = r["mbps"] / 1000.0
            msgs = r["msgs_s"]
            series[m].append((msgs, gbs))

    return {"sizes": sizes, "series": series}


def generate_svg(data: dict, backend: str, *, axis_limits=None) -> str:
    sizes = data["sizes"]
    series = data["series"]
    n = len(sizes)

    hw_label = detect_hardware()
    hw_offset = 14 if hw_label else 0

    x_left, x_right = 90, 760
    y_top = 49 + hw_offset
    y_bot = 370 + hw_offset
    svg_h = 460 + hw_offset
    plot_w = x_right - x_left
    plot_h = y_bot - y_top
    mid_x = (x_left + x_right) / 2

    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    if axis_limits:
        msg_lo, msg_hi, gbs_lo, gbs_hi = axis_limits
    else:
        all_msgs = [pt[0] for pts in series.values() for pt in pts if pt[0] > 0]
        all_gbs = [pt[1] for pts in series.values() for pt in pts if pt[1] > 0]
        msg_lo = math.floor(math.log10(min(all_msgs) * 0.8))
        msg_hi = math.ceil(math.log10(max(all_msgs) * 1.15))
        gbs_lo = math.floor(math.log10(min(all_gbs) * 0.8))
        gbs_hi = math.ceil(math.log10(max(all_gbs) * 1.15))

    def y_msg(v):
        if v <= 0:
            return y_bot
        frac = (math.log10(v) - msg_lo) / (msg_hi - msg_lo)
        return y_bot - frac * plot_h

    def y_tput(v):
        if v <= 0:
            return y_bot
        frac = (math.log10(v) - gbs_lo) / (gbs_hi - gbs_lo)
        return y_bot - frac * plot_h

    colors = {
        "PLAIN": "#374151",
        "CURVE": "#dc2626",
        "BLAKE3ZMQ": "#2563eb",
    }
    order = ["PLAIN", "CURVE", "BLAKE3ZMQ"]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 850 {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="850" height="{svg_h}" fill="white"/>')

    # Title
    L.append(
        f'  <text x="{mid_x:.1f}" y="{32 + hw_offset:.1f}" text-anchor="middle"'
        f' fill="#111827" font-size="13" font-weight="700">'
        f'PUSH/PULL throughput: mechanism overhead, TCP loopback'
        f' (omq-{backend}, higher is better)</text>'
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x:.1f}" y="{y_top - 3:.1f}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    # Left axis: msg/s log scale
    for decade in range(msg_lo, msg_hi + 1):
        base = 10 ** decade
        for mult in [1, 2, 5]:
            v = base * mult
            if v < 10 ** msg_lo or v > 10 ** msg_hi:
                continue
            yy = y_msg(v)
            is_decade = mult == 1
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}"'
                f' y2="{yy:.1f}" stroke="#e5e7eb" stroke-width="1"'
                f'{" stroke-dasharray=\"2,8\"" if not is_decade else ""}/>'
            )
            L.append(
                f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
                f' dominant-baseline="middle"'
                f' fill="{"#374151" if is_decade else "#9ca3af"}"'
                f' font-size="{"10" if is_decade else "9"}">'
                f'{_fmt_y_rate(v)}</text>'
            )

    # Right axis: throughput log scale
    for decade in range(gbs_lo, gbs_hi + 1):
        base = 10 ** decade
        for mult in [1, 2, 5]:
            v = base * mult
            if v < 10 ** gbs_lo or v > 10 ** gbs_hi:
                continue
            yy = y_tput(v)
            is_decade = mult == 1
            label = f"{v:g}" if v >= 1 else f"{v:.3g}"
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}"'
                f' y2="{yy:.1f}" stroke="#e5e7eb" stroke-width="1"'
                f' stroke-dasharray="{"3,6" if is_decade else "2,8"}"/>'
            )
            L.append(
                f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
                f' dominant-baseline="middle"'
                f' fill="{"#6b7280" if is_decade else "#9ca3af"}"'
                f' font-size="{"10" if is_decade else "9"}">'
                f'{label} GB/s</text>'
            )

    # Vertical gridlines
    for x in xs:
        L.append(
            f'  <line x1="{x:.1f}" y1="{y_top}" x2="{x:.1f}" y2="{y_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    # Axes
    L.append(
        f'  <line x1="{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_right}" y1="{y_top}" x2="{x_right}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    L.append(
        f'  <line x1="{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    # X-axis labels
    for i, s in enumerate(sizes):
        L.append(
            f'  <text x="{xs[i]:.1f}" y="{y_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(s)}</text>'
        )

    # Left axis title
    mid_y = (y_top + y_bot) / 2
    L.append(
        f'  <text x="40" y="{mid_y:.1f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{mid_y:.1f})">msg/s</text>'
    )

    # Dashed msg/s lines
    for name in order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_msg(series[name][i][0]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2" stroke-dasharray="6,4"/>'
        )

    # Solid throughput lines with dots
    for name in order:
        pts = " ".join(
            f"{xs[i]:.1f},{y_tput(series[name][i][1]):.1f}" for i in range(n)
        )
        L.append(
            f'  <polyline points="{pts}" fill="none" stroke="{colors[name]}"'
            f' stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'
        )
        for i in range(n):
            yy = y_tput(series[name][i][1])
            L.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{colors[name]}" stroke="white" stroke-width="1"/>'
            )

    # Mechanism legend (colored lines with dots)
    leg_y = y_bot + 40
    item_w = 140
    total_w = len(order) * item_w
    start_x = mid_x - total_w / 2

    for i, name in enumerate(order):
        lx = start_x + i * item_w
        c = colors[name]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{c}"/>')
        L.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
            f' font-size="11" font-weight="500">{name}</text>'
        )

    # Line-type legend (dashed = msg/s, solid = GB/s)
    lt_y = leg_y + 22
    lt_total = 340
    lt_start = mid_x - lt_total / 2

    L.append(
        f'  <line x1="{lt_start:.0f}" y1="{lt_y}" x2="{lt_start + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2" stroke-dasharray="6,4"/>'
    )
    L.append(
        f'  <text x="{lt_start + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">msg/s (left axis, log)</text>'
    )

    lt_right = lt_start + 180
    L.append(
        f'  <line x1="{lt_right:.0f}" y1="{lt_y}" x2="{lt_right + 20:.0f}" y2="{lt_y}"'
        f' stroke="#6b7280" stroke-width="2"/>'
    )
    L.append(f'  <circle cx="{lt_right + 10:.0f}" cy="{lt_y}" r="2" fill="#6b7280"/>')
    L.append(
        f'  <text x="{lt_right + 26:.0f}" y="{lt_y + 4}" fill="#6b7280"'
        f' font-size="10">throughput / GB/s (right axis, log)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    cache_dir = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
    repo = Path(__file__).resolve().parent.parent
    out_dir = repo / "doc" / "charts" / "mechanism"
    out_dir.mkdir(parents=True, exist_ok=True)

    backends = sys.argv[1:] if len(sys.argv) > 1 else ["compio", "tokio"]

    all_data = {}
    for backend in backends:
        jsonl = cache_dir / f"results_{backend}.jsonl"
        if not jsonl.exists():
            print(f"SKIP: {jsonl} not found", file=sys.stderr)
            continue

        data = load_data(jsonl)
        if not data["sizes"]:
            print(f"SKIP: no mechanism data in {jsonl.name}. Run: "
                  f"cargo bench -p omq-{backend} --bench mechanism "
                  f"--features 'plain curve blake3zmq'", file=sys.stderr)
            continue
        all_data[backend] = data

    axis_limits = None
    if len(all_data) > 1:
        all_msgs = [pt[0] for d in all_data.values()
                     for pts in d["series"].values() for pt in pts if pt[0] > 0]
        all_gbs = [pt[1] for d in all_data.values()
                    for pts in d["series"].values() for pt in pts if pt[1] > 0]
        axis_limits = (
            math.floor(math.log10(min(all_msgs) * 0.8)),
            math.ceil(math.log10(max(all_msgs) * 1.15)),
            math.floor(math.log10(min(all_gbs) * 0.8)),
            math.ceil(math.log10(max(all_gbs) * 1.15)),
        )

    for backend, data in all_data.items():
        svg = generate_svg(data, backend, axis_limits=axis_limits)
        output = out_dir / f"{backend}.svg"
        output.write_text(svg)
        print(f"Written: {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
