#!/usr/bin/env python3
"""Generate SPSC comparison SVG chart from benchmark results."""

import math
import os
import sys
from pathlib import Path

YRING_DIR = Path(__file__).resolve().parent.parent

CHANNEL_ORDER = [
    "yring (batch=1)",
    "yring (batch=64)",
    "rtrb per-item",
    "rtrb chunked",
    "crossbeam bounded",
    "kanal bounded sync",
]

COLORS = {
    "yring (batch=1)":    ("#e85d04", "#b34803"),
    "yring (batch=64)":   ("#dc2626", "#a81e1e"),
    "rtrb per-item":      ("#2563eb", "#1d4ed8"),
    "rtrb chunked":       ("#7c3aed", "#6d28d9"),
    "crossbeam bounded":  ("#525c68", "#3d454f"),
    "kanal bounded sync": ("#0891b2", "#0e7490"),
}

LABELS = {
    "yring (batch=1)":    "yring (per-item, no batching)",
    "yring (batch=64)":   "yring (batch=64)",
    "rtrb per-item":      "rtrb v0.3 (per-item)",
    "rtrb chunked":       "rtrb v0.3 (chunk API, batch=64)",
    "crossbeam bounded":  "crossbeam-channel v0.5 (bounded MPMC)",
    "kanal bounded sync": "kanal v0.1 (bounded sync MPMC)",
}

RESULTS = {
    "[u8; 32]": {
        "yring (batch=1)": 267.7,
        "yring (batch=64)": 365.7,
        "rtrb per-item": 31.4,
        "rtrb chunked": 625.0,
        "crossbeam bounded": 14.0,
        "kanal bounded sync": 5.8,
    },
    "[u8; 64]": {
        "yring (batch=1)": 98.4,
        "yring (batch=64)": 243.2,
        "rtrb per-item": 32.2,
        "rtrb chunked": 240.4,
        "crossbeam bounded": 15.3,
        "kanal bounded sync": 2.7,
    },
    "[u8; 128]": {
        "yring (batch=1)": 62.3,
        "yring (batch=64)": 137.6,
        "rtrb per-item": 32.3,
        "rtrb chunked": 172.0,
        "crossbeam bounded": 14.0,
        "kanal bounded sync": 0.7,
    },
}

HW_LABEL = "Linux VM, i7-8700B @ 3.20 GHz, 6 cores, performance governor, turbo off"

GROUPS = list(RESULTS.keys())


def nice_step(max_val, target_lines):
    raw = max_val / target_lines
    mag = 10 ** int(f"{raw:.0e}".split("e")[1])
    for s in [1, 2, 5, 10]:
        step = s * mag
        if max_val / step <= target_lines + 1:
            return step
    return mag * 10


def generate_chart():
    n_groups = len(GROUPS)
    n_bars = len(CHANNEL_ORDER)

    svg_w = 700
    x_left, x_right = 70, 680
    plot_w = x_right - x_left

    top_margin = 55
    plot_h = 250
    p_top = top_margin
    p_bot = p_top + plot_h

    all_vals = [v for g in RESULTS.values() for v in g.values()]
    y_max = max(all_vals) * 1.15

    def y(v):
        return p_bot - (v / y_max) * plot_h

    group_w = plot_w / n_groups
    bar_w = min(group_w * 0.7 / n_bars, 50)
    inner_gap = bar_w * 0.15
    total_bars_w = n_bars * bar_w + (n_bars - 1) * inner_gap

    mid_x = svg_w / 2

    legend_items = [(k, LABELS[k]) for k in CHANNEL_ORDER]
    leg_row_h = 18
    leg_cols = 2
    leg_rows = math.ceil(len(legend_items) / leg_cols)

    svg_h = int(p_bot + 40 + leg_rows * leg_row_h + 20)

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="#0d1117"/>')

    # title
    L.append(
        f'  <text x="{mid_x}" y="22" text-anchor="middle" fill="#e6edf3"'
        f' font-size="14" font-weight="700">'
        f'SPSC channel throughput (M items/s, higher is better)'
        f'</text>'
    )
    # subtitle
    L.append(
        f'  <text x="{mid_x}" y="38" text-anchor="middle" fill="#7d8590"'
        f' font-size="10">cap=1024, 2s per config. {HW_LABEL}</text>'
    )

    # y-axis label
    y_mid = (p_top + p_bot) / 2
    L.append(
        f'  <text x="22" y="{y_mid}" text-anchor="middle" fill="#e6edf3"'
        f' font-size="11" font-weight="600"'
        f' transform="rotate(-90,22,{y_mid})">M items/s</text>'
    )

    # gridlines
    step = nice_step(y_max, 6)
    v = step
    while v <= y_max:
        yy = y(v)
        L.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#21262d" stroke-width="1"/>'
        )
        L.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#7d8590" font-size="10">'
            f'{v:.0f}</text>'
        )
        v += step

    # baseline
    L.append(
        f'  <line x1="{x_left}" y1="{p_bot}" x2="{x_right}" y2="{p_bot}"'
        f' stroke="#30363d" stroke-width="1.5"/>'
    )

    # bars
    for gi, group in enumerate(GROUPS):
        gx = x_left + gi * group_w
        bar_start = gx + (group_w - total_bars_w) / 2

        for bi, channel in enumerate(CHANNEL_ORDER):
            val = RESULTS[group][channel]
            main_c, _ = COLORS[channel]
            bx = bar_start + bi * (bar_w + inner_gap)
            bh = (val / y_max) * plot_h
            by = p_bot - bh

            L.append(
                f'  <rect x="{bx:.1f}" y="{by:.1f}"'
                f' width="{bar_w:.1f}" height="{bh:.1f}"'
                f' fill="{main_c}" rx="1"/>'
            )

            # value label above bar
            label_y = by - 5
            if val >= 100:
                label = f"{val:.0f}"
            else:
                label = f"{val:.1f}"
            L.append(
                f'  <text x="{bx + bar_w / 2:.1f}" y="{label_y:.1f}"'
                f' text-anchor="middle" fill="#e6edf3" font-size="8"'
                f' font-weight="600">{label}</text>'
            )

        # group label
        gcx = gx + group_w / 2
        L.append(
            f'  <text x="{gcx:.1f}" y="{p_bot + 16}" text-anchor="middle"'
            f' fill="#e6edf3" font-size="11" font-weight="600">{group}</text>'
        )

    # legend
    leg_y = p_bot + 38
    leg_col_x = [mid_x - 220, mid_x + 30]
    for i, (key, label) in enumerate(legend_items):
        col = i // leg_rows
        row = i % leg_rows
        if col >= leg_cols:
            break
        lx = leg_col_x[col]
        ly = leg_y + row * leg_row_h
        main_c, _ = COLORS[key]
        L.append(
            f'  <rect x="{lx:.0f}" y="{ly - 5}" width="12" height="12"'
            f' fill="{main_c}" rx="2"/>'
        )
        L.append(
            f'  <text x="{lx + 18:.0f}" y="{ly + 5}" fill="#e6edf3"'
            f' font-size="10" font-weight="500">{label}</text>'
        )

    L.append("</svg>")
    return "\n".join(L) + "\n"


def main():
    svg = generate_chart()
    out = YRING_DIR / "doc" / "spsc_comparison.svg"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg)
    print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
