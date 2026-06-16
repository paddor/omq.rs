#!/usr/bin/env python3
"""Generate PUB/SUB lz4 compression chart with link-speed projection.

Three panels (1 Gbps, 100 Mbps, 10 Mbps), three axes per panel:
  dotted = sender CPU % (left, linear)
  solid  = aggregate virtual SUB throughput (inner right, log)
  dashed = msg/s (outer right, log)

Produces: doc/charts/pubsub/lz4_tcp.svg
"""

import json
import math
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
from gen_compression_chart import (
    fmt_size, detect_hardware, _log_ticks, _cpu_ticks,
    _fmt_cpu, _fmt_tput, _fmt_msgs,
)

CACHE_DIR = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "omq"
JSONL_PATH = CACHE_DIR / "results_pubsub_lz4.jsonl"

LINK_SPEEDS = [
    ("1g",   1_000_000_000 / 8,  "1 Gbps"),
    ("100m", 100_000_000 / 8,    "100 Mbps"),
    ("10m",  10_000_000 / 8,     "10 Mbps"),
]

COLORS = {
    "tcp":          "#eab308",
    "lz4+tcp":      "#60a5fa",
    "lz4+tcp+dict": "#1d4ed8",
}
LABELS = {
    "tcp":          "tcp (no compression)",
    "lz4+tcp":      "lz4+tcp (no dict)",
    "lz4+tcp+dict": "lz4+tcp + dict",
}
SERIES_ORDER = ["tcp", "lz4+tcp", "lz4+tcp+dict"]


# ── data ──────────────────────────────────────────────────────────

def load_raw_data() -> tuple[dict, int, int]:
    """Returns (raw_data, dict_size, peers)."""
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found", file=sys.stderr)
        sys.exit(1)

    all_rows = []
    for line in JSONL_PATH.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                all_rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    pubsub = [r for r in all_rows
              if r.get("pattern", "").startswith("pubsub_lz4")]
    if not pubsub:
        print("ERROR: no pubsub_lz4 rows", file=sys.stderr)
        sys.exit(1)

    dict_rows = [r for r in pubsub if r.get("dict_size")]
    dict_size = max(r["dict_size"] for r in dict_rows) if dict_rows else 2048
    peers = max((r.get("peers", 32) for r in pubsub), default=32)

    def series_key(r):
        if r["pattern"] == "pubsub_lz4_dict":
            return "lz4+tcp+dict"
        return r["transport"]

    newest: dict[tuple, dict] = {}
    for r in pubsub:
        k = (series_key(r), r["msg_size"])
        if k not in newest or r["run_id"] > newest[k]["run_id"]:
            newest[k] = r

    sizes_set = set()
    series: dict[str, dict[int, dict]] = {}
    for r in newest.values():
        key = series_key(r)
        sz = r["msg_size"]
        sizes_set.add(sz)
        elapsed = r.get("elapsed", 0)
        cpu_time = r.get("cpu_time", 0)
        cpu_pct = (cpu_time / elapsed * 100) if elapsed > 0 else 0
        entry = {
            "msg_size": sz,
            "wire_bytes": r.get("wire_bytes", sz),
        }
        if "msgs_s" in r:
            entry["cpu_msgs_s"] = r["msgs_s"]
            entry["cpu_pct"] = cpu_pct
        series.setdefault(key, {})[sz] = entry

    # Dict series inherits cpu_msgs_s from lz4+tcp
    if "lz4+tcp+dict" in series and "lz4+tcp" in series:
        for sz, d in series["lz4+tcp+dict"].items():
            if "cpu_msgs_s" not in d and sz in series["lz4+tcp"]:
                src = series["lz4+tcp"][sz]
                d["cpu_msgs_s"] = src.get("cpu_msgs_s", 0)
                d["cpu_pct"] = src.get("cpu_pct", 0)

    return {"sizes": sorted(sizes_set), "series": series}, dict_size, peers


def project(raw: dict, link_bytes_s: float, peers: int) -> dict:
    """Project throughput at a link speed."""
    series = {}
    for key, size_data in raw["series"].items():
        series[key] = {}
        for sz, d in size_data.items():
            cpu = d.get("cpu_msgs_s")
            if cpu is None:
                continue
            wire = d["wire_bytes"]
            wire_per_msg = wire * peers
            link_msgs = link_bytes_s / max(wire_per_msg, 1)
            eff_msgs = min(cpu, link_msgs)
            # Aggregate virtual throughput: all subscribers combined
            agg_virt_mbs = eff_msgs * d["msg_size"] * peers / 1_000_000
            ratio = eff_msgs / cpu if cpu > 0 else 0
            series[key][sz] = {
                "msgs_s": eff_msgs,
                "virt_mbps": agg_virt_mbs,
                "cpu_pct": d.get("cpu_pct", 0) * ratio,
            }
    return {"sizes": raw["sizes"], "series": series}


# ── SVG ───────────────────────────────────────────────────────────

def generate_svg(
    panels: dict[str, dict],
    peers: int,
    dict_size_label: str | None = None,
    hw_label: str | None = None,
) -> str:
    links = [tag for tag, _, _ in LINK_SPEEDS if tag in panels]
    if not links:
        return ""

    n_panels = len(links)
    x_left = 90
    x_right = 700
    x_right2 = 780
    plot_w = x_right - x_left
    panel_h = 220
    panel_gap = 70
    top_margin = 44 if hw_label else 30
    x_label_space = 20
    legend_h = 60
    bottom_pad = 40
    right_pad = 15

    svg_h = (top_margin + n_panels * (panel_h + x_label_space)
             + (n_panels - 1) * panel_gap + legend_h + bottom_pad)
    svg_w = x_right2 + 80 + right_pad
    mid_x = (x_left + x_right) / 2

    all_sizes = set()
    for d in panels.values():
        all_sizes.update(d["sizes"])
    sizes = sorted(all_sizes)
    n = len(sizes)
    if n < 2:
        return ""
    xs = [x_left + i * plot_w / (n - 1) for i in range(n)]

    L = []
    L.append(
        f'<svg xmlns="http://www.w3.org/2000/svg"'
        f' viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    L.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    title = (f"PUB/SUB lz4+tcp: structured JSON, 1 PUB → {peers} SUBs"
             f" (omq-tokio)")
    if dict_size_label:
        title += f", dict {dict_size_label}"
    L.append(
        f'  <text x="{mid_x}" y="16" text-anchor="middle" fill="#111827"'
        f' font-size="12" font-weight="700">{title}</text>'
    )
    if hw_label:
        L.append(
            f'  <text x="{mid_x}" y="30" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    last_x_label_y = 0

    for pi, link_tag in enumerate(links):
        data = panels[link_tag]
        series = data["series"]
        y_top = top_margin + pi * (panel_h + x_label_space + panel_gap) + 25
        y_bot = y_top + panel_h
        plot_h = y_bot - y_top

        cpu_data_max = max((d["cpu_pct"] for s in series.values()
                            for d in s.values()), default=100)
        cpu_ceil, cpu_ticks = _cpu_ticks(max(cpu_data_max, 100))

        all_virt = [d["virt_mbps"] for s in series.values()
                    for d in s.values() if d["virt_mbps"] > 0]
        virt_min = min(all_virt) if all_virt else 1
        virt_max = max(all_virt) if all_virt else 100
        tp_min, tp_max, tp_ticks = _log_ticks(
            max(virt_min, 0.01), max(virt_max, 1))

        all_msgs = [d["msgs_s"] for s in series.values()
                    for d in s.values() if d["msgs_s"] > 0]
        msgs_min = min(all_msgs) if all_msgs else 1
        msgs_max = max(all_msgs) if all_msgs else 1000
        ms_min, ms_max, ms_ticks = _log_ticks(
            max(msgs_min, 1), max(msgs_max, 10))

        def y_cpu(v, _bot=y_bot, _h=plot_h, _ceil=cpu_ceil):
            return _bot - max(0, min(1, v / _ceil)) * _h

        def y_tput(v, _bot=y_bot, _h=plot_h, _lo=tp_min, _hi=tp_max):
            if v <= 0:
                return _bot
            frac = max(0, min(1, (math.log10(v) - _lo) / (_hi - _lo)))
            return _bot - frac * _h

        def y_msgs(v, _bot=y_bot, _h=plot_h, _lo=ms_min, _hi=ms_max):
            if v <= 0:
                return _bot
            frac = max(0, min(1, (math.log10(v) - _lo) / (_hi - _lo)))
            return _bot - frac * _h

        link_label = dict(LINK_SPEEDS).get(
            link_tag, {}).get(2, link_tag) if False else \
            next(lb for t, _, lb in LINK_SPEEDS if t == link_tag)
        L.append(
            f'  <text x="{mid_x:.1f}" y="{y_top - 10}"'
            f' text-anchor="middle" fill="#111827"'
            f' font-size="12" font-weight="700">{link_label}</text>'
        )

        # CPU% gridlines (left, linear)
        for val in cpu_ticks:
            yy = y_cpu(val)
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}"'
                f' x2="{x_right}" y2="{yy:.1f}"'
                f' stroke="#e5e7eb" stroke-width="1"/>'
            )
            L.append(
                f'  <text x="{x_left - 8}" y="{yy:.1f}"'
                f' text-anchor="end" dominant-baseline="middle"'
                f' fill="#374151" font-size="9">{_fmt_cpu(val)}</text>'
            )

        # Virtual throughput gridlines (inner right, log)
        for mb in tp_ticks:
            yy = y_tput(mb)
            L.append(
                f'  <line x1="{x_left}" y1="{yy:.1f}"'
                f' x2="{x_right}" y2="{yy:.1f}"'
                f' stroke="#e5e7eb" stroke-width="1"'
                f' stroke-dasharray="3,6"/>'
            )
            L.append(
                f'  <text x="{x_right + 8}" y="{yy:.1f}"'
                f' text-anchor="start" dominant-baseline="middle"'
                f' fill="#6b7280" font-size="9">{_fmt_tput(mb)}</text>'
            )

        # msg/s gridlines (outer right, log)
        for ms in ms_ticks:
            yy = y_msgs(ms)
            L.append(
                f'  <text x="{x_right2 + 8}" y="{yy:.1f}"'
                f' text-anchor="start" dominant-baseline="middle"'
                f' fill="#9ca3af" font-size="9">{_fmt_msgs(ms)}/s</text>'
            )

        # Vertical gridlines
        for x in xs:
            L.append(
                f'  <line x1="{x:.1f}" y1="{y_top}"'
                f' x2="{x:.1f}" y2="{y_bot}"'
                f' stroke="#e5e7eb" stroke-width="1"/>'
            )

        # Axes
        for ax in [
            f'{x_left}" y1="{y_top}" x2="{x_left}" y2="{y_bot}',
            f'{x_right}" y1="{y_top}" x2="{x_right}" y2="{y_bot}',
            f'{x_left}" y1="{y_bot}" x2="{x_right}" y2="{y_bot}',
        ]:
            L.append(
                f'  <line x1="{ax}" stroke="#9ca3af"'
                f' stroke-width="1.5"/>'
            )
        L.append(
            f'  <line x1="{x_right2}" y1="{y_top}"'
            f' x2="{x_right2}" y2="{y_bot}"'
            f' stroke="#d1d5db" stroke-width="1"/>'
        )

        # X-axis labels
        last_x_label_y = y_bot + 14
        for i, s in enumerate(sizes):
            L.append(
                f'  <text x="{xs[i]:.1f}" y="{last_x_label_y}"'
                f' text-anchor="middle" fill="#374151"'
                f' font-size="8">{fmt_size(s)}</text>'
            )

        # CPU% axis title
        mid_y = (y_top + y_bot) / 2
        L.append(
            f'  <text x="40" y="{mid_y:.1f}" text-anchor="middle"'
            f' dominant-baseline="middle" fill="#374151"'
            f' font-size="10" font-weight="600"'
            f' transform="rotate(-90,40,{mid_y:.1f})">sender CPU %</text>'
        )

        present = [k for k in SERIES_ORDER if k in series]

        # Dotted: CPU %
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n)
                      if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_cpu(d[s]['cpu_pct']):.1f}"
                for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none"'
                f' stroke="{COLORS[name]}"'
                f' stroke-width="2" stroke-dasharray="2,3"'
                f' opacity="0.7"/>'
            )

        # Solid: aggregate virtual throughput with dots
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n)
                      if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_tput(d[s]['virt_mbps']):.1f}"
                for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none"'
                f' stroke="{COLORS[name]}"'
                f' stroke-width="2.5" stroke-linecap="round"'
                f' stroke-linejoin="round"/>'
            )
            for i, s in active:
                yy = y_tput(d[s]["virt_mbps"])
                L.append(
                    f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}"'
                    f' r="2.5" fill="{COLORS[name]}"'
                    f' stroke="white" stroke-width="1"/>'
                )

        # Dashed: msg/s
        for name in present:
            d = series[name]
            active = [(i, sizes[i]) for i in range(n)
                      if sizes[i] in d]
            if not active:
                continue
            pts = " ".join(
                f"{xs[i]:.1f},{y_msgs(d[s]['msgs_s']):.1f}"
                for i, s in active
            )
            L.append(
                f'  <polyline points="{pts}" fill="none"'
                f' stroke="{COLORS[name]}"'
                f' stroke-width="1.5" stroke-dasharray="5,3"/>'
            )

    # Legend
    leg_y1 = last_x_label_y + 18
    leg_y2 = leg_y1 + 12
    leg_y3 = leg_y2 + 12

    all_present = []
    for link_tag in links:
        for k in SERIES_ORDER:
            if k in panels[link_tag]["series"] and k not in all_present:
                all_present.append(k)

    item_w = plot_w / max(len(all_present), 1)
    for i, name in enumerate(all_present):
        lx = x_left + i * item_w
        c = COLORS[name]
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y1}"'
            f' x2="{lx + 14:.0f}" y2="{leg_y1}"'
            f' stroke="{c}" stroke-width="1.5"'
            f' stroke-dasharray="2,3" opacity="0.7"/>'
        )
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y2}"'
            f' x2="{lx + 14:.0f}" y2="{leg_y2}"'
            f' stroke="{c}" stroke-width="2.5"/>'
        )
        L.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y3}"'
            f' x2="{lx + 14:.0f}" y2="{leg_y3}"'
            f' stroke="{c}" stroke-width="1.5"'
            f' stroke-dasharray="5,3"/>'
        )
        L.append(
            f'  <text x="{lx + 18:.0f}" y="{leg_y1 + 4}"'
            f' fill="#374151" font-size="10"'
            f' font-weight="500">{LABELS.get(name, name)}</text>'
        )

    footer_y = leg_y3 + 18
    L.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f'dotted = sender CPU % linear (left)'
        f' · solid = aggregate virtual SUB throughput log (inner right)'
        f' · dashed = msg/s log (outer right)</text>'
    )

    L.append("</svg>")
    return "\n".join(L) + "\n"


# ── main ──────────────────────────────────────────────────────────

def main():
    raw, dict_size, peers = load_raw_data()

    ds_label = (f"{dict_size // 1024}K" if dict_size >= 1024
                else f"{dict_size}B")
    LABELS["lz4+tcp+dict"] = f"lz4+tcp + {ds_label} dict"

    hw = detect_hardware()

    panels = {}
    for tag, bps, _ in LINK_SPEEDS:
        panels[tag] = project(raw, bps, peers)

    svg = generate_svg(panels, peers, dict_size_label=ds_label,
                       hw_label=hw)
    if svg:
        out = REPO / "doc" / "charts" / "pubsub" / "lz4_tcp.svg"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(svg)
        print(f"Written: {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
