#!/usr/bin/env python3
"""Generate omq-node sync/async API vs zeromq.js benchmark charts.

Chart style is intentionally kept in sync with bindings/pyomq/scripts/update_perf.py.
The benchmark runner appends rows to ~/.cache/omq.node/bindings.jsonl, then this
script renders doc/charts/bindings.svg under the Node binding.
"""

import argparse
import json
import math
import os
import sys

DEFAULT_SIZES = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
SIZES = DEFAULT_SIZES.copy()
LATENCY_MAX_SIZE = 4096
CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "doc", "charts")
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "omq.node")
JSONL_FILE = os.path.join(_CACHE_DIR, "bindings.jsonl")

# Colors: warm = omq-node, cool = zeromq.js
C_OMQ_SYNC = "#ef4444"
C_OMQ_ASYNC = "#fb923c"
C_ZMQ = "#60a5fa"


def load_jsonl():
    rows = []
    try:
        with open(JSONL_FILE) as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


def _run_id(row):
    return row.get("run_id") or row.get("runId") or row.get("date") or ""


def _kind(row):
    return row.get("kind") or row.get("metric") or ""


def _msg_size(row):
    return row.get("msg_size") or row.get("size") or 0


def _msgs_s(row):
    return row.get("msgs_s") or row.get("msgPerSec") or 0.0


def _p50_us(row):
    return row.get("p50_us") or row.get("p50Us") or row.get("latencyUs") or 0.0


def _mode(row):
    mode = row.get("mode")
    if mode:
        return mode
    return "async" if row.get("impl") == "zeromq.js" else "sync"


def _latest_by_key(rows):
    latest = {}
    for row in rows:
        impl = row.get("impl", "")
        mode = _mode(row)
        kind = _kind(row)
        transport = row.get("transport", "")
        size = _msg_size(row)
        key = (impl, mode, kind, transport, size)
        prev = latest.get(key)
        if prev is None or _run_id(row) >= _run_id(prev):
            latest[key] = row
    return latest


def _latest_complete_rows(rows):
    by_run = {}
    for row in rows:
        impl = row.get("impl", "")
        mode = _mode(row)
        run_id = _run_id(row)
        if not impl or not run_id:
            continue
        key = (_kind(row), row.get("transport", ""), _msg_size(row))
        by_run.setdefault((impl, mode, run_id), {})[key] = row

    latest = {}
    for impl, mode in [("omq-node", "sync"), ("omq-node", "async"), ("zeromq.js", "async")]:
        required = set()
        for size in SIZES:
            required.add(("throughput", "tcp", size))
        for size in latency_sizes_from(SIZES):
            required.add(("latency", "tcp", size))

        complete = [
            (run_id, run_rows)
            for (run_impl, run_mode, run_id), run_rows in by_run.items()
            if run_impl == impl and run_mode == mode and required.issubset(run_rows)
        ]
        if not complete:
            continue

        _, run_rows = max(complete, key=lambda item: item[0])
        for kind, transport, size in required:
            latest[(impl, mode, kind, transport, size)] = run_rows[(kind, transport, size)]
    return latest


def _latest_rows(rows):
    latest = _latest_complete_rows(rows)
    for key, row in _latest_by_key(rows).items():
        latest.setdefault(key, row)
    return latest


def chart_data_from_jsonl():
    latest = _latest_rows(load_jsonl())
    latency_sizes = latency_sizes_from(SIZES)

    def get_tp(impl, mode, transport, size):
        row = latest.get((impl, mode, "throughput", transport, size))
        return _msgs_s(row) if row else 0.0

    def get_lat(impl, mode, size):
        row = latest.get((impl, mode, "latency", "tcp", size))
        return _p50_us(row) if row else 0.0

    return {
        "omq_sync_tp": [get_tp("omq-node", "sync", "tcp", size) for size in SIZES],
        "omq_async_tp": [get_tp("omq-node", "async", "tcp", size) for size in SIZES],
        "zmq_async_tp": [get_tp("zeromq.js", "async", "tcp", size) for size in SIZES],
        "omq_sync_lat": [get_lat("omq-node", "sync", size) for size in latency_sizes],
        "omq_async_lat": [get_lat("omq-node", "async", size) for size in latency_sizes],
        "zmq_async_lat": [get_lat("zeromq.js", "async", size) for size in latency_sizes],
    }


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
    return f"{val:g} μs"


def _fmt_mbps(val):
    if val >= 1000:
        return f"{val / 1000:g} GB/s"
    if val >= 10:
        return f"{val:.0f} MB/s"
    return f"{val:.1f} MB/s"


def _nice_ceil(value):
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for multiple in [1, 2, 5, 10]:
        candidate = multiple * base
        if candidate >= value:
            return candidate
    return 10 * base


def _read_chart_hw():
    config = {}
    path = os.path.join(REPO_ROOT, ".chart_hw")
    try:
        with open(path) as f:
            for line in f:
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
    n = len(SIZES)
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
    small_indices = [SIZES.index(s) for s in small_sizes]
    large_indices = [SIZES.index(s) for s in large_sizes]
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

    omq_sync_tp = data["omq_sync_tp"]
    omq_async_tp = data["omq_async_tp"]
    zmq_async_tp = data["zmq_async_tp"]

    tp_values = [omq_sync_tp, omq_async_tp, zmq_async_tp]
    msg_max = 2_000_000
    gbs_values = [
        values[i] * SIZES[i] / 1_000_000_000
        for values in tp_values
        for i in large_indices
    ]
    gbs_max = max(1, math.ceil(max(gbs_values, default=0)))

    def y_msg(v):
        return t1_bot - (v / msg_max) * t1_h if msg_max > 0 else t1_bot

    def y_gbs(v):
        return t1_bot - (v / gbs_max) * t1_h if gbs_max > 0 else t1_bot

    lat_max = 200.0
    lat_step = 20

    def y_lat(v):
        return t2_bot - (v / lat_max) * t2_h if lat_max > 0 else t2_bot

    lines = []
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    lines.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="#000000"/>')

    lines.append(
        f'  <text x="{mid_x}" y="{t1_top - 65}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>"
    )
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{t1_top - 51}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    for panel_left, panel_right, panel_xs, ticks, panel_max, formatter, label_x in (
        (
            top_left,
            top_mid,
            small_xs,
            [msg_max * i / 10 for i in range(1, 11)],
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
            lambda value: f"{value:g} GB/s",
            top_right + 8,
        ),
    ):
        for tick in ticks:
            yy = t1_bot - (tick / panel_max) * t1_h
            lines.append(f'  <line x1="{panel_left}" y1="{yy:.1f}" x2="{panel_right}" y2="{yy:.1f}" stroke="#374151" stroke-width="1"/>')
            anchor = "end" if label_x < panel_left else "start"
            lines.append(f'  <text x="{label_x}" y="{yy:.1f}" text-anchor="{anchor}" dominant-baseline="middle" fill="#e5e7eb" font-size="10">{formatter(tick)}</text>')
        for x in panel_xs:
            lines.append(f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}" stroke="#374151" stroke-width="1"/>')
        if panel_left == top_left:
            lines.append(f'  <line x1="{panel_left}" y1="{t1_top}" x2="{panel_left}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>')
        if panel_right == top_right:
            lines.append(f'  <line x1="{panel_right}" y1="{t1_top}" x2="{panel_right}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>')
        lines.append(f'  <line x1="{panel_left}" y1="{t1_bot}" x2="{panel_right}" y2="{t1_bot}" stroke="#9ca3af" stroke-width="1.5"/>')

    lines.append(f'  <text x="{(top_left + top_mid) / 2:.1f}" y="{t1_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">small messages</text>')
    lines.append(f'  <text x="{(top_right_left + top_right) / 2:.1f}" y="{t1_top - 17}" text-anchor="middle" fill="#f9fafb" font-size="12" font-weight="700">medium/large messages</text>')

    tp_series = [
        ("omq-node sync API", C_OMQ_SYNC, omq_sync_tp),
        ("omq-node async API", C_OMQ_ASYNC, omq_async_tp),
        ("zeromq.js async API", C_ZMQ, zmq_async_tp),
    ]

    for _, color, vals in tp_series:
        small_vals = [vals[i] for i in small_indices]
        _draw_series(lines, small_xs, small_vals, y_msg, color, stroke_width="2", dash=' stroke-dasharray="6,4"')

    for _, color, vals in tp_series:
        gbs = [vals[i] * SIZES[i] / 1e9 for i in large_indices]
        _draw_series(lines, large_xs, gbs, y_gbs, color, stroke_width="2.5")
        for i, v in enumerate(gbs):
            if v <= 0:
                continue
            yy = y_gbs(v)
            lines.append(
                f'  <circle cx="{large_xs[i]:.1f}" cy="{yy:.1f}" r="3"'
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
    lines.append(f'  <text x="{mid_x:.1f}" y="{t1_bot + 32}" text-anchor="middle" fill="#9ca3af" font-size="9">dashed = message rate · solid = bandwidth</text>')

    lines.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#f9fafb"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 μs (lower is better)</text>"
    )

    for i in range(1, 11):
        v = lat_step * i
        yy = y_lat(v)
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#374151" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#e5e7eb" font-size="10">'
            f"{_fmt_y_us(v)}</text>"
        )

    for x in lat_xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#374151" stroke-width="1"/>'
        )

    lines.append(
        f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    lines.append(
        f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    lat_series = [
        ("omq-node sync API", C_OMQ_SYNC, data["omq_sync_lat"]),
        ("omq-node async API", C_OMQ_ASYNC, data["omq_async_lat"]),
        ("zeromq.js async API", C_ZMQ, data["zmq_async_lat"]),
    ]

    for _, color, vals in lat_series:
        _draw_series(lines, lat_xs, vals, y_lat, color, stroke_width="2.5")
        for i, v in enumerate(vals):
            if v <= 0:
                continue
            yy = y_lat(v)
            lines.append(
                f'  <circle cx="{lat_xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="#000000" stroke-width="1"/>'
            )

    for i in range(lat_n):
        lines.append(
            f'  <text x="{lat_xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#e5e7eb" font-size="8.5">{fmt_size(latency_sizes[i])}</text>'
        )

    leg_y = t2_bot + 40
    legend_items = [
        ("omq-node sync API", C_OMQ_SYNC),
        ("omq-node async API", C_OMQ_ASYNC),
        ("zeromq.js async API", C_ZMQ),
    ]
    item_w = 180
    total_w = len(legend_items) * item_w
    start_x = mid_x - total_w / 2

    for idx, (label, color) in enumerate(legend_items):
        lx = start_x + idx * item_w
        lines.append(
            f'  <line x1="{lx:.0f}" y1="{leg_y}" x2="{lx + 14:.0f}" y2="{leg_y}"'
            f' stroke="{color}" stroke-width="2.5"/>'
        )
        lines.append(f'  <circle cx="{lx + 7:.0f}" cy="{leg_y}" r="2.5" fill="{color}"/>')
        lines.append(
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#e5e7eb"'
            f' font-size="11" font-weight="500">{label}</text>'
        )

    footer_y = leg_y + 18
    lines.append(
        f'  <text x="{mid_x:.1f}" y="{footer_y}" text-anchor="middle"'
        f' fill="#9ca3af" font-size="9">'
        f"dashed = msg/s (left) · solid = throughput (right)</text>"
    )

    lines.append("</svg>")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines))
        f.write("\n")
    print(f"  wrote {path}")


def _draw_series(lines, xs, vals, y_func, color, stroke_width, dash=""):
    segment = []
    for i, value in enumerate(vals):
        if value > 0:
            segment.append(f"{xs[i]:.1f},{y_func(value):.1f}")
            continue
        if segment:
            _append_polyline(lines, segment, color, stroke_width, dash)
            segment = []
    if segment:
        _append_polyline(lines, segment, color, stroke_width, dash)


def _append_polyline(lines, points, color, stroke_width, dash):
    pts = " ".join(points)
    lines.append(
        f'  <polyline points="{pts}" fill="none" stroke="{color}"'
        f' stroke-width="{stroke_width}"{dash} stroke-linecap="round" stroke-linejoin="round"/>'
    )


def parse_sizes(value):
    sizes = []
    for item in value.split(","):
        item = item.strip().lower()
        if not item:
            continue
        multiplier = 1
        if item.endswith("k"):
            multiplier = 1024
            item = item[:-1]
        try:
            size = int(item) * multiplier
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"invalid size {item!r}") from error
        if size <= 0:
            raise argparse.ArgumentTypeError("message sizes must be positive")
        sizes.append(size)
    if not sizes:
        raise argparse.ArgumentTypeError("at least one message size required")
    return sizes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chart-only", action="store_true", help="regenerate SVG from existing JSONL")
    parser.add_argument("--sizes", type=parse_sizes, help="comma-separated message sizes, e.g. 8,128,1k,32k")
    args = parser.parse_args()

    global SIZES
    if args.sizes is not None:
        SIZES = args.sizes

    if not args.chart_only:
        print("Only chart generation lives here. Run `node scripts/omq-node-bench.js` for benchmarks.", file=sys.stderr)
        sys.exit(2)

    gen_combined_chart(chart_data_from_jsonl(), os.path.join(CHART_DIR, "bindings.svg"))


if __name__ == "__main__":
    main()
