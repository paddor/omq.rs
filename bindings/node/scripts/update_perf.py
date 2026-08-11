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

DEFAULT_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]
SIZES = DEFAULT_SIZES.copy()
CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "doc", "charts")
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "omq.node")
JSONL_FILE = os.path.join(_CACHE_DIR, "bindings.jsonl")

# Colors: warm = omq-node, cool = zeromq.js
C_OMQ_SYNC = "#dc2626"
C_OMQ_ASYNC = "#f97316"
C_ZMQ = "#2563eb"


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
        "omq_sync_lat": [get_lat("omq-node", "sync", size) for size in SIZES],
        "omq_async_lat": [get_lat("omq-node", "async", size) for size in SIZES],
        "zmq_async_lat": [get_lat("zeromq.js", "async", size) for size in SIZES],
    }


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
    return f"{val:g} µs"


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
    try:
        cpu = None
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                    break
        cores = os.cpu_count()
        if cpu and cores:
            label = f"{cpu}, {cores} cores"
            prefix = os.environ.get("OMQ_HW_PREFIX") or hw_conf.get("prefix")
            postfix = os.environ.get("OMQ_HW_POSTFIX") or hw_conf.get("postfix")
            extras = [e.strip() for e in postfix.split(",")] if postfix else []
            hw_extras = os.environ.get("OMQ_HW_EXTRAS")
            if hw_extras:
                extras.extend(hw_extras.split(","))
            extras = [e.strip() for e in extras if e.strip()]
            if extras:
                label += ", " + ", ".join(extras)
            if prefix:
                label = f"{prefix}, {label}"
            return label
    except OSError:
        pass
    return None


def gen_combined_chart(data, path):
    n = len(SIZES)
    hw_label = _detect_hardware()
    hw_offset = 14 if hw_label else 0
    svg_w = 850
    svg_h = 670 + hw_offset
    x_left, x_right = 90, 760
    plot_w = x_right - x_left

    t1_top = 35 + hw_offset
    t1_bot = 370 + hw_offset
    t1_h = t1_bot - t1_top
    t2_top = t1_bot + 80
    t2_bot = t2_top + 120
    t2_h = t2_bot - t2_top

    xs = [x_left + i * plot_w / max(n - 1, 1) for i in range(n)]
    mid_x = (x_left + x_right) / 2

    omq_sync_tp = data["omq_sync_tp"]
    omq_async_tp = data["omq_async_tp"]
    zmq_async_tp = data["zmq_async_tp"]

    msg_max = 2_000_000
    mbps_max = 5_000

    def y_msg(v):
        return t1_bot - (v / msg_max) * t1_h if msg_max > 0 else t1_bot

    def y_mbps(v):
        return t1_bot - (v / mbps_max) * t1_h if mbps_max > 0 else t1_bot

    lat_max = 200.0
    lat_step = 20

    def y_lat(v):
        return t2_bot - (v / lat_max) * t2_h if lat_max > 0 else t2_bot

    lines = []
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_w} {svg_h}"'
        f' font-family="system-ui, -apple-system, sans-serif">'
    )
    lines.append(f'  <rect width="{svg_w}" height="{svg_h}" fill="white"/>')

    lines.append(
        f'  <text x="{mid_x}" y="{t1_top - 17}" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f"PUSH/PULL throughput: 2-process, TCP loopback (higher is better)</text>"
    )
    if hw_label:
        lines.append(
            f'  <text x="{mid_x}" y="{t1_top - 3}" text-anchor="middle"'
            f' fill="#9ca3af" font-size="10">{hw_label}</text>'
        )

    tick_count = 10
    for i in range(1, tick_count + 1):
        frac = i / tick_count
        msg_val = int(msg_max * frac)
        mbps_val = mbps_max * frac
        yy = t1_bot - frac * t1_h
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151"'
            f' font-size="10">{_fmt_y_rate(msg_val)}</text>'
        )
        lines.append(
            f'  <text x="{x_right + 8}" y="{yy:.1f}" text-anchor="start"'
            f' dominant-baseline="middle" fill="#6b7280"'
            f' font-size="10">{_fmt_mbps(mbps_val)}</text>'
        )

    for x in xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t1_top}" x2="{x:.1f}" y2="{t1_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    lines.append(
        f'  <line x1="{x_left}" y1="{t1_top}" x2="{x_left}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    lines.append(
        f'  <line x1="{x_right}" y1="{t1_top}" x2="{x_right}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    lines.append(
        f'  <line x1="{x_left}" y1="{t1_bot}" x2="{x_right}" y2="{t1_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t1_mid = (t1_top + t1_bot) / 2
    lines.append(
        f'  <text x="40" y="{t1_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t1_mid:.0f})">msg/s</text>'
    )

    tp_series = [
        ("omq-node sync API", C_OMQ_SYNC, omq_sync_tp),
        ("omq-node async API", C_OMQ_ASYNC, omq_async_tp),
        ("zeromq.js async API", C_ZMQ, zmq_async_tp),
    ]

    for _, color, vals in tp_series:
        _draw_series(lines, xs, vals, y_msg, color, stroke_width="2", dash=' stroke-dasharray="6,4"')

    for _, color, vals in tp_series:
        mbps = [v * SIZES[i] / 1e6 for i, v in enumerate(vals)]
        _draw_series(lines, xs, mbps, y_mbps, color, stroke_width="2.5")
        for i, v in enumerate(mbps):
            if v <= 0:
                continue
            yy = y_mbps(v)
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="white" stroke-width="1"/>'
            )

    for i in range(n):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{t1_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(SIZES[i])}</text>'
        )

    lines.append(
        f'  <text x="{mid_x}" y="{t2_top - 17}" text-anchor="middle" fill="#111827"'
        f' font-size="13" font-weight="700">'
        f"REQ/REP latency: 2-process, TCP loopback, p50 µs (lower is better)</text>"
    )

    for i in range(1, 11):
        v = lat_step * i
        yy = y_lat(v)
        lines.append(
            f'  <line x1="{x_left}" y1="{yy:.1f}" x2="{x_right}" y2="{yy:.1f}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )
        lines.append(
            f'  <text x="{x_left - 8}" y="{yy:.1f}" text-anchor="end"'
            f' dominant-baseline="middle" fill="#374151" font-size="10">'
            f"{_fmt_y_us(v)}</text>"
        )

    for x in xs:
        lines.append(
            f'  <line x1="{x:.1f}" y1="{t2_top}" x2="{x:.1f}" y2="{t2_bot}"'
            f' stroke="#e5e7eb" stroke-width="1"/>'
        )

    lines.append(
        f'  <line x1="{x_left}" y1="{t2_top}" x2="{x_left}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )
    lines.append(
        f'  <line x1="{x_left}" y1="{t2_bot}" x2="{x_right}" y2="{t2_bot}"'
        f' stroke="#9ca3af" stroke-width="1.5"/>'
    )

    t2_mid = (t2_top + t2_bot) / 2
    lines.append(
        f'  <text x="40" y="{t2_mid:.0f}" text-anchor="middle"'
        f' dominant-baseline="middle" fill="#374151" font-size="10" font-weight="600"'
        f' transform="rotate(-90,40,{t2_mid:.0f})">p50 latency (µs)</text>'
    )

    lat_series = [
        ("omq-node sync API", C_OMQ_SYNC, data["omq_sync_lat"]),
        ("omq-node async API", C_OMQ_ASYNC, data["omq_async_lat"]),
        ("zeromq.js async API", C_ZMQ, data["zmq_async_lat"]),
    ]

    for _, color, vals in lat_series:
        _draw_series(lines, xs, vals, y_lat, color, stroke_width="2.5")
        for i, v in enumerate(vals):
            if v <= 0:
                continue
            yy = y_lat(v)
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{yy:.1f}" r="3"'
                f' fill="{color}" stroke="white" stroke-width="1"/>'
            )

    for i in range(n):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{t2_bot + 14}" text-anchor="middle"'
            f' fill="#374151" font-size="8.5">{fmt_size(SIZES[i])}</text>'
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
            f'  <text x="{lx + 20:.0f}" y="{leg_y + 4}" fill="#374151"'
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
