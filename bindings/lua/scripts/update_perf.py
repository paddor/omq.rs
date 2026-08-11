#!/usr/bin/env python3
"""Measure OMQ.lua two-process TCP PUSH/PULL throughput.

Rows are append-only in ~/.cache/omq.lua/bindings.jsonl by default. The SVG
chart uses the latest cached row per implementation and message size.
"""

import argparse
import datetime as dt
import json
import math
import os
import shutil
import subprocess
import sys
import time
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
IMPL = "omq.lua"
COLOR = "#dc2626"


def parse_sizes(value):
    out = []
    for part in value.split(","):
        text = part.strip().lower()
        if text.endswith("k"):
            out.append(int(text[:-1]) * 1024)
        else:
            out.append(int(text))
    return out


def lua_env(profile):
    lib_dir = ROOT / "native" / "target" / profile
    lib_pattern = "?.dylib" if sys.platform == "darwin" else "lib?.so"
    env = os.environ.copy()
    env["LUA_PATH"] = f"{ROOT / 'lua'}/?.lua;;"
    env["LUA_CPATH"] = f"{lib_dir}/{lib_pattern};;"
    return env


def build_native(release):
    cmd = ["cargo", "build", "--manifest-path", str(ROOT / "native" / "Cargo.toml")]
    if release:
        cmd.append("--release")
    subprocess.run(cmd, cwd=REPO, check=True)


def read_line(proc, prefix, timeout=30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if line:
            line = line.strip()
            if line.startswith(prefix):
                return line
            continue
        if proc.poll() is not None:
            raise RuntimeError(proc.stderr.read().strip() or f"peer exited before {prefix}")
        time.sleep(0.01)
    raise TimeoutError(f"timed out waiting for {prefix}")


def run_size(lua_bin, size, duration, warmup, profile):
    env = lua_env(profile)
    peer = ROOT / "scripts" / "bench_peer.lua"
    pull = subprocess.Popen(
        [lua_bin, str(peer), "pull", "tcp://127.0.0.1:*", str(size), str(duration), str(warmup)],
        cwd=REPO,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    push = None
    try:
        ready = read_line(pull, "READY ")
        endpoint = ready.split(" ", 1)[1]
        push = subprocess.Popen(
            [lua_bin, str(peer), "push", endpoint, str(size), str(duration), str(warmup)],
            cwd=REPO,
            env=env,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        result = read_line(pull, "RESULT ", timeout=duration + warmup + 20.0)
        _, messages, seconds = result.split()
        messages = int(messages)
        seconds = float(seconds)
        return {
            "impl": IMPL,
            "kind": "throughput",
            "transport": "tcp",
            "msg_size": size,
            "messages": messages,
            "seconds": seconds,
            "msgs_s": messages / seconds if seconds > 0 else 0.0,
            "gb_s": (messages * size) / seconds / 1_000_000_000 if seconds > 0 else 0.0,
        }
    finally:
        if push is not None and push.poll() is None:
            push.terminate()
            try:
                push.wait(timeout=2)
            except subprocess.TimeoutExpired:
                push.kill()
        if pull.poll() is None:
            pull.terminate()
            try:
                pull.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pull.kill()


def append_rows(rows):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with JSONL.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")


def load_rows():
    rows = []
    try:
        with JSONL.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    return rows


def latest_rows(rows):
    latest = {}
    for row in rows:
        key = (row.get("impl"), row.get("kind"), row.get("transport"), row.get("msg_size"))
        prev = latest.get(key)
        if prev is None or row.get("run_id", "") >= prev.get("run_id", ""):
            latest[key] = row
    return latest


def fmt_size(size):
    return f"{size // 1024} KiB" if size >= 1024 else f"{size} B"


def nice_ceil(value):
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for mul in (1, 2, 5, 10):
        candidate = mul * base
        if candidate >= value:
            return candidate
    return 10 * base


def render_chart(sizes):
    latest = latest_rows(load_rows())
    values = [
        latest.get((IMPL, "throughput", "tcp", size), {}).get("msgs_s", 0.0)
        for size in sizes
    ]
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    width, height = 980, 520
    left, top, right, bottom = 84, 44, 28, 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_y = nice_ceil(max(values) if values else 0)
    if not sizes:
        sizes = DEFAULT_SIZES
        values = [0.0 for _ in sizes]
    step = plot_w / max(1, len(sizes) - 1)

    def x_at(i):
        return left + i * step

    def y_at(v):
        return top + plot_h - (v / max_y) * plot_h

    points = " ".join(f"{x_at(i):.1f},{y_at(v):.1f}" for i, v in enumerate(values))
    y_ticks = []
    for i in range(6):
        val = max_y * i / 5
        y = y_at(val)
        label = f"{val / 1_000_000:g}M" if val >= 1_000_000 else f"{val / 1_000:g}k"
        y_ticks.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width-right}" y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="{left-10}" y="{y+4:.1f}" text-anchor="end" font-size="12" fill="#475569">{label}</text>'
        )
    x_labels = []
    for i, size in enumerate(sizes):
        x = x_at(i)
        x_labels.append(
            f'<text x="{x:.1f}" y="{height-42}" text-anchor="middle" font-size="12" fill="#475569" transform="rotate(-35 {x:.1f} {height-42})">{fmt_size(size)}</text>'
        )
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#ffffff"/>
<text x="{left}" y="28" font-size="22" font-family="ui-sans-serif, system-ui" fill="#0f172a">OMQ.lua TCP PUSH/PULL throughput</text>
<text x="{left}" y="50" font-size="12" font-family="ui-sans-serif, system-ui" fill="#64748b">latest append-only rows from {JSONL}</text>
<g font-family="ui-sans-serif, system-ui">
{''.join(y_ticks)}
<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#94a3b8"/>
<line x1="{left}" y1="{top+plot_h}" x2="{width-right}" y2="{top+plot_h}" stroke="#94a3b8"/>
<polyline fill="none" stroke="{COLOR}" stroke-width="3" points="{points}"/>
{''.join(f'<circle cx="{x_at(i):.1f}" cy="{y_at(v):.1f}" r="4" fill="{COLOR}"><title>{fmt_size(sizes[i])}: {v:,.0f} msg/s</title></circle>' for i, v in enumerate(values))}
{''.join(x_labels)}
<text x="{width/2:.1f}" y="{height-10}" text-anchor="middle" font-size="13" fill="#334155">message size</text>
<text x="20" y="{top+plot_h/2:.1f}" text-anchor="middle" font-size="13" fill="#334155" transform="rotate(-90 20 {top+plot_h/2:.1f})">messages/s</text>
</g>
</svg>
'''
    CHART.write_text(svg, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--sizes")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--warmup-duration", type=float)
    parser.add_argument("--lua", default=os.environ.get("OMQ_LUA", "/usr/bin/lua"))
    parser.add_argument("--chart-only", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--no-chart", action="store_true")
    parser.add_argument("--debug", action="store_true", help="use debug native build")
    args = parser.parse_args()

    sizes = parse_sizes(args.sizes) if args.sizes else (QUICK_SIZES if args.quick else DEFAULT_SIZES)
    duration = args.duration if args.duration is not None else (0.75 if args.quick else 3.0)
    warmup = args.warmup_duration if args.warmup_duration is not None else (0.2 if args.quick else 0.5)
    profile = "debug" if args.debug else "release"

    if args.chart_only:
        render_chart(sizes)
        return
    if shutil.which(args.lua) is None and not Path(args.lua).exists():
        raise SystemExit(f"lua not found: {args.lua}")
    if not args.no_build:
        build_native(release=not args.debug)

    run_id = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rows = []
    for round_no in range(args.rounds):
        for size in sizes:
            row = run_size(args.lua, size, duration, warmup, profile)
            row.update({"run_id": run_id, "round": round_no, "created_at": dt.datetime.now(dt.UTC).isoformat()})
            rows.append(row)
            print(f"{fmt_size(size):>8}: {row['msgs_s']:,.0f} msg/s")
    if not args.no_save:
        append_rows(rows)
    if not args.no_chart:
        render_chart(sizes)


if __name__ == "__main__":
    main()
