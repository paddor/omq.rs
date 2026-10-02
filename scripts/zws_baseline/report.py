#!/usr/bin/env python3
"""Summarize raw runs or conservatively gate matched reference/candidate pairs."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import median

from run import case_id, cases


def summarize(rows):
    print("| Case | Build | Runs | Metric | Median | Min | Max |")
    print("|---|---|---:|---|---:|---:|---:|")
    groups = defaultdict(list)
    for row in rows:
        for metric in metrics(row):
            groups[row["case"], row["label"], metric].append(row[metric])
    for (case, label, metric), values in sorted(groups.items()):
        print(f"| {case} | {label} | {len(values)} | {metric} | "
              f"{median(values):.6g} | {min(values):.6g} | {max(values):.6g} |")


def metrics(row):
    return ("p50_us", "p99_us") if row["pattern"] == "latency" else ("payload_gb_s",)


def classify(ratios, latency, regression_percent=10):
    if len(ratios) < 5:
        return "INCOMPLETE"
    regression = regression_percent / 100
    threshold = 1 + regression if latency else 1 - regression
    passing = [r <= threshold if latency else r >= threshold for r in ratios]
    if all(passing):
        return "PASS"
    if not any(passing):
        return "FAIL"
    return "INCONCLUSIVE"


def compare(rows, labels, expected, regression_percent):
    indexed = {}
    hashes = defaultdict(set)
    for row in rows:
        if row["label"] not in labels:
            continue
        key = row["case"], row["label"], row["round"]
        if key in indexed:
            raise ValueError(f"duplicate result {key}; use one matched experiment")
        indexed[key] = row
        hashes[row["label"]].add(row["sha256"])
    if any(len(values) != 1 for values in hashes.values()):
        raise ValueError("a label names multiple executable hashes")
    print("| Case | Metric | Pairs | Median ratio | Min ratio | Max ratio | Gate |")
    print("|---|---|---:|---:|---:|---:|---|")
    failed = False
    for case in expected:
        name = case_id(case)
        rounds = sorted({r for c, _, r in indexed if c == name})
        for metric in metrics(case):
            ratios = []
            missing = False
            for round_number in rounds:
                pair = [indexed.get((name, label, round_number)) for label in labels]
                if any(row is None for row in pair):
                    missing = True
                    continue
                baseline, candidate = pair
                for setting in ("duration", "iterations", "cpu_masks", "io_threads", "profile"):
                    if baseline[setting] != candidate[setting]:
                        raise ValueError(f"unmatched {setting}: {name}")
                if baseline["profile"] is not None:
                    raise ValueError("profiled results cannot pass the performance gate")
                ratios.append(candidate[metric] / baseline[metric])
            status = ("INCOMPLETE" if missing else
                      classify(ratios, metric.endswith("_us"), regression_percent))
            failed |= status != "PASS"
            spread = (f"{median(ratios):.4f} | {min(ratios):.4f} | {max(ratios):.4f}"
                      if ratios else "- | - | -")
            print(f"| {name} | {metric} | {len(ratios)} | {spread} | {status} |")
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--compare", nargs=2, metavar=("REFERENCE", "CANDIDATE"))
    parser.add_argument("--suite", default="all", choices=("all", "core", "scaling", "latency", "codec"))
    parser.add_argument("--case", default="")
    parser.add_argument("--regression-percent", type=float, default=10,
                        help="maximum per-case regression (default: 10; historical: 5)")
    args = parser.parse_args()
    if not 0 <= args.regression_percent < 100:
        parser.error("--regression-percent must be between 0 (inclusive) and 100")
    expected = [c for c in cases() if (args.suite == "all" or c["suite"] == args.suite)
                and args.case in case_id(c)]
    names = {case_id(c) for c in expected}
    rows = [r for line in args.input.read_text().splitlines()
            if (r := json.loads(line))["case"] in names]
    if not rows:
        parser.error("no matching measurements")
    if args.compare:
        raise SystemExit(compare(rows, args.compare, expected, args.regression_percent))
    summarize(rows)


if __name__ == "__main__":
    main()
