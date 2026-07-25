"""Shared helpers for pyomq soak tests."""

import os
import time
from typing import List, Tuple


def soak_duration() -> float:
    return float(os.environ.get("OMQ_SOAK_DURATION_SECS", "120"))


def tcp_ep() -> str:
    return "tcp://127.0.0.1:0"


_inproc_counter = 0


def inproc_ep(label: str) -> str:
    global _inproc_counter
    _inproc_counter += 1
    return f"inproc://soak-{label}-{_inproc_counter}-{os.getpid()}"


def read_rss_bytes() -> int:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * 4096
    except OSError:
        return 0


class ResourceMonitor:
    """Sample RSS once per second from a background thread."""

    def __init__(self):
        import threading

        self._samples: List[Tuple[float, int]] = []
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop:
            self._samples.append((time.monotonic(), read_rss_bytes()))
            time.sleep(1)

    def stop(self) -> "ResourceReport":
        self._stop = True
        self._thread.join(timeout=5)
        return ResourceReport(self._samples)


class ResourceReport:
    def __init__(self, samples: List[Tuple[float, int]]):
        self.samples = samples

    def assert_no_leak(self, label: str):
        n = len(self.samples)
        if n < 10:
            print(f"[{label}] too few RSS samples ({n}) to check for leaks")
            return

        warmup = n // 5
        post = self.samples[warmup:]

        bl_end = max(len(post) // 10, 1)
        baseline = sum(v for _, v in post[:bl_end]) // bl_end

        tail_start = len(post) * 4 // 5
        tail = post[tail_start:]
        tail_max = max(v for _, v in tail) if tail else 0
        peak = max(v for _, v in self.samples) if self.samples else 0

        growth_pct = ((tail_max - baseline) / baseline * 100) if baseline else 0
        growth_mib = (tail_max - baseline) / 1_048_576

        mib = 1_048_576
        print(
            f"[{label}] RSS: baseline {baseline / mib:.1f} MiB, "
            f"tail max {tail_max / mib:.1f} MiB, "
            f"peak {peak / mib:.1f} MiB, "
            f"growth {growth_pct:.1f}%"
        )

        threshold = 25.0 if n >= 120 else 100.0
        assert growth_pct < threshold or growth_mib < 10.0, (
            f"[{label}] RSS leak detected: grew {growth_pct:.1f}% / "
            f"{growth_mib:.1f} MiB from baseline "
            f"({baseline / mib:.1f} MiB -> {tail_max / mib:.1f} MiB)"
        )
