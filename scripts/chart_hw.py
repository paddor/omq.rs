"""Shared hardware label detection for chart generation scripts.

Reads CPU model and core count from /proc/cpuinfo, governor and turbo
state from sysfs, with overrides from environment variables or a
.chart_hw config file in the repo root.

Override precedence (highest to lowest):
  1. OMQ_HW_PREFIX / OMQ_HW_POSTFIX / OMQ_HW_EXTRAS env vars
  2. .chart_hw file (key=value, keys: prefix, postfix, extras)
  3. sysfs auto-detection (governor, turbo)
"""

import os
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent


def _read_chart_hw() -> dict[str, str]:
    """Read .chart_hw config file from repo root."""
    path = _REPO / ".chart_hw"
    result = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip()
    return result


def detect_hardware() -> str | None:
    """Return a hardware label string for chart subtitles."""
    hw_conf = _read_chart_hw()

    try:
        cpu = None
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                cpu = cpu.replace("(R)", "").replace("(TM)", "").replace("CPU ", "")
                break
        cores = os.cpu_count()
        if cpu and cores:
            label = f"{cpu}, {cores} cores"
            extras = []
            try:
                gov = open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read().strip()
                if gov == "performance":
                    extras.append("performance governor")
            except OSError:
                pass
            for path, off_val in [
                ("/sys/devices/system/cpu/intel_pstate/no_turbo", "1"),
                ("/sys/devices/system/cpu/cpufreq/boost", "0"),
            ]:
                try:
                    if open(path).read().strip() == off_val:
                        extras.append("turbo off")
                    break
                except OSError:
                    continue
            postfix = os.environ.get("OMQ_HW_POSTFIX") or hw_conf.get("postfix")
            if postfix:
                extras = [e.strip() for e in postfix.split(",")]
            elif not extras:
                hw_extras = os.environ.get("OMQ_HW_EXTRAS") or hw_conf.get("extras")
                if hw_extras:
                    extras.extend(hw_extras.split(","))
            if extras:
                label += ", " + ", ".join(extras)
            prefix = os.environ.get("OMQ_HW_PREFIX") or hw_conf.get("prefix")
            if prefix:
                label = f"{prefix}, {label}"
            return label
    except OSError:
        pass
    return None
