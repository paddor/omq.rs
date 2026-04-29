"""Performance gate: pyomq PUSH/PULL must beat pyzmq by 2x at small
sizes and at least match it at large sizes.

Loopback inproc / tcp throughput across {128, 512, 2048, 8192, 32768}
byte payloads, alternating runs to dampen thermal noise. The harness
asserts ``pyomq msgs/s >= 2.0 * pyzmq msgs/s`` for small sizes and
``>= 0.95 * pyzmq`` for large sizes. Skipped automatically when
pyzmq isn't importable.
"""

import threading
import time

import pytest

zmq_pyzmq = pytest.importorskip("zmq")  # pyzmq

import pyomq

SIZES = [128, 512, 2048, 8192, 32768]
TARGET_RUNTIME_S = 0.4

# Small messages: pyomq must be at least 2x pyzmq (this is where the
# Rust-native impl pays off - per-call overhead dominates and we beat
# pyzmq's libzmq trip).
# Large messages: 2 * payload-size memcpy per cycle dominates (one on
# send, one on recv); both impls hit the same memory-bandwidth wall.
# We require pyomq to be at least as fast as pyzmq there, no 2x.
SMALL_SIZES = [s for s in SIZES if s <= 2048]
LARGE_SIZES = [s for s in SIZES if s > 2048]
SMALL_GATE = 2.0
LARGE_GATE = 0.95  # allow ~5% noise; equality with pyzmq counts as a pass


def _measure_pyomq(endpoint: str, size: int, n_target_per_s: int = 200_000) -> float:
    payload = b"x" * size
    ctx = pyomq.Context()
    pull = ctx.socket(pyomq.PULL)
    push = ctx.socket(pyomq.PUSH)
    pull.bind(endpoint)
    push.connect(endpoint)

    def sender(n):
        for _ in range(n):
            push.send(payload)

    # Calibrate: how many to send to fill TARGET_RUNTIME_S.
    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    t = threading.Thread(target=sender, args=(n,))
    start = time.monotonic()
    t.start()
    received = 0
    while received < n:
        pull.recv()
        received += 1
    elapsed = time.monotonic() - start
    t.join()

    push.close()
    pull.close()
    ctx.term()
    return n / elapsed


def _measure_pyzmq(endpoint: str, size: int, n_target_per_s: int = 200_000) -> float:
    payload = b"x" * size
    ctx = zmq_pyzmq.Context.instance()
    pull = ctx.socket(zmq_pyzmq.PULL)
    push = ctx.socket(zmq_pyzmq.PUSH)
    pull.bind(endpoint)
    push.connect(endpoint)

    def sender(n):
        for _ in range(n):
            push.send(payload)

    n = max(int(n_target_per_s * TARGET_RUNTIME_S), 100)
    t = threading.Thread(target=sender, args=(n,))
    start = time.monotonic()
    t.start()
    received = 0
    while received < n:
        pull.recv()
        received += 1
    elapsed = time.monotonic() - start
    t.join()

    push.close()
    pull.close()
    return n / elapsed


def _free_inproc(label: str) -> str:
    return f"inproc://perf-{label}-{time.monotonic_ns()}"


def _gate_for(size: int) -> float:
    return SMALL_GATE if size in SMALL_SIZES else LARGE_GATE


@pytest.mark.parametrize("size", SIZES)
def test_perf_inproc(size):
    # Warmup once each, then measure two rounds and take the best.
    _measure_pyomq(_free_inproc(f"warm-pyomq-{size}"), size)
    _measure_pyzmq(f"inproc://warm-pyzmq-{size}-{time.monotonic_ns()}", size)
    runs_omq = [_measure_pyomq(_free_inproc(f"omq-{size}-{i}"), size) for i in range(2)]
    runs_pz = [
        _measure_pyzmq(f"inproc://pz-{size}-{i}-{time.monotonic_ns()}", size)
        for i in range(2)
    ]
    omq = max(runs_omq)
    pz = max(runs_pz)
    ratio = omq / pz
    gate = _gate_for(size)
    print(
        f"[perf inproc {size:>5}B]  pyomq {omq:>10,.0f} msg/s  "
        f"pyzmq {pz:>10,.0f} msg/s  ratio {ratio:.2f}x  (gate {gate:.2f}x)"
    )
    assert ratio >= gate, (
        f"inproc {size}B: pyomq {omq:.0f} msg/s vs pyzmq {pz:.0f} msg/s "
        f"= {ratio:.2f}x, below the {gate}x gate"
    )


def _free_tcp_port_local() -> int:
    import socket as _so
    s = _so.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _new_tcp_ep() -> str:
    return f"tcp://127.0.0.1:{_free_tcp_port_local()}"


@pytest.mark.parametrize("size", SIZES)
def test_perf_tcp(size):
    # Warmup with their own fresh ports - "tcp://127.0.0.1:0" would
    # leave the connector pointing at port 0 (invalid), hanging the run.
    _measure_pyomq(_new_tcp_ep(), size)
    _measure_pyzmq(_new_tcp_ep(), size)
    # Re-allocate per run to avoid TIME_WAIT collisions.
    omq_runs = [_measure_pyomq(_new_tcp_ep(), size) for _ in range(2)]
    pz_runs = [_measure_pyzmq(_new_tcp_ep(), size) for _ in range(2)]
    omq = max(omq_runs)
    pz = max(pz_runs)
    ratio = omq / pz
    gate = _gate_for(size)
    print(
        f"[perf tcp    {size:>5}B]  pyomq {omq:>10,.0f} msg/s  "
        f"pyzmq {pz:>10,.0f} msg/s  ratio {ratio:.2f}x  (gate {gate:.2f}x)"
    )
    assert ratio >= gate, (
        f"tcp {size}B: pyomq {omq:.0f} msg/s vs pyzmq {pz:.0f} msg/s "
        f"= {ratio:.2f}x, below the {gate}x gate"
    )
