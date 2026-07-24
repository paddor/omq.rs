"""Soak: validate behavior across different IO thread counts.

Spawns subprocesses with io_threads=1 (current_thread runtime),
io_threads=2, and io_threads=4 (multi_thread runtime). Each subprocess
runs a PUSH/PULL throughput test and a REQ/REP latency test. Validates
that all configurations work correctly and that multi-thread doesn't
introduce races.

Uses subprocess because io_threads is set once per process before the
first socket materializes.
"""

import os
import subprocess
import sys
import time

from conftest import soak_duration

WORKER_SCRIPT = """
import struct
import sys
import threading
import time

import pyomq as zmq

IO_THREADS = int(sys.argv[1])
DURATION = float(sys.argv[2])

ctx = zmq.Context(io_threads=IO_THREADS)

# --- PUSH/PULL throughput ---
pull = ctx.socket(zmq.PULL)
push = ctx.socket(zmq.PUSH)
ep = pull.bind("tcp://127.0.0.1:0")
push.connect(ep)
push.setsockopt(zmq.SNDTIMEO, 5000)
pull.setsockopt(zmq.RCVTIMEO, 5000)

stop = False
sent = 0
recvd = 0
corrupt = 0

def sender():
    global sent, stop
    seq = 0
    while not stop:
        msg = struct.pack("<Q", seq) + b"X" * 56
        try:
            push.send(msg)
            seq += 1
            sent = seq
        except Exception:
            pass

def receiver():
    global recvd, corrupt, stop
    while not stop:
        try:
            msg = pull.recv()
            seq = struct.unpack("<Q", msg[:8])[0]
            if msg[8:] != b"X" * 56:
                corrupt += 1
            recvd += 1
        except Exception:
            pass

t_send = threading.Thread(target=sender, daemon=True)
t_recv = threading.Thread(target=receiver, daemon=True)
t_recv.start()
t_send.start()

start = time.monotonic()
half = DURATION / 2
while time.monotonic() - start < half:
    time.sleep(0.5)

stop = True
t_send.join(timeout=3)
t_recv.join(timeout=3)
push.close()
pull.close()

throughput_ok = recvd > 0 and corrupt == 0
elapsed = time.monotonic() - start
tput = recvd / elapsed if elapsed > 0 else 0

# --- REQ/REP latency ---
rep = ctx.socket(zmq.REP)
req = ctx.socket(zmq.REQ)
ep2 = rep.bind("tcp://127.0.0.1:0")
req.connect(ep2)
rep.setsockopt(zmq.RCVTIMEO, 5000)
rep.setsockopt(zmq.SNDTIMEO, 5000)
req.setsockopt(zmq.RCVTIMEO, 5000)
req.setsockopt(zmq.SNDTIMEO, 5000)
time.sleep(0.1)

cycles = 0
start2 = time.monotonic()
while time.monotonic() - start2 < half:
    req.send(struct.pack("<Q", cycles))
    msg = rep.recv()
    rep.send(msg)
    reply = req.recv()
    seq = struct.unpack("<Q", reply[:8])[0]
    assert seq == cycles, f"REQ/REP mismatch: {seq} != {cycles}"
    cycles += 1

latency_ok = cycles > 0
elapsed2 = time.monotonic() - start2
rtt = elapsed2 / cycles * 1e6 if cycles > 0 else 0

req.close()
rep.close()
ctx.term()

print(f"io_threads={IO_THREADS} throughput={tput:.0f}msg/s recvd={recvd} "
      f"corrupt={corrupt} cycles={cycles} rtt={rtt:.0f}us "
      f"ok={throughput_ok and latency_ok}")
sys.exit(0 if throughput_ok and latency_ok else 1)
"""


def test_io_threads_variants():
    duration = soak_duration()
    per_variant = max(duration / 3, 10)

    results = {}
    for n_threads in [1, 2, 4]:
        print(
            f"\n[io_threads] running with io_threads={n_threads}, "
            f"duration={per_variant:.0f}s"
        )
        result = subprocess.run(
            [sys.executable, "-c", WORKER_SCRIPT, str(n_threads), str(per_variant)],
            capture_output=True,
            text=True,
            timeout=per_variant + 30,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        print(f"  stdout: {result.stdout.strip()}")
        if result.stderr.strip():
            print(f"  stderr: {result.stderr.strip()}")
        assert result.returncode == 0, (
            f"io_threads={n_threads} failed: "
            f"rc={result.returncode}\n{result.stdout}\n{result.stderr}"
        )
        results[n_threads] = result.stdout.strip()

    print(f"\n[io_threads] all variants passed: {list(results.keys())}")
