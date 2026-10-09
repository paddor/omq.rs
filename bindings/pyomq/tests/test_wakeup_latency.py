"""Gate blocking wait paths against timer polling.

Each case runs ROUNDS lockstep waits. A path that sleeps 1 ms between
readiness checks needs at least ROUNDS ms, far above BUDGET_S.
"""

import os
import sys
import threading
import time
from pathlib import Path

import pyomq as zmq
import pytest

ROUNDS = 1000
WARMUP = 100
BUDGET_S = 0.6
ATTEMPTS = 3
TIMEOUT_MS = 5_000


def _best_of(run):
    """Return the fastest attempt; noisy hosts only fail if all are slow."""
    return min(run() for _ in range(ATTEMPTS))


def _serve_rep(rep, rounds, poller=None):
    for _ in range(rounds):
        if poller is not None:
            assert poller.poll(TIMEOUT_MS), "poll timed out"
        rep.send(rep.recv())


def _time_req(req, rounds):
    for _ in range(WARMUP):
        req.send(b"x")
        req.recv()
    started = time.perf_counter()
    for _ in range(rounds - WARMUP):
        req.send(b"x")
        req.recv()
    return time.perf_counter() - started


def _reqrep_round_trips(timeouts=False, poll=False):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        if timeouts:
            for sock in (rep, req):
                sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
                sock.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
        rep.bind("tcp://127.0.0.1:0")
        req.connect(rep.last_endpoint)
        poller = None
        if poll:
            poller = zmq.Poller()
            poller.register(rep, zmq.POLLIN)
        server = threading.Thread(target=_serve_rep, args=(rep, ROUNDS, poller))
        server.start()
        elapsed = _time_req(req, ROUNDS)
        server.join(TIMEOUT_MS / 1000)
        assert not server.is_alive()
        return elapsed
    finally:
        req.close(linger=0)
        rep.close(linger=0)
        ctx.term()


@pytest.mark.parametrize(
    "timeouts,poll",
    [(False, False), (True, False), (False, True)],
    ids=["blocking", "timeouts", "poller"],
)
def test_reqrep_waits_wake_promptly(timeouts, poll):
    elapsed = _best_of(lambda: _reqrep_round_trips(timeouts=timeouts, poll=poll))
    assert elapsed < BUDGET_S, f"{ROUNDS - WARMUP} round trips took {elapsed:.3f}s"


def _muted_sends():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        push.setsockopt(zmq.SNDHWM, 1)
        push.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
        pull.setsockopt(zmq.RCVHWM, 1)
        pull.bind("inproc://wakeup-muted-send")
        push.connect("inproc://wakeup-muted-send")
        # Fill the queue so each round's first send starts muted. Receivers
        # release queue space in batches, so each round drains the depth.
        depth = 0
        while True:
            try:
                push.send(b"x", zmq.NOBLOCK)
                depth += 1
            except zmq.Again:
                break
        go = threading.Semaphore(0)

        def consume():
            for _ in range(ROUNDS):
                assert go.acquire(timeout=TIMEOUT_MS / 1000)
                for _ in range(depth):
                    pull.recv()

        consumer = threading.Thread(target=consume)
        consumer.start()
        started = time.perf_counter()
        for _ in range(ROUNDS):
            go.release()
            for _ in range(depth):
                push.send(b"x")
        elapsed = time.perf_counter() - started
        consumer.join(TIMEOUT_MS / 1000)
        assert not consumer.is_alive()
        return elapsed
    finally:
        push.close(linger=0)
        pull.close(linger=0)
        ctx.term()


def test_muted_send_with_timeout_wakes_promptly():
    elapsed = _best_of(_muted_sends)
    # Rounds are shorter than round trips; 1 ms polling costs ~0.8 s.
    assert elapsed < BUDGET_S / 2, f"{ROUNDS} muted send rounds took {elapsed:.3f}s"


def _forked_rep_round_trips():
    port_r, port_w = os.pipe()
    pid = os.fork()  # ty: ignore[unresolved-attribute, unused-ignore-comment]
    if pid == 0:
        os.close(port_r)
        code = 1
        try:
            ctx = zmq.Context()
            rep = ctx.socket(zmq.REP)
            rep.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
            rep.bind("tcp://127.0.0.1:0")
            os.write(port_w, rep.last_endpoint)
            os.close(port_w)
            _serve_rep(rep, ROUNDS)
            rep.close(linger=0)
            code = 0
        finally:
            os._exit(code)
    os.close(port_w)
    endpoint = os.read(port_r, 256).decode()
    os.close(port_r)
    ctx = zmq.Context()
    req = ctx.socket(zmq.REQ)
    req.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    try:
        req.connect(endpoint)
        return _time_req(req, ROUNDS)
    finally:
        req.close(linger=0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0


@pytest.mark.skipif(os.name == "nt", reason="fork is Unix-only")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="macOS does not reliably fork after Rust runtime threads exist",
)
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork:DeprecationWarning"
)
def test_forked_child_waits_wake_promptly():
    elapsed = _best_of(_forked_rep_round_trips)
    assert elapsed < BUDGET_S, f"{ROUNDS - WARMUP} round trips took {elapsed:.3f}s"


def test_native_source_has_no_sleep_polling():
    """Waits must park on a wakeup, never poll on a timer."""
    src = Path(__file__).resolve().parents[1] / "src"
    hits = [
        f"{path.relative_to(src)}:{lineno}: {line.strip()}"
        for path in sorted(src.rglob("*.rs"))
        for lineno, line in enumerate(path.read_text().splitlines(), 1)
        if "thread::sleep(" in line
    ]
    assert not hits, "sleep polling in native source:\n" + "\n".join(hits)
