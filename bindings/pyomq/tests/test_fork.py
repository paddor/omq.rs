"""Verify sockets re-materialize correctly after fork."""

import os
import select
import time

import pytest

import pyomq as zmq

pytestmark = pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork:DeprecationWarning"
)
pytestmark = [
    pytestmark,
    pytest.mark.skipif(os.name == "nt", reason="fork is Unix-only"),
]

FORK_TIMEOUT_MS = 5_000
FORK_TIMEOUT_S = FORK_TIMEOUT_MS / 1_000


def _waitpid_timeout(pid):
    """Reap a child, killing it if the forked operation wedges."""
    deadline = time.monotonic() + FORK_TIMEOUT_S
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            if os.waitstatus_to_exitcode(status) != 0:
                pytest.fail("forked child exited with an error")
            return status
        time.sleep(0.01)
    os.kill(pid, 9)
    os.waitpid(pid, 0)
    pytest.fail("forked child did not exit before timeout")


def _child_recv(ep, result_fd):
    """Run in forked child: connect, recv one message, write result."""
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    pull.connect(ep)
    pull.setsockopt(zmq.RCVTIMEO, FORK_TIMEOUT_MS)
    msg = pull.recv()
    os.write(result_fd, msg)
    pull.close()
    os._exit(0)


def test_socket_works_after_fork():
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDTIMEO, FORK_TIMEOUT_MS)
    ep = push.bind("tcp://127.0.0.1:0")

    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        _child_recv(ep, w)
    else:
        os.close(w)
        try:
            time.sleep(0.1)
            push.send(b"after-fork")
            ready, _, _ = select.select([r], [], [], FORK_TIMEOUT_S)
            if not ready:
                pytest.fail("forked child did not receive before timeout")
            data = os.read(r, 1024)
        finally:
            try:
                _waitpid_timeout(pid)
            finally:
                os.close(r)
                push.close()
        assert data == b"after-fork"


def test_pre_materialized_socket_works_after_fork():
    """Socket materialized before fork gets fresh state in child."""
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    push.setsockopt(zmq.SNDTIMEO, FORK_TIMEOUT_MS)
    pull.setsockopt(zmq.RCVTIMEO, FORK_TIMEOUT_MS)
    ep = pull.bind("tcp://127.0.0.1:0")
    push.connect(ep)

    # Materialize by sending a message before fork.
    push.send(b"pre-fork")
    assert pull.recv() == b"pre-fork"

    r, w = os.pipe()
    ack_r, ack_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        # Child: the parent's push socket is stale. Create a fresh one.
        child_ctx = zmq.Context()
        child_push = child_ctx.socket(zmq.PUSH)
        child_push.setsockopt(zmq.SNDTIMEO, FORK_TIMEOUT_MS)
        child_push.connect(ep)
        child_push.send(b"child-msg")
        os.write(w, b"s")
        os.read(ack_r, 1)
        os.close(ack_r)
        child_push.close()
        os._exit(0)
    else:
        os.close(w)
        os.close(ack_r)
        try:
            ready, _, _ = select.select([r], [], [], FORK_TIMEOUT_S)
            if not ready:
                pytest.fail("forked child did not send before timeout")
            os.read(r, 1)
            msg = pull.recv()
        finally:
            os.write(ack_w, b"a")
            os.close(ack_w)
            try:
                _waitpid_timeout(pid)
            finally:
                os.close(r)
                push.close()
                pull.close()
        assert msg == b"child-msg"
