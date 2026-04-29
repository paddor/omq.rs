"""Shared fixtures."""

import socket
import time

import pytest


def _free_tcp_port() -> int:
    """Allocate a free TCP port. Race-free enough for tests."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


@pytest.fixture
def tcp_endpoint() -> str:
    return f"tcp://127.0.0.1:{_free_tcp_port()}"


@pytest.fixture
def inproc_endpoint(request) -> str:
    # Unique per test so binds don't collide.
    return f"inproc://{request.node.name}"


@pytest.fixture
def ipc_endpoint(tmp_path_factory) -> str:
    # AF_UNIX paths cap near 108 bytes; pytest's nested tmp_path can
    # blow past that. Use a short unique path under /tmp.
    import tempfile, os
    fd, path = tempfile.mkstemp(suffix=".sock", prefix="pyomq-")
    os.close(fd)
    os.unlink(path)
    return f"ipc://{path}"


def wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Block-poll until predicate returns truthy or timeout. Returns the
    final predicate result."""
    deadline = time.monotonic() + timeout
    out = predicate()
    while not out and time.monotonic() < deadline:
        time.sleep(interval)
        out = predicate()
    return bool(out)
