"""Shared fixtures."""

import asyncio
import sys
import time

import pytest


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "event_loop: mark test requiring specific event loop type"
    )


def pytest_asyncio_loop_factories(config, item):
    if sys.platform == "win32":
        if m := item.get_closest_marker("event_loop"):
            ev = {}
            if "selector" in m.args:
                ev["selector"] = asyncio.SelectorEventLoop
            if "proactor" in m.args:
                ev["proactor"] = asyncio.ProactorEventLoop
            return ev
        return {"proactor": asyncio.ProactorEventLoop}
    else:
        return {
            "default": asyncio.new_event_loop,
        }


@pytest.fixture
def require_selector_event_loop():
    pass


@pytest.fixture
def tcp_endpoint() -> str:
    return "tcp://127.0.0.1:0"


@pytest.fixture
def inproc_endpoint(request) -> str:
    # Unique per test so binds don't collide.
    return f"inproc://{request.node.name}"


@pytest.fixture
def ipc_endpoint(request) -> str:
    import hashlib
    import os

    tag = hashlib.sha1(f"{os.getpid()}-{request.node.name}".encode()).hexdigest()[:16]
    if sys.platform == "win32":
        return f"ipc://pyomq-{tag}"
    # Linux abstract namespace: no filesystem entry, auto-cleaned on close.
    return f"ipc://@pyomq-{tag}"


def wait_for(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Block-poll until predicate returns truthy or timeout. Returns the
    final predicate result."""
    deadline = time.monotonic() + timeout
    out = predicate()
    while not out and time.monotonic() < deadline:
        time.sleep(interval)
        out = predicate()
    return bool(out)
