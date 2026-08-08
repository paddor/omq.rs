"""Internal test support hooks for pyomq's own test suite."""

from __future__ import annotations

from . import _native


def rust_thread_send_via_share_key(key: int, endpoint: str, payload: bytes) -> None:
    """Start a Rust thread that imports ``key`` and sends one inproc message."""
    _native.rust_thread_send_via_share_key(key, endpoint, payload)
