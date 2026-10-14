"""Smoke tests for non-TCP endpoint schemes in the Python binding."""

import sys

import pyomq
import pytest


@pytest.mark.parametrize("transport", ["dart", "shm"])
def test_push_pull_transport(transport, tmp_path):
    if sys.platform != "linux":
        pytest.skip(f"{transport} is Linux-only")
    if transport == "dart" and not pyomq.has("dart"):
        pytest.fail("pyomq was built without DART")

    endpoint = (
        "dart://127.0.0.1:0"
        if transport == "dart"
        else f"shm://{tmp_path / 'pyomq.sock'}"
    )
    sender_type = receiver_type = pyomq.CHANNEL
    with (
        pyomq.Context() as context,
        context.socket(receiver_type) as receiver,
        context.socket(sender_type) as sender,
    ):
        receiver.rcvtimeo = 5_000
        receiver.bind(endpoint)
        bound = receiver.last_endpoint
        if isinstance(bound, bytes):
            bound = bound.decode()
        sender.connect(bound)
        sender.send(b"python transport smoke")
        assert receiver.recv() == b"python transport smoke"
