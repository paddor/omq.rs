"""Tagged monitor unions. ty 0.0.63 does not narrow TypedDict tags yet."""

from typing import assert_type

from pyomq import MonitorEvent


def narrow_event(event: MonitorEvent) -> None:
    if event["event"] == "handshake_failed":
        assert_type(event["reason"], str)
    if event["event"] == "lagged":
        assert_type(event["count"], int)
