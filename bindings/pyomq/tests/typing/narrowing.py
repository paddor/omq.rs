"""Tagged monitor unions. ty 0.0.63 does not narrow TypedDict tags yet."""

from typing import Literal, assert_type

from pyomq import MonitorEvent


def narrow_event(event: MonitorEvent) -> None:
    if event["event"] == "handshake_failed":
        assert_type(event["reason"], str)
    if event["event"] == "lagged":
        assert_type(event["count"], int)
    if event["event"] == "connect_stopped":
        assert_type(event["reason"], Literal["handshake_refused"])
        assert_type(event["mechanism"], str)
        assert_type(event["refusal_reason"], str)
        assert_type(event["status_code"], int | None)
