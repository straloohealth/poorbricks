"""Unit tests for the alert sink abstraction."""

from __future__ import annotations

from typing import Any

import pytest

from poorbricks.alerting import (
    Alert,
    NoopSink,
    SlackWebhookSink,
    build_sink,
    filter_by_severity,
)


def _alert(severity: str = "warn", kind: str = "drift") -> Alert:
    return Alert(kind=kind, pipeline_key="postgres:t", severity=severity, summary="x")


def test_noop_sink_does_nothing() -> None:
    sink = NoopSink()
    sink.send(_alert())
    sink.send_batch([_alert(), _alert()])  # no exception, no effect


def test_filter_by_severity() -> None:
    alerts = [_alert("info"), _alert("warn"), _alert("error")]
    kept = filter_by_severity(alerts, "warn")
    assert {a.severity for a in kept} == {"warn", "error"}


def test_slack_sink_posts_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: dict[str, Any] = {}

    def _fake_post(url: str, json: dict[str, Any], timeout: int) -> None:
        posted["url"] = url
        posted["text"] = json["text"]

    import requests

    monkeypatch.setattr(requests, "post", _fake_post)
    SlackWebhookSink("https://hooks.example/x").send_batch([_alert(), _alert("error")])
    assert posted["url"] == "https://hooks.example/x"
    assert "drift" in posted["text"]


def test_slack_sink_swallows_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*a: object, **k: object) -> None:
        raise ConnectionError("down")

    import requests

    monkeypatch.setattr(requests, "post", _boom)
    # Must not raise.
    SlackWebhookSink("https://hooks.example/x").send(_alert())


def test_build_sink_is_noop_under_pytest() -> None:
    # PYTEST_CURRENT_TEST is set while a test runs, so build_sink avoids network.
    assert isinstance(build_sink(), NoopSink)
