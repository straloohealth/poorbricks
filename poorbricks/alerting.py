"""Pluggable alert sink for run-health signals.

The framework propagates its own operational signals (run failures, row-count
anomalies, data regressions, drift, stale pipelines) through a small sink
abstraction. The default is a Slack webhook when ``SLACK_WEBHOOK_URL`` is
present, and a no-op otherwise (and always under pytest) so tests and local
runs never hit the network.

Sending is best-effort: a sink never raises into a pipeline run — a transport
failure is logged to stdout (so it lands in worker pod logs) and swallowed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

_SEVERITY_ORDER = {"info": 0, "warn": 1, "error": 2}


@dataclass
class Alert:
    """One operational signal worth surfacing to humans."""

    kind: str  # "failure" | "row_count_anomaly" | "regression" | "drift" | "staleness"
    pipeline_key: str
    severity: str  # "info" | "warn" | "error"
    summary: str
    context: dict[str, Any] = field(default_factory=dict)
    environment: str = "unknown"
    sha: str | None = None

    def as_text(self) -> str:
        env = f"[{self.environment}]"
        head = f"{env} {self.kind} — {self.pipeline_key}: {self.summary}"
        if self.sha:
            head += f" (sha={self.sha})"
        return head


@runtime_checkable
class AlertSink(Protocol):
    def send(self, alert: Alert) -> None: ...

    def send_batch(self, alerts: list[Alert]) -> None: ...


class NoopSink:
    """Drops alerts. Default for tests / local runs with no webhook."""

    def send(self, alert: Alert) -> None:
        return None

    def send_batch(self, alerts: list[Alert]) -> None:
        return None


class SlackWebhookSink:
    """POSTs each alert to a Slack incoming webhook. Never raises."""

    def __init__(self, webhook_url: str) -> None:
        self._url = webhook_url

    def send(self, alert: Alert) -> None:
        self._post(alert.as_text())

    def send_batch(self, alerts: list[Alert]) -> None:
        if not alerts:
            return
        self._post("\n".join(a.as_text() for a in alerts))

    def _post(self, text: str) -> None:
        try:
            import requests

            requests.post(self._url, json={"text": text}, timeout=10)
        except Exception as exc:  # noqa: BLE001 — alerting must never fail a run
            print(f"[alert] send failed: {exc}", flush=True)


class MultiSink:
    """Fan an alert out to several sinks (each isolated from the others)."""

    def __init__(self, sinks: list[AlertSink]) -> None:
        self._sinks = sinks

    def send(self, alert: Alert) -> None:
        for sink in self._sinks:
            try:
                sink.send(alert)
            except Exception as exc:  # noqa: BLE001
                print(f"[alert] sink error: {exc}", flush=True)

    def send_batch(self, alerts: list[Alert]) -> None:
        for sink in self._sinks:
            try:
                sink.send_batch(alerts)
            except Exception as exc:  # noqa: BLE001
                print(f"[alert] sink error: {exc}", flush=True)


def build_sink() -> AlertSink:
    """Construct the configured sink.

    Order: explicit ``noop`` / under pytest → NoopSink; ``slack`` or ``auto``
    with a ``SLACK_WEBHOOK_URL`` present → SlackWebhookSink; otherwise NoopSink.
    """
    from poorbricks.settings import settings

    if "PYTEST_CURRENT_TEST" in os.environ:
        return NoopSink()

    choice = (settings.alert_sink or "auto").lower()
    if choice == "noop":
        return NoopSink()
    webhook = settings.slack_webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if choice in ("slack", "auto") and webhook:
        return SlackWebhookSink(webhook)
    return NoopSink()


def filter_by_severity(alerts: list[Alert], min_severity: str) -> list[Alert]:
    """Keep only alerts at or above ``min_severity``."""
    floor = _SEVERITY_ORDER.get(min_severity, 1)
    return [a for a in alerts if _SEVERITY_ORDER.get(a.severity, 1) >= floor]


def emit(alerts: list[Alert], sink: AlertSink | None = None) -> None:
    """Filter by configured severity and send a batch. Best-effort, never raises."""
    if not alerts:
        return
    try:
        from poorbricks.settings import settings

        kept = filter_by_severity(alerts, settings.alert_min_severity)
        if not kept:
            return
        (sink or build_sink()).send_batch(kept)
    except Exception as exc:  # noqa: BLE001 — alerting must never fail a run
        print(f"[alert] emit failed: {exc}", flush=True)


__all__ = [
    "Alert",
    "AlertSink",
    "MultiSink",
    "NoopSink",
    "SlackWebhookSink",
    "build_sink",
    "emit",
    "filter_by_severity",
]
