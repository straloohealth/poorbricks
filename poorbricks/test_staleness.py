"""Unit tests for the stale-data monitor's pure classifier."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from poorbricks.staleness import PipelineCadence, evaluate

_NOW = datetime(2026, 5, 29, 12, 0, 0, tzinfo=UTC)
_DAILY = 24 * 3600.0


def _cadences() -> dict[str, PipelineCadence]:
    return {
        "postgres:fresh": PipelineCadence("postgres:fresh", "0 2 * * *", _DAILY),
        "postgres:stale": PipelineCadence("postgres:stale", "0 2 * * *", _DAILY),
        "postgres:never": PipelineCadence("postgres:never", "0 2 * * *", _DAILY),
    }


def test_evaluate_classifies_ok_overdue_missing() -> None:
    last = {
        "postgres:fresh": _NOW - timedelta(hours=6),  # well within a day
        "postgres:stale": _NOW - timedelta(days=3),  # > 1.5 days
        # "postgres:never" has no run recorded
    }
    verdicts = {v.pipeline_key: v for v in evaluate(_cadences(), last, _NOW)}
    assert verdicts["postgres:fresh"].state == "ok"
    assert verdicts["postgres:stale"].state == "overdue"
    assert verdicts["postgres:never"].state == "missing"
    assert verdicts["postgres:never"].last_run is None


def test_overdue_boundary_uses_1_5x_interval() -> None:
    cadences = {"k": PipelineCadence("k", "0 * * * *", 3600.0)}  # hourly
    # 1.4h old → ok; 1.6h old → overdue (grace = 0.5 * interval).
    ok = evaluate(cadences, {"k": _NOW - timedelta(hours=1.4)}, _NOW)[0]
    overdue = evaluate(cadences, {"k": _NOW - timedelta(hours=1.6)}, _NOW)[0]
    assert ok.state == "ok"
    assert overdue.state == "overdue"


def _daily_dag_source(pipeline: str) -> str:
    from poorbricks.airflow.dag_generator import generate_dag_file
    from poorbricks.airflow.workflow import TaskConfig, WorkflowConfig

    wf = WorkflowConfig("wf", "0 2 * * *", (TaskConfig(id="t", pipeline=pipeline),))
    return generate_dag_file(wf, prefix="repo", image="img")


class _FakeDagStore:
    def __init__(self, dags: dict[str, str]) -> None:
        self._dags = dags  # {prefix: source}

    def list_prefixes(self) -> list[str]:
        return list(self._dags)

    def list_dags(self, prefix: str) -> list[str]:
        return ["wf"]

    def get(self, prefix: str, name: str) -> str:
        return self._dags[prefix]


def test_cadences_skip_dev_prefix() -> None:
    from poorbricks.staleness import cadences_from_dags

    store = _FakeDagStore(
        {
            "repo": _daily_dag_source("postgres:a"),
            "dev-repo": _daily_dag_source("postgres:b"),
        }
    )
    cadences = cadences_from_dags(store, _NOW)
    assert "postgres:a" in cadences  # prod DAG counted
    assert "postgres:b" not in cadences  # dev- DAG excluded


def test_run_monitor_emits_overdue_alert() -> None:
    from poorbricks.run_history import RunRecord
    from poorbricks.staleness import run_monitor

    old = _NOW - timedelta(days=5)

    class _FakeHist:
        def last_run_per_pipeline(self, environment: str | None = None) -> dict:
            return {
                "postgres:orders": RunRecord(
                    pipeline_key="postgres:orders",
                    table_name="orders",
                    environment="prod",
                    mode="production",
                    status="ok",
                    started_at=old,
                    finished_at=old,
                    duration_s=1.0,
                )
            }

    captured: list = []

    class _FakeSink:
        def send(self, alert: object) -> None:
            captured.append(alert)

        def send_batch(self, alerts: list) -> None:
            captured.extend(alerts)

    verdicts = run_monitor(
        sink=_FakeSink(),
        dag_store=_FakeDagStore({"repo": _daily_dag_source("postgres:orders")}),
        store=_FakeHist(),
        now=_NOW,
    )
    assert any(v.state == "overdue" for v in verdicts)
    assert any(getattr(a, "kind", "") == "staleness" for a in captured)
