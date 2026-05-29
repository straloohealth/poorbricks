"""Tests for web-debug endpoints + dev-target resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.main import _dag_target_kwargs, app


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    # ``_build_store`` reads ``dags_dir`` — point it at an empty tmp dir so the
    # store-backed endpoints (e.g. /v1/staleness) don't touch /opt/airflow/dags.
    monkeypatch.setattr(
        "api.main.settings",
        ApiSettings(airflow_url="http://af", dags_dir=str(tmp_path)),
    )
    return TestClient(app)


def test_dag_target_kwargs_prod_vs_dev() -> None:
    cfg = ApiSettings(
        postgres_host="prod-host",
        postgres_db="poorbricks",
        dev_postgres_host="",
        dev_postgres_db="",
        dev_schema_suffix="__dev",
        worker_retries=2,
    )
    prod = _dag_target_kwargs(cfg, "prod")
    assert prod["environment"] == "prod"
    assert prod["schema_suffix"] == ""
    assert prod["postgres_host"] == "prod-host"
    assert prod["retries"] == 2

    dev = _dag_target_kwargs(cfg, "dev")
    assert dev["environment"] == "dev"
    assert dev["schema_suffix"] == "__dev"
    # Falls back to the prod host when no dedicated dev host is configured.
    assert dev["postgres_host"] == "prod-host"
    assert dev["retries"] == 0


def test_trigger_dag_endpoint(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "poorbricks.airflow.watch.trigger_dag_run",
        lambda url, dag_id, **kw: "run-123",
    )
    resp = client.post("/v1/dags/dev-myrepo__gold/run")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-123"


def test_trigger_dag_endpoint_rejects_bad_id(client: TestClient) -> None:
    resp = client.post("/v1/dags/bad id!/run")
    assert resp.status_code == 400


def test_table_preview_endpoint(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from utils import postgres as pg_module

    class _FakeInspector:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def sample_table(
            self, schema: str, name: str, limit: int = 50
        ) -> pg_module.TableSnapshot:
            return pg_module.TableSnapshot(
                schema=schema,
                name=name,
                row_count=3,
                size_bytes=1024,
                columns=[],
                sample_rows=[{"id": 1}, {"id": 2}],
            )

    monkeypatch.setattr(pg_module, "PostgresInspector", _FakeInspector)
    resp = client.get("/v1/table/silver__dev/dim_patient?limit=2")
    assert resp.status_code == 200
    body = resp.json()
    assert body["row_count"] == 3
    assert body["sample_rows"] == [{"id": 1}, {"id": 2}]


def test_table_preview_rejects_bad_identifier(client: TestClient) -> None:
    resp = client.get('/v1/table/silver";DROP/dim')
    assert resp.status_code in (400, 404)


def test_runs_endpoint_serializes_datetimes(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import UTC, datetime

    from poorbricks.run_history import RunRecord

    rec = RunRecord(
        pipeline_key="postgres:x",
        table_name="x",
        environment="prod",
        mode="production",
        status="ok",
        started_at=datetime(2026, 5, 29, tzinfo=UTC),
        finished_at=datetime(2026, 5, 29, tzinfo=UTC),
        duration_s=1.0,
        row_count=5,
    )

    class _FakeStore:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def recent(self, limit: int = 100) -> list[RunRecord]:
            return [rec]

    monkeypatch.setattr("poorbricks.run_history.RunHistoryStore", _FakeStore)
    resp = client.get("/v1/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["pipeline_key"] == "postgres:x"
    # datetimes are serialized to ISO strings, not raw objects.
    assert isinstance(body[0]["started_at"], str)


def test_staleness_endpoint_empty_when_no_dags(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FakeStore:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def last_run_per_pipeline(self, environment: str | None = None) -> dict:
            return {}

    monkeypatch.setattr("poorbricks.run_history.RunHistoryStore", _FakeStore)
    # The fixture points dags_dir at an empty tmp dir → no cadences → no verdicts.
    resp = client.get("/v1/staleness")
    assert resp.status_code == 200
    assert resp.json() == []
