"""End-to-end tests for the upload API using ``test_table_repo`` fixtures."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.main import app

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_REPO = REPO_ROOT / "test_table_repo"


def _tarball(tables_dir: Path, workflows_dir: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(tables_dir, arcname="tables")
        tar.add(workflows_dir, arcname="workflows")
    return buf.getvalue()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("POORBRICKS_API_DAG_STORE", "local")
    monkeypatch.setenv("POORBRICKS_API_DAGS_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setattr("api.main.settings", ApiSettings())
    return TestClient(app)


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_invalid_prefix_rejected(client: TestClient) -> None:
    response = client.post(
        "/v1/upload",
        data={"prefix": "bad prefix!", "sha": "abc"},
        files={"code": ("c.tar.gz", b"", "application/gzip")},
    )
    assert response.status_code == 400


def test_stats_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """/v1/stats aggregates PostgresInspector snapshots into warehouse stats."""
    from utils import postgres as pg_module

    class _FakeInspector:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def inspect(self, sample_size: int = 10) -> list[pg_module.TableSnapshot]:
            return [
                pg_module.TableSnapshot(
                    schema="silver",
                    name="dim_patient",
                    row_count=5_000,
                    size_bytes=2_048,
                    columns=[],
                    sample_rows=[],
                ),
                pg_module.TableSnapshot(
                    schema="bronze",
                    name="smith_users",
                    row_count=7_500,
                    size_bytes=4_096,
                    columns=[],
                    sample_rows=[],
                ),
            ]

        def server_info(self) -> dict[str, str]:
            return {"host": "h", "port": "5432", "database": "poorbricks", "user": "u"}

    monkeypatch.setattr(pg_module, "PostgresInspector", _FakeInspector)

    response = client.get("/v1/stats")
    assert response.status_code == 200
    body = response.json()
    assert body["table_count"] == 2
    assert body["total_rows"] == 12_500
    assert body["server"]["database"] == "poorbricks"
    by_name = {t["name"]: t for t in body["tables"]}
    assert by_name["dim_patient"]["row_count"] == 5_000
    assert by_name["dim_patient"]["schema"] == "silver"


def test_upload_rejects_table_repo_with_missing_contract(client: TestClient) -> None:
    """test_table_repo contains a 'missing_contract' pipeline that
    intentionally references an upstream not in the contracts store —
    verify_local should fail and the API should return 422.
    """
    payload = _tarball(TEST_REPO / "tables", TEST_REPO / "workflows")
    response = client.post(
        "/v1/upload",
        data={"prefix": "test-repo", "sha": "deadbeef"},
        files={"code": ("c.tar.gz", payload, "application/gzip")},
    )
    body = response.json()
    assert response.status_code == 422
    assert body["ok"] is False
    assert any("missing_contract" in e or "ContractError" in e for e in body["errors"])
