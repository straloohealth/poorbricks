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


# ---------------------------------------------------------------------------
# GET /v1/db-contract — DB-derived synthetic contracts
# ---------------------------------------------------------------------------

# Mock documents standing in for a live MongoDB collection. camelCase keys,
# 24-hex ObjectId-style _id, and large HTML content — the shape the real
# watson.notes collection has.
_MOCK_DOCS = [
    {
        "_id": f"{i:024x}",
        "authorId": f"navigator-{i}",
        "content": "<p>" + "clinical observation " * 30 + "</p>",
        "count": i,
    }
    for i in range(30)
]


class _FakeClock:
    """Mutable stand-in for the ``time`` module used by api.main."""

    def __init__(self, now: float) -> None:
        self.now = now

    def time(self) -> float:
        return self.now


@pytest.fixture
def _clear_db_cache() -> object:
    from api import main as api_main

    api_main._db_contract_cache.clear()
    yield None
    api_main._db_contract_cache.clear()


def test_db_contract_returns_inferred_synthetic_contract(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, _clear_db_cache: object
) -> None:
    monkeypatch.setattr(
        "utils.mongo_sample.sample_random_docs",
        lambda uri, db, coll, n: list(_MOCK_DOCS),
    )
    response = client.get(
        "/v1/db-contract", params={"db": "watson", "collection": "notes"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["sampled_count"] == len(_MOCK_DOCS)
    assert body["cache"] == "miss"
    # Native camelCase field names are preserved — never normalized.
    field_names = {f["name"] for f in body["schema_json"]["fields"]}
    assert {"_id", "authorId", "content", "count"} <= field_names
    assert len(body["example_rows"]) == 25
    # Example rows are synthetic, not real: HTML content is generated, and no
    # row repeats the verbatim mock content.
    real_content = _MOCK_DOCS[0]["content"]
    assert all("<" in row["content"] for row in body["example_rows"])
    assert all(row["content"] != real_content for row in body["example_rows"])


def test_db_contract_empty_collection_returns_404(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, _clear_db_cache: object
) -> None:
    from utils.mongo_sample import EmptyCollectionError

    def _raise(*_a: object, **_k: object) -> list[dict]:
        raise EmptyCollectionError("watson.notes is empty")

    monkeypatch.setattr("utils.mongo_sample.sample_random_docs", _raise)
    response = client.get(
        "/v1/db-contract", params={"db": "watson", "collection": "notes"}
    )
    assert response.status_code == 404


def test_db_contract_unreachable_returns_503(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, _clear_db_cache: object
) -> None:
    def _raise(*_a: object, **_k: object) -> list[dict]:
        raise RuntimeError("connection refused")

    monkeypatch.setattr("utils.mongo_sample.sample_random_docs", _raise)
    response = client.get(
        "/v1/db-contract", params={"db": "watson", "collection": "notes"}
    )
    assert response.status_code == 503


def test_db_contract_invalid_name_returns_400(client: TestClient) -> None:
    response = client.get(
        "/v1/db-contract", params={"db": "bad name!", "collection": "notes"}
    )
    assert response.status_code == 400


def test_db_contract_cache_hit_skips_resampling(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, _clear_db_cache: object
) -> None:
    calls: list[int] = []

    def _sampler(uri: str, db: str, coll: str, n: int) -> list[dict]:
        calls.append(1)
        return list(_MOCK_DOCS)

    monkeypatch.setattr("utils.mongo_sample.sample_random_docs", _sampler)
    params = {"db": "watson", "collection": "notes"}
    first = client.get("/v1/db-contract", params=params).json()
    second = client.get("/v1/db-contract", params=params).json()
    assert first["cache"] == "miss"
    assert second["cache"] == "hit"
    assert len(calls) == 1  # sampled once; second call served from cache


def test_db_contract_cache_expires_after_ttl(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, _clear_db_cache: object
) -> None:
    from api import main as api_main

    calls: list[int] = []

    def _sampler(uri: str, db: str, coll: str, n: int) -> list[dict]:
        calls.append(1)
        return list(_MOCK_DOCS)

    monkeypatch.setattr("utils.mongo_sample.sample_random_docs", _sampler)
    clock = _FakeClock(1_000_000.0)
    monkeypatch.setattr(api_main, "time", clock)
    params = {"db": "watson", "collection": "notes"}
    client.get("/v1/db-contract", params=params)
    clock.now += 25 * 60 * 60  # advance past the 24h TTL
    client.get("/v1/db-contract", params=params)
    assert len(calls) == 2  # re-sampled after the entry expired
