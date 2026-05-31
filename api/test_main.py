"""End-to-end tests for the upload API using ``test_table_repo`` fixtures."""

from __future__ import annotations

import ast
import io
import tarfile
from collections.abc import Iterator
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
    # The failure response carries a phase trail showing how far it got.
    assert any(p["name"] == "verify_local" for p in body.get("phases", []))


def test_status_reports_phase(client: TestClient) -> None:
    """/v1/status reports the upload phase (idle when no upload is running)."""
    body = client.get("/v1/status").json()
    assert body["busy"] is False
    assert body["phase"] == "idle"


def test_upload_internal_error_returns_structured_500(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexpected exception in _handle_upload yields a structured JSON 500
    (message + phase + traceback), never an empty body — so a CI upload
    failure is never silent."""

    def _boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated extract failure")

    monkeypatch.setattr("api.main._extract_safely", _boom)
    response = client.post(
        "/v1/upload",
        data={"prefix": "smith", "sha": "abc123"},
        files={"code": ("c.tar.gz", b"not-a-real-tarball", "application/gzip")},
    )
    assert response.status_code == 500
    body = response.json()
    assert body["ok"] is False
    assert "simulated extract failure" in body["message"]
    assert body["phase"] == "extracting"
    assert isinstance(body["traceback"], list) and body["traceback"]


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


# ---------------------------------------------------------------------------
# GET /v1/code/{prefix} and POST /v1/regenerate — multi-node worker support
# ---------------------------------------------------------------------------

# A pre-Spot ("legacy") generated DAG: PVC-mounted code + nodeSelector pinning.
# parse_generated_dag reads only the format-stable parts, so it round-trips
# both this layout and the current init-container one.
_LEGACY_DAG = '''\
"""Auto-generated by poorbricks.airflow.dag_generator. Do not edit."""

from datetime import datetime

DAG_ID = 'myrepo__nightly'
PREFIX = 'myrepo'
NAMESPACE = 'airflow'
SCHEDULE = '0 3 * * *'
START_DATE = datetime(2024, 1, 1)
IMAGE = 'legacy-image:v1'
RUNTIME_SECRET = 'poorbricks-runtime'
POSTGRES_CREDS_SECRET = 'poorbricks-server-postgresql-creds'
CODE_PVC_CLAIM = 'airflow-dags'
CODE_SUBPATH = '__code__/myrepo'
NODE_SELECTOR = {'poorbricks.io/dags': 'true'}


def _build_task(task_id, pipeline_key, command):
    return None


task_extract = _build_task('extract', 'postgres:extract', 'run')
task_report = _build_task('report', 'postgres:report', 'run')

task_extract >> task_report
'''


@pytest.fixture
def code_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, Path]]:
    """A TestClient whose DAG store + code tree live under a tmp directory."""
    dags = tmp_path / "dags"
    dags.mkdir()
    monkeypatch.setenv("POORBRICKS_API_DAG_STORE", "local")
    monkeypatch.setenv("POORBRICKS_API_DAGS_DIR", str(dags))
    monkeypatch.setenv(
        "POORBRICKS_API_WORKER_IMAGE", "registry.example/poorbricks:current"
    )
    monkeypatch.setattr("api.main.settings", ApiSettings())
    yield TestClient(app), dags


def test_get_code_returns_tarball(code_client: tuple[TestClient, Path]) -> None:
    """GET /v1/code/{prefix} streams the published tables/ tree as a tarball."""
    client, dags = code_client
    code_tree = dags / "__code__" / "myrepo" / "tables" / "silver" / "dim"
    code_tree.mkdir(parents=True)
    (code_tree / "transform.py").write_text("print('x')\n")

    response = client.get("/v1/code/myrepo")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/gzip"
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tar:
        names = tar.getnames()
    assert "tables/silver/dim/transform.py" in names


def test_get_code_404_when_prefix_unpublished(
    code_client: tuple[TestClient, Path],
) -> None:
    client, _ = code_client
    assert client.get("/v1/code/ghost").status_code == 404


def test_get_code_400_on_invalid_prefix(
    code_client: tuple[TestClient, Path],
) -> None:
    client, _ = code_client
    assert client.get("/v1/code/bad prefix!").status_code == 400


def test_regenerate_migrates_legacy_dag(
    code_client: tuple[TestClient, Path],
) -> None:
    """POST /v1/regenerate re-renders a stored pre-Spot DAG onto the
    multi-node + Spot worker spec, in place, with no re-upload."""
    client, dags = code_client
    (dags / "myrepo").mkdir()
    dag_path = dags / "myrepo" / "nightly.py"
    dag_path.write_text(_LEGACY_DAG)

    response = client.post("/v1/regenerate")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["count"] == 1
    assert body["regenerated"] == ["myrepo/nightly"]
    assert body["failed"] == []

    rendered = dag_path.read_text()
    ast.parse(rendered)
    # The re-rendered DAG carries the new init-container + Spot worker spec.
    assert "_INIT_CONTAINERS" in rendered
    assert "fetch-code" in rendered
    assert "cloud.google.com/gke-spot" in rendered
    assert "V1PersistentVolumeClaimVolumeSource" not in rendered
    assert "NODE_SELECTOR" not in rendered
    # The task graph + format-stable params survived the round trip.
    assert "task_extract >> task_report" in rendered
    assert "DAG_ID = 'myrepo__nightly'" in rendered
    assert "datetime(2024, 1, 1)" in rendered
    # The image is refreshed to the current worker image — the stale image
    # baked in the legacy DAG (which would lack the fetch-code module) is gone.
    assert "registry.example/poorbricks:current" in rendered
    assert "legacy-image:v1" not in rendered


def test_regenerate_dev_dag_is_manual_only(
    code_client: tuple[TestClient, Path],
) -> None:
    """A ``dev-`` prefixed DAG is re-rendered manual-trigger-only: the cron is
    stripped (SCHEDULE = None) even when the stored DAG carried one, so dev
    jobs never auto-run and are never evaluated for staleness."""
    client, dags = code_client
    (dags / "dev-myrepo").mkdir()
    dag_path = dags / "dev-myrepo" / "nightly.py"
    dag_path.write_text(_LEGACY_DAG)  # carries SCHEDULE = '0 3 * * *'

    body = client.post("/v1/regenerate").json()

    assert body["ok"] is True
    assert body["regenerated"] == ["dev-myrepo/nightly"]
    rendered = dag_path.read_text()
    ast.parse(rendered)
    assert "SCHEDULE = None" in rendered
    assert "'0 3 * * *'" not in rendered


def test_regenerate_reports_unparseable_dag(
    code_client: tuple[TestClient, Path],
) -> None:
    """A DAG that cannot be parsed is reported under 'failed' and left intact."""
    client, dags = code_client
    (dags / "broken").mkdir()
    bad_dag = dags / "broken" / "wf.py"
    bad_dag.write_text("this is @@ not valid python")

    body = client.post("/v1/regenerate").json()

    assert body["ok"] is True
    assert body["count"] == 0
    assert any(f["dag"] == "broken/wf" for f in body["failed"])
    # The unparseable file is untouched.
    assert bad_dag.read_text() == "this is @@ not valid python"


def test_regenerate_busy_returns_503(
    code_client: tuple[TestClient, Path],
) -> None:
    """While an upload holds the lock, /v1/regenerate refuses with 503."""
    from api import main as api_main

    client, _ = code_client
    api_main._upload_lock.acquire()
    try:
        response = client.post("/v1/regenerate")
    finally:
        api_main._upload_lock.release()
    assert response.status_code == 503


@pytest.mark.integration
def test_source_comment_crud(client: TestClient) -> None:
    """POST/GET/DELETE line comments on a table's source (needs local Mongo)."""
    table = "_cc_api_test_table"
    base = f"/v1/source/{table}/comments"

    # Validation: unknown file, inverted range, and empty body are all 400.
    assert (
        client.post(
            base, json={"file": "x.py", "line_start": 1, "line_end": 1, "body": "hi"}
        ).status_code
        == 400
    )
    assert (
        client.post(
            base,
            json={"file": "transform.py", "line_start": 5, "line_end": 2, "body": "hi"},
        ).status_code
        == 400
    )
    assert (
        client.post(
            base,
            json={"file": "transform.py", "line_start": 1, "line_end": 1, "body": "  "},
        ).status_code
        == 400
    )

    try:
        created = client.post(
            base,
            json={
                "file": "transform.py",
                "line_start": 3,
                "line_end": 4,
                "body": "  needs an index  ",
                "release_sha": "deadbeef",
            },
        ).json()
        assert created["id"]
        assert created["body"] == "needs an index"  # trimmed
        assert created["release_sha"] == "deadbeef"
        assert created["line_end"] == 4

        listed = client.get(base).json()
        assert any(c["id"] == created["id"] for c in listed)
        assert client.get(base + "?file=config.py").json() == []

        d = client.delete(f"{base}/{created['id']}")
        assert d.status_code == 200 and d.json()["deleted"] == created["id"]
        # Second delete is a 404 (manual removal is idempotent from the UI's view).
        assert client.delete(f"{base}/{created['id']}").status_code == 404
    finally:
        from poorbricks.code_comments import _coll

        _coll().delete_many({"table_name": table})
