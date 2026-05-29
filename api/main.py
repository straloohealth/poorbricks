"""FastAPI app.

Single endpoint ``POST /v1/upload`` does:

1. Extract the uploaded tarball into a temp dir.
2. ``verify_local`` → 422 on any contract mismatch.
3. Parse ``workflows/*.yaml`` → generate DAG source.
4. ``DagStore.put`` for each workflow.
5. ``DagStore.prune`` so deletions in the table-repo propagate.
6. Return JSON with the generated DAG names and pruned names.

Spark fixture tests (verify_ci) run in the consumer repo's CI before upload
and are not repeated here to avoid long-running requests that timeout the
Tailscale HTTP proxy.
"""

from __future__ import annotations

import io
import json
import re
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from poorbricks.airflow import (
    LocalDagStore,
    WorkflowConfig,
    WorkflowParseError,
    generate_dag_file,
    load_workflows,
)
from poorbricks.airflow.dag_parser import parse_generated_dag
from poorbricks.airflow.dag_store import DagStore

from .config import ApiSettings, settings

app = FastAPI(title="poorbricks-server", version="0.1.0")

_PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SHA_RE = re.compile(r"^[A-Za-z0-9_.\-/]+$")
_DAG_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
_upload_lock = threading.Lock()
_upload_status: dict[str, Any] = {
    "phase": "idle",
    "prefix": None,
    "sha": None,
    "started_at": None,
}


def _set_phase(phase: str, **extra: Any) -> None:
    """Record the current upload phase so GET /v1/status reports live progress."""
    _upload_status["phase"] = phase
    _upload_status.update(extra)
    print(f"[upload] phase: {phase}", flush=True)


# In-memory cache for GET /v1/db-contract. Inferring a contract is expensive
# (sample 1000 docs + infer schema + synthesize rows), so each
# (db, collection, sample_size) result is cached for 24h. Process-local is
# sufficient — the API deployment runs a single replica.
_MONGO_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_DB_CONTRACT_TTL_SECONDS = 24 * 60 * 60
_DB_CONTRACT_MAX_SAMPLE = 5000
_DB_CONTRACT_EXAMPLE_ROWS = 25
_db_contract_cache: dict[tuple[str, str, int], tuple[float, dict[str, Any]]] = {}
_db_contract_lock = threading.Lock()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/status")
def status() -> dict[str, Any]:
    return {"busy": _upload_lock.locked(), **_upload_status}


@app.get("/v1/contracts")
def list_contracts_endpoint() -> list[dict[str, Any]]:
    from utils.contracts import list_contracts

    return list_contracts()


@app.get("/v1/contracts/{table_name}")
def get_contract_endpoint(table_name: str) -> dict[str, Any]:
    from utils.contracts import fetch_contract_from_mongo

    try:
        return fetch_contract_from_mongo(table_name)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Contract {table_name!r} not found"
        )


@app.get("/v1/db-contract")
async def db_contract_endpoint(
    db: str, collection: str, sample_size: int = 1000
) -> dict[str, Any]:
    """Infer a contract from a live MongoDB collection.

    Samples up to ``sample_size`` random documents from ``db.collection``,
    infers a native-format schema + per-field profile, and generates
    *synthetic* example rows from that profile — no real document value is
    ever returned. Results are cached in memory for 24h.
    """
    from utils.mongo_sample import EmptyCollectionError

    _validate_mongo_name(db, "db")
    _validate_mongo_name(collection, "collection")
    size = max(1, min(sample_size, _DB_CONTRACT_MAX_SAMPLE))
    try:
        return await run_in_threadpool(_get_or_build_db_contract, db, collection, size)
    except EmptyCollectionError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — surface any Mongo/inference failure
        raise HTTPException(
            status_code=503,
            detail=f"could not build db-contract for {db}.{collection}: {exc}",
        )


@app.get("/v1/stats")
def stats_endpoint() -> dict[str, Any]:
    """Postgres warehouse stats — per-table row counts and on-disk sizes.

    Read-only snapshot used to verify that pipelines are materialising rows.
    """
    from utils.postgres import PostgresInspector

    inspector = PostgresInspector()
    snapshots = inspector.inspect(sample_size=0)
    return {
        "server": inspector.server_info(),
        "table_count": len(snapshots),
        "total_rows": sum(s.row_count for s in snapshots),
        "tables": [
            {
                "schema": s.schema,
                "name": s.name,
                "row_count": s.row_count,
                "size_bytes": s.size_bytes,
            }
            for s in snapshots
        ],
    }


@app.get("/v1/runs")
def runs_endpoint(limit: int = 100) -> list[dict[str, Any]]:
    """Recent pipeline runs from the run-history store (web-debug timeline).

    Read-only. Returns the most recent runs across all pipelines, newest
    first, so the UI can show what ran, when, and with what outcome.
    """
    from dataclasses import asdict

    from poorbricks.run_history import RunHistoryStore

    bounded = max(1, min(limit, 1000))
    records = RunHistoryStore().recent(limit=bounded)
    out: list[dict[str, Any]] = []
    for rec in records:
        row = asdict(rec)
        # Datetimes → ISO strings so the payload is JSON-serializable.
        for key in ("started_at", "finished_at"):
            value = row.get(key)
            if value is not None and hasattr(value, "isoformat"):
                row[key] = value.isoformat()
        out.append(row)
    return out


@app.get("/v1/staleness")
def staleness_endpoint() -> list[dict[str, Any]]:
    """Per-pipeline freshness verdicts (ok / overdue / missing) for the dashboard.

    Derives each scheduled pipeline's expected cadence from its stored DAG and
    compares it to the last recorded run — the same logic the staleness monitor
    alerts on.
    """
    from datetime import UTC, datetime

    from poorbricks.run_history import RunHistoryStore
    from poorbricks.staleness import cadences_from_dags, evaluate

    now = datetime.now(UTC)
    dag_store = _build_store(settings)
    cadences = cadences_from_dags(dag_store, now)
    last_finished = {
        key: rec.finished_at
        for key, rec in RunHistoryStore()
        .last_run_per_pipeline(environment="prod")
        .items()
        if rec.finished_at
    }
    return [v.to_dict() for v in evaluate(cadences, last_finished, now)]


@app.post("/v1/dags/{dag_id}/run")
def trigger_dag_endpoint(dag_id: str) -> dict[str, Any]:
    """Trigger a manual Airflow DAG run (web-debug: run a dev DAG and watch it)."""
    from poorbricks.airflow.watch import trigger_dag_run

    if not _DAG_ID_RE.fullmatch(dag_id):
        raise HTTPException(
            status_code=400, detail=f"dag_id must match [A-Za-z0-9_-]+, got {dag_id!r}"
        )
    try:
        run_id = trigger_dag_run(settings.airflow_url, dag_id)
    except Exception as exc:  # noqa: BLE001 — surface the Airflow error to the caller
        raise HTTPException(
            status_code=502, detail=f"failed to trigger {dag_id!r}: {exc}"
        )
    return {"dag_id": dag_id, "run_id": run_id, "airflow_url": settings.airflow_url}


@app.get("/v1/table/{schema}/{name}")
def table_preview_endpoint(schema: str, name: str, limit: int = 50) -> dict[str, Any]:
    """Preview rows from a warehouse table (web-debug: navigate dev results).

    ``schema`` may be a dev schema like ``silver__dev``. Identifiers are strictly
    validated since they are interpolated into the query.
    """
    from dataclasses import asdict

    from utils.postgres import PostgresInspector

    if not _IDENT_RE.fullmatch(schema) or not _IDENT_RE.fullmatch(name):
        raise HTTPException(
            status_code=400, detail="schema/name must match [A-Za-z0-9_]+"
        )
    bounded = max(1, min(limit, 500))
    try:
        snapshot = PostgresInspector().sample_table(schema, name, bounded)
    except Exception as exc:  # noqa: BLE001 — table missing / unreadable → 404
        raise HTTPException(
            status_code=404, detail=f"could not read {schema}.{name}: {exc}"
        )
    return asdict(snapshot)


@app.post("/v1/upload")
async def upload(
    prefix: str = Form(...),
    sha: str = Form(...),
    code: UploadFile = File(...),
    environment: str = Form("prod"),
) -> JSONResponse:
    _validate_prefix(prefix)
    _validate_sha(sha)
    if environment not in ("prod", "dev"):
        raise HTTPException(
            status_code=400,
            detail=f"environment must be 'prod' or 'dev', got {environment!r}",
        )
    if not _upload_lock.acquire(blocking=False):
        return JSONResponse(
            {"ok": False, "message": "server busy: another upload is in progress"},
            status_code=503,
        )
    try:
        payload = await code.read()
        result = await run_in_threadpool(
            _handle_upload, prefix, sha, payload, settings, environment
        )
    except Exception as exc:  # never return an empty-body 500
        import traceback

        return JSONResponse(
            {
                "ok": False,
                "message": f"internal error during upload: {exc}",
                "phase": _upload_status.get("phase"),
                "traceback": traceback.format_exc().splitlines()[-12:],
            },
            status_code=500,
        )
    finally:
        _set_phase("idle")
        _upload_lock.release()
    status_code = 200 if result.get("ok") else 422
    return JSONResponse(result, status_code=status_code)


@app.get("/v1/code/{prefix}")
async def get_code(prefix: str) -> Response:
    """Stream the table-code tarball a worker init container downloads.

    Serves the ``tables/`` tree ``_publish_code_to_pvc`` persisted at
    ``{dags_dir}/{code_pvc_root}/{prefix}/tables`` as a gzip tarball, so a
    worker pod fetches its code over HTTP instead of mounting the RWO PVC.
    """
    _validate_prefix(prefix)
    code_dir = Path(settings.dags_dir) / settings.code_pvc_root / prefix / "tables"
    if not code_dir.is_dir():
        raise HTTPException(
            status_code=404, detail=f"no code published for prefix {prefix!r}"
        )
    try:
        payload = await run_in_threadpool(_build_code_tarball, code_dir)
    except FileNotFoundError:
        # _publish_code_to_pvc swaps the tree via os.rename; a request landing
        # in that sub-millisecond gap sees the directory vanish mid-tar.
        raise HTTPException(
            status_code=404, detail=f"no code published for prefix {prefix!r}"
        )
    return Response(content=payload, media_type="application/gzip")


@app.post("/v1/regenerate")
async def regenerate() -> JSONResponse:
    """Re-render every stored DAG in place with the current dag_generator.

    Migrates already-uploaded DAGs onto a new worker pod spec without
    re-uploading any table repo: each stored DAG already carries its full
    task graph, so it is parsed back to a workflow and re-rendered against the
    current worker image. A DAG that fails to parse or render is reported
    under ``failed`` and left untouched, so one bad DAG never aborts the batch.
    """
    if not _upload_lock.acquire(blocking=False):
        return JSONResponse(
            {"ok": False, "message": "server busy: another upload is in progress"},
            status_code=503,
        )
    try:
        result = await run_in_threadpool(_regenerate_all, settings)
    finally:
        _upload_lock.release()
    return JSONResponse(result)


def _validate_prefix(prefix: str) -> None:
    if not prefix or not _PREFIX_RE.fullmatch(prefix):
        raise HTTPException(
            status_code=400,
            detail=f"prefix must match [A-Za-z0-9_-]+, got {prefix!r}",
        )


def _validate_sha(sha: str) -> None:
    if not sha or not _SHA_RE.fullmatch(sha):
        raise HTTPException(
            status_code=400,
            detail=f"sha contains invalid characters: {sha!r}",
        )


def _validate_mongo_name(value: str, label: str) -> None:
    if not value or not _MONGO_NAME_RE.fullmatch(value):
        raise HTTPException(
            status_code=400,
            detail=f"{label} must match [A-Za-z0-9_.-]+, got {value!r}",
        )


def _get_or_build_db_contract(
    db: str, collection: str, sample_size: int
) -> dict[str, Any]:
    """Return a cached db-contract, or build + cache a fresh one (24h TTL)."""
    key = (db, collection, sample_size)
    now = time.time()
    with _db_contract_lock:
        cached = _db_contract_cache.get(key)
        if cached is not None and now - cached[0] < _DB_CONTRACT_TTL_SECONDS:
            return {**cached[1], "cache": "hit"}
    # Sample + infer + synthesize outside the lock — a cold-key double-build
    # race is harmless (last write wins) and the slow work must not block
    # concurrent requests for other collections.
    contract = _build_db_contract(db, collection, sample_size)
    with _db_contract_lock:
        _db_contract_cache[key] = (time.time(), contract)
    return {**contract, "cache": "miss"}


def _build_db_contract(db: str, collection: str, sample_size: int) -> dict[str, Any]:
    """Sample the live collection, infer its schema, generate synthetic rows."""
    from poorbricks.settings import settings as fw_settings
    from utils.mongo_sample import sample_random_docs
    from utils.schema_infer import infer
    from utils.synth_data import generate

    docs = sample_random_docs(fw_settings.mongo_uri, db, collection, sample_size)
    result = infer(docs)
    example_rows = generate(result.struct, result.profile, n=_DB_CONTRACT_EXAMPLE_ROWS)
    return {
        "db": db,
        "collection": collection,
        "schema_json": result.struct.jsonValue(),
        "example_rows": example_rows,
        "field_profile": result.profile,
        "sampled_count": len(docs),
        "inferred_at": datetime.now(UTC).isoformat(),
        "warnings": result.warnings,
    }


def _dag_target_kwargs(cfg: ApiSettings, environment: str) -> dict[str, Any]:
    """Resolve the env-specific ``generate_dag_file`` kwargs.

    A dev upload writes to the dev Postgres target under the dev schema suffix
    and fast-fails (no retries); a prod upload uses the prod target + retries.
    """
    common = {
        "postgres_port": cfg.postgres_port,
        "contracts_api_url": cfg.contracts_api_url,
        "retry_delay_minutes": cfg.worker_retry_delay_minutes,
    }
    if environment == "dev":
        return {
            **common,
            "environment": "dev",
            "postgres_host": cfg.dev_postgres_host or cfg.postgres_host,
            "postgres_db": cfg.dev_postgres_db or cfg.postgres_db,
            "schema_suffix": cfg.dev_schema_suffix,
            "retries": 0,
        }
    return {
        **common,
        "environment": "prod",
        "postgres_host": cfg.postgres_host,
        "postgres_db": cfg.postgres_db,
        "schema_suffix": "",
        "retries": cfg.worker_retries,
    }


def _handle_upload(
    prefix: str,
    sha: str,
    payload: bytes,
    cfg: ApiSettings,
    environment: str = "prod",
) -> dict[str, Any]:
    """Synchronous upload pipeline (runs in a worker thread).

    Each phase is recorded into the shared upload status (surfaced live by
    GET /v1/status) and into a ``phases`` trail returned in the response, so a
    slow or failed upload always shows how far it got and where it broke.

    A ``dev`` upload is namespaced to a ``dev-<prefix>`` DAG prefix and writes
    to a dev Postgres schema, so it runs on the shared Airflow without touching
    prod DAGs or tables.
    """
    import time

    effective_prefix = f"dev-{prefix}" if environment == "dev" else prefix
    target_kwargs = _dag_target_kwargs(cfg, environment)
    phases: list[dict[str, Any]] = []

    def begin(name: str) -> None:
        now = time.monotonic()
        if phases:
            phases[-1]["seconds"] = round(now - phases[-1]["start"], 2)
        phases.append({"name": name, "start": now})
        _set_phase(name, prefix=prefix, sha=sha)

    def trail() -> list[dict[str, Any]]:
        if phases:
            phases[-1].setdefault(
                "seconds", round(time.monotonic() - phases[-1]["start"], 2)
            )
        return [{"name": p["name"], "seconds": p.get("seconds", 0.0)} for p in phases]

    _set_phase(
        "starting",
        prefix=prefix,
        sha=sha,
        started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )
    _reset_pipeline_module_cache()
    with tempfile.TemporaryDirectory(prefix=f"poorbricks-{prefix}-") as tmp:
        root = Path(tmp)
        begin("extracting")
        _extract_safely(payload, root)
        tables_dir = root / "tables"
        workflows_dir = root / "workflows"

        if not tables_dir.is_dir():
            return _fail(
                "missing 'tables/' directory in uploaded tarball", phases=trail()
            )
        if not workflows_dir.is_dir():
            return _fail(
                "missing 'workflows/' directory in uploaded tarball", phases=trail()
            )

        # Local verify — contract schemas only, no Spark.
        # Spark fixture tests run in the consumer repo's CI (tables-test job)
        # before upload, so we skip the expensive verify_ci here.
        from poorbricks.verify import verify_local
        from utils.contracts import fetch_contract_from_mongo

        begin("verify_local")
        # The server owns Mongo access, so it verifies uploads against the
        # store directly instead of round-tripping through its own HTTP API.
        contract_errors = verify_local(
            tables_root=tables_dir, contract_fetcher=fetch_contract_from_mongo
        )
        if contract_errors:
            return _fail(
                "verify_local failed",
                errors=[e.format() for e in contract_errors],
                phases=trail(),
            )

        # Source-file presence gate — defence in depth on top of
        # verify_local. Catches the 2026-05-25 ghost-contract failure
        # mode where source files were deleted in a repo, an upload
        # succeeded with no producers, contracts persisted in Mongo,
        # and every downstream consumer silently read empty rows.
        #
        # Rule: every silver/gold transform.py the upload claims to
        # publish must exist on disk as a non-empty file. verify_local
        # checks schema-shape; this adds the simpler "the file is
        # actually there" check that wasn't enforced before.
        begin("source_presence_gate")
        missing_sources = []
        empty_sources = []
        for tdir in tables_dir.rglob("config.py"):
            tname = tdir.parent.name
            transform_path = tdir.parent / "transform.py"
            if not transform_path.exists():
                missing_sources.append({"table": tname, "path": str(transform_path)})
                continue
            if transform_path.stat().st_size == 0:
                empty_sources.append({"table": tname, "path": str(transform_path)})
        if missing_sources or empty_sources:
            return _fail(
                "source_presence_gate failed: tarball would publish ghost contracts",
                errors={
                    "missing_transform_files": missing_sources,
                    "empty_transform_files": empty_sources,
                },
                phases=trail(),
            )

        begin("parsing_workflows")
        try:
            workflows = load_workflows(workflows_dir)
        except WorkflowParseError as exc:
            return _fail(f"workflow parse error: {exc}", phases=trail())
        except (FileNotFoundError, NotADirectoryError) as exc:
            return _fail(str(exc), phases=trail())

        begin("generating_dags")
        dag_store = _build_store(cfg)
        _publish_code_to_pvc(
            tables_dir=tables_dir,
            dags_dir=Path(cfg.dags_dir),
            code_root=cfg.code_pvc_root,
            prefix=effective_prefix,
        )
        dag_names: list[str] = []
        for wf in workflows:
            content = generate_dag_file(
                wf,
                prefix=effective_prefix,
                image=wf.image or cfg.worker_image,
                namespace=cfg.worker_namespace,
                runtime_secret=cfg.runtime_secret_name,
                sha=sha,
                **target_kwargs,
            )
            dag_store.put(effective_prefix, wf.name, content)
            dag_names.append(wf.name)

        pruned = dag_store.prune(effective_prefix, keep=set(dag_names))

        # Reconcile contracts: a pipeline removed from the repo must not leave
        # a ghost contract behind. Delete this prefix's orphaned contracts,
        # but never one still consumed by another repo (kept + warned).
        begin("reconcile_contracts")
        contract_cleanup = _reconcile_contracts(
            effective_prefix, _published_tables(workflows)
        )

        return {
            "ok": True,
            "prefix": prefix,
            "effective_prefix": effective_prefix,
            "environment": environment,
            "sha": sha,
            "dag_names": sorted(dag_names),
            "pruned": pruned,
            "removed_contracts": contract_cleanup["deleted"],
            "contract_warnings": contract_cleanup["warnings"],
            "workflows": [_serialize(wf) for wf in workflows],
            "phases": trail(),
        }


def _published_tables(workflows: Iterable[WorkflowConfig]) -> set[str]:
    """The set of table names this upload publishes.

    Union of (a) the registry populated by ``verify_local``'s discovery over the
    extracted tree and (b) the table names referenced by workflow task pipeline
    keys (``"<storage>:<table>"`` → ``"<table>"``) — belt and suspenders.
    """
    tables: set[str] = set()
    try:
        from poorbricks.registry import all_pipelines

        tables.update(meta.table_name for meta in all_pipelines().values())
    except Exception:  # noqa: BLE001 — registry is best-effort here
        pass
    for wf in workflows:
        for task in wf.tasks:
            key = task.pipeline
            tables.add(key.split(":", 1)[1] if ":" in key else key)
    return tables


def _external_consumers(details: list[dict[str, Any]], prefix: str) -> set[str]:
    """Tables consumed by contracts owned by a DIFFERENT prefix.

    A contract still consumed elsewhere (via declared ``inputs`` or captured
    ``lineage.consumed``) must never be deleted out from under that consumer.
    """
    consumers: set[str] = set()
    for doc in details:
        if doc.get("prefix") == prefix:
            continue
        for inp in doc.get("inputs", []) or []:
            table = inp.get("table_name")
            if table:
                consumers.add(table)
        consumed = (doc.get("lineage") or {}).get("consumed") or {}
        consumers.update(consumed.keys())
    return consumers


def _reconcile_contracts(prefix: str, keep_published: set[str]) -> dict[str, Any]:
    """Delete this prefix's orphaned contracts; keep+warn ones used elsewhere."""
    from utils.contracts import list_contract_details, prune_contracts

    if not keep_published:
        # Could not determine what this upload publishes — refuse to mass-delete.
        return {"deleted": [], "warnings": ["skipped: no published tables resolved"]}

    details = list_contract_details()
    owned = {d["table_name"] for d in details if d.get("prefix") == prefix}
    consumed_elsewhere = _external_consumers(details, prefix)
    still_consumed = sorted((owned - keep_published) & consumed_elsewhere)
    # Adding still-consumed tables to `keep` protects them from prune.
    deleted = prune_contracts(prefix, keep_published | consumed_elsewhere)
    warnings = [
        f"{t}: removed from repo but still consumed by another repo — kept"
        for t in still_consumed
    ]
    return {"deleted": deleted, "warnings": warnings}


def _build_store(cfg: ApiSettings) -> DagStore:
    return LocalDagStore(root=Path(cfg.dags_dir))


def _build_code_tarball(code_dir: Path) -> bytes:
    """Tar + gzip ``code_dir`` into memory, rooted at ``tables/``."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(code_dir, arcname="tables")
    return buf.getvalue()


def _regenerate_all(cfg: ApiSettings) -> dict[str, Any]:
    """Re-render every stored DAG in place; one bad DAG never aborts the batch."""
    dag_store = _build_store(cfg)
    regenerated: list[str] = []
    failed: list[dict[str, str]] = []
    for prefix in dag_store.list_prefixes():
        # A dev DAG keeps its dev target + suffix on regenerate; derive it from
        # the ``dev-`` prefix (the new env constants need not be parsed back).
        environment = "dev" if prefix.startswith("dev-") else "prod"
        target_kwargs = _dag_target_kwargs(cfg, environment)
        for name in dag_store.list_dags(prefix):
            dag_ref = f"{prefix}/{name}"
            try:
                parsed = parse_generated_dag(dag_store.get(prefix, name))
                workflow = WorkflowConfig(
                    name=name,
                    schedule=parsed.schedule,
                    tasks=parsed.tasks,
                )
                # Bake the *current* worker image, not the one parsed from the
                # stored DAG: a migration target is precisely a new image (e.g.
                # the first one carrying the fetch-code init-container module).
                # Keeping the parsed image would re-render a DAG that cannot
                # start. ``parsed.image`` is intentionally not used here.
                content = generate_dag_file(
                    workflow,
                    prefix=prefix,
                    image=cfg.worker_image,
                    namespace=parsed.namespace,
                    runtime_secret=parsed.runtime_secret,
                    postgres_creds_secret=parsed.postgres_creds_secret,
                    start_year=parsed.start_year,
                    **target_kwargs,
                )
                dag_store.put(prefix, name, content)
                regenerated.append(dag_ref)
            except Exception as exc:  # noqa: BLE001 — record + skip one bad DAG
                failed.append({"dag": dag_ref, "error": str(exc)})
    return {
        "ok": True,
        "regenerated": sorted(regenerated),
        "failed": failed,
        "count": len(regenerated),
    }


def _publish_code_to_pvc(
    *, tables_dir: Path, dags_dir: Path, code_root: str, prefix: str
) -> None:
    """Atomically swap the extracted ``tables/`` tree into
    ``{dags_dir}/{code_root}/{prefix}/tables``.

    Worker pods no longer mount this PVC: the api-server serves the tree
    back to their ``fetch-code`` init containers via ``GET /v1/code/{prefix}``.
    The persisted tree is also what ``POST /v1/regenerate`` relies on staying
    available between uploads.

    The swap is done via ``os.rename`` of two sibling directories under the
    same code_root so any reader landing mid-upload sees either the old code
    or the new code — never a half-written tree.
    """
    import os
    import shutil
    import uuid

    code_root_dir = dags_dir / code_root
    code_root_dir.mkdir(parents=True, exist_ok=True)
    final_dir = code_root_dir / prefix
    staging_dir = code_root_dir / f".{prefix}.staging.{uuid.uuid4().hex}"
    old_dir = code_root_dir / f".{prefix}.old.{uuid.uuid4().hex}"

    staging_dir.mkdir()
    shutil.copytree(tables_dir, staging_dir / "tables")

    if final_dir.exists():
        os.rename(final_dir, old_dir)
    os.rename(staging_dir, final_dir)
    if old_dir.exists():
        shutil.rmtree(old_dir, ignore_errors=True)


def _extract_safely(payload: bytes, dest: Path) -> None:
    """Extract a tarball, rejecting any path that escapes ``dest``."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        for member in tar.getmembers():
            target = (dest / member.name).resolve()
            if not str(target).startswith(str(dest.resolve())):
                raise HTTPException(
                    status_code=400,
                    detail=f"tarball contains unsafe path: {member.name!r}",
                )
        tar.extractall(dest)


def _load_profiles(profiles_dir: Path) -> dict[str, Any]:
    if not profiles_dir.exists():
        return {}
    out: dict[str, Any] = {}
    for path in sorted(profiles_dir.glob("*.json")):
        try:
            out[path.stem] = json.loads(path.read_text())
        except json.JSONDecodeError:
            out[path.stem] = {"error": "invalid_json"}
    return out


def _serialize(obj: Any) -> Any:
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, Iterable) and not isinstance(obj, str | bytes | dict):
        return [_serialize(x) for x in obj]
    return obj


def _fail(
    message: str,
    *,
    errors: list[str] | dict[str, Any] | None = None,
    phases: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "message": message, "errors": errors or []}
    if phases is not None:
        out["phases"] = phases
    return out


def _reset_pipeline_module_cache() -> None:
    """Clear cached imports + sys.path entries from prior uploads.

    Each upload extracts code into a fresh tmp dir and adds the parent to
    ``sys.path``. Without clearing, ``importlib`` returns stale module
    specs whose ``origin`` points at deleted directories, and downstream
    arch checks then report every required file as missing.

    Also clears the in-process pipeline registry so re-uploads don't keep
    decorators from previous requests.
    """
    for mod_name in list(sys.modules):
        if mod_name == "tables" or mod_name.startswith("tables."):
            del sys.modules[mod_name]
    sys.path[:] = [p for p in sys.path if not p.startswith("/tmp/poorbricks-")]
    from poorbricks.registry import _pipelines, _scenarios

    _pipelines.clear()
    _scenarios.clear()


# Re-export for tests / programmatic use.
__all__ = ["WorkflowConfig", "app"]
