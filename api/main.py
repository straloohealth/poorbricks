"""FastAPI app.

Single endpoint ``POST /v1/upload`` does:

1. Extract the uploaded tarball into a temp dir.
2. ``verify_local`` → 422 on any contract mismatch.
3. ``verify_ci`` (fixtures mode) → 422 on rule / expectation failure.
4. Parse ``workflows/*.yaml`` → generate DAG source.
5. ``DagStore.put`` for each workflow.
6. ``DagStore.prune`` so deletions in the table-repo propagate.
7. Return JSON with the generated DAG names, pruned names, and profiles.
"""

from __future__ import annotations

import json
import re
import sys
import tarfile
import tempfile
from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from poorbricks.airflow import (
    LocalDagStore,
    WorkflowConfig,
    WorkflowParseError,
    generate_dag_file,
    load_workflows,
)
from poorbricks.airflow.dag_store import DagStore

from .config import ApiSettings, settings

app = FastAPI(title="poorbricks-server", version="0.1.0")

_PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SHA_RE = re.compile(r"^[A-Za-z0-9_.\-/]+$")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/upload")
async def upload(
    prefix: str = Form(...),
    sha: str = Form(...),
    code: UploadFile = File(...),
) -> JSONResponse:
    _validate_prefix(prefix)
    _validate_sha(sha)
    payload = await code.read()
    result = await run_in_threadpool(_handle_upload, prefix, sha, payload, settings)
    status_code = 200 if result.get("ok") else 422
    return JSONResponse(result, status_code=status_code)


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


def _handle_upload(
    prefix: str, sha: str, payload: bytes, cfg: ApiSettings
) -> dict[str, Any]:
    """Synchronous upload pipeline (runs in a worker thread)."""
    _reset_pipeline_module_cache()
    with tempfile.TemporaryDirectory(prefix=f"poorbricks-{prefix}-") as tmp:
        root = Path(tmp)
        _extract_safely(payload, root)
        tables_dir = root / "tables"
        workflows_dir = root / "workflows"
        profiles_dir = root / "profiles"

        if not tables_dir.is_dir():
            return _fail("missing 'tables/' directory in uploaded tarball")
        if not workflows_dir.is_dir():
            return _fail("missing 'workflows/' directory in uploaded tarball")

        # Local verify — contract schemas only, no Spark.
        from poorbricks.verify import verify_ci, verify_local

        contract_errors = verify_local(tables_root=tables_dir)
        if contract_errors:
            return _fail(
                "verify_local failed",
                errors=[e.format() for e in contract_errors],
            )

        # CI verify — full Spark run against fixtures, exports profiles.
        ci_errors = verify_ci(
            tables_root=tables_dir,
            export_dir=profiles_dir,
            mode="fixtures",
        )
        if ci_errors:
            return _fail(
                "verify_ci failed",
                errors=[e.format() for e in ci_errors],
            )

        # Parse workflows + generate DAGs.
        try:
            workflows = load_workflows(workflows_dir)
        except WorkflowParseError as exc:
            return _fail(f"workflow parse error: {exc}")
        except (FileNotFoundError, NotADirectoryError) as exc:
            return _fail(str(exc))

        dag_store = _build_store(cfg)
        repo_clone_secret = (
            cfg.repo_clone_secret_template.format(prefix=prefix)
            if cfg.use_repo_clone_secret
            else None
        )
        dag_names: list[str] = []
        for wf in workflows:
            content = generate_dag_file(
                wf,
                prefix=prefix,
                image=wf.image or cfg.worker_image,
                table_repo_url=cfg.table_repo_url_template.format(prefix=prefix),
                table_repo_sha=sha,
                namespace=cfg.worker_namespace,
                runtime_secret=cfg.runtime_secret_name,
                repo_clone_secret=repo_clone_secret,
            )
            dag_store.put(prefix, wf.name, content)
            dag_names.append(wf.name)

        pruned = dag_store.prune(prefix, keep=set(dag_names))
        profiles = _load_profiles(profiles_dir)

        return {
            "ok": True,
            "prefix": prefix,
            "sha": sha,
            "dag_names": sorted(dag_names),
            "pruned": pruned,
            "workflows": [_serialize(wf) for wf in workflows],
            "profiles": profiles,
        }


def _build_store(cfg: ApiSettings) -> DagStore:
    return LocalDagStore(root=Path(cfg.dags_dir))


def _extract_safely(payload: bytes, dest: Path) -> None:
    """Extract a tarball, rejecting any path that escapes ``dest``."""
    import io

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


def _fail(message: str, *, errors: list[str] | None = None) -> dict[str, Any]:
    return {"ok": False, "message": message, "errors": errors or []}


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
