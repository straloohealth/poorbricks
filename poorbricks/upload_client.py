"""``poorbricks upload`` — tarball ``tables/`` + ``workflows/`` and POST it
to a framework-repo API server.

The server runs the full verification suite (``verify_local``,
``verify_ci``), profiles the output, generates Airflow DAGs, and uploads
them to the configured DAG store. This CLI just packages the code and
blocks on the response.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass
class UploadResult:
    ok: bool
    status_code: int
    body: dict[str, Any]


def upload(
    server_url: str,
    prefix: str,
    sha: str,
    tables_dir: Path,
    workflows_dir: Path,
    timeout: float = 600.0,
    poll_interval: float = 30.0,
    max_wait: float = 7200.0,
) -> UploadResult:
    """POST a tarball of ``tables/`` + ``workflows/`` to ``server_url``.

    Blocks until the server responds (or ``timeout`` elapses). Retries
    automatically when the server returns 503 (busy) or 502 (gateway
    timeout during a concurrent upload), waiting ``poll_interval`` seconds
    between attempts up to ``max_wait`` total seconds.
    """
    if not tables_dir.is_dir():
        raise FileNotFoundError(f"tables-dir not found: {tables_dir}")
    if not workflows_dir.is_dir():
        raise FileNotFoundError(f"workflows-dir not found: {workflows_dir}")

    tarball = _build_tarball(tables_dir=tables_dir, workflows_dir=workflows_dir)
    url = server_url.rstrip("/") + "/v1/upload"
    waited = 0.0
    while True:
        with httpx.Client(timeout=timeout) as client:
            response = client.post(
                url,
                data={"prefix": prefix, "sha": sha},
                files={
                    "code": ("code.tar.gz", io.BytesIO(tarball), "application/gzip")
                },
            )
        if response.status_code in (502, 503) and waited < max_wait:
            print(
                f"Server busy (HTTP {response.status_code}), "
                f"retrying in {poll_interval:.0f}s "
                f"(waited {waited:.0f}s / {max_wait:.0f}s max)…",
                file=sys.stderr,
            )
            time.sleep(poll_interval)
            waited += poll_interval
            continue
        body: dict[str, Any]
        try:
            body = response.json()
        except json.JSONDecodeError:
            body = {"raw": response.text}
        return UploadResult(
            ok=response.is_success,
            status_code=response.status_code,
            body=body,
        )


def _build_tarball(*, tables_dir: Path, workflows_dir: Path) -> bytes:
    """Produce an in-memory ``tar.gz`` with ``tables/`` and ``workflows/``
    at the archive root."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(tables_dir, arcname="tables", filter=_skip_pycache)
        tar.add(workflows_dir, arcname="workflows", filter=_skip_pycache)
    return buf.getvalue()


def _skip_pycache(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
    parts = Path(tarinfo.name).parts
    if "__pycache__" in parts or any(p.endswith(".pyc") for p in parts):
        return None
    return tarinfo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="poorbricks upload",
        description=(
            "Tarball tables/+workflows/ and POST to a framework-repo server "
            "for verification and DAG generation."
        ),
    )
    parser.add_argument("--server", required=True, help="server base URL")
    parser.add_argument("--prefix", required=True, help="repo namespace")
    parser.add_argument("--sha", required=True, help="git SHA of the table-repo")
    parser.add_argument("--tables-dir", type=Path, default=Path("tables"))
    parser.add_argument("--workflows-dir", type=Path, default=Path("workflows"))
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=30.0,
        help="seconds to wait between retries when server is busy (default: 30)",
    )
    parser.add_argument(
        "--max-wait",
        type=float,
        default=7200.0,
        help="maximum total seconds to wait for a busy server before giving up (default: 7200)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help=(
            "after a successful upload, trigger one manual DAG run per "
            "uploaded workflow, poll until terminal, and print a per-task "
            "summary (with log excerpts when available)"
        ),
    )
    parser.add_argument(
        "--airflow-url",
        default=None,
        help=(
            "Airflow webserver base URL (only used with --watch). Defaults to "
            "the company airflow ingress."
        ),
    )
    parser.add_argument(
        "--watch-poll-interval",
        type=float,
        default=None,
        help="seconds between Airflow API polls when --watch (default: 15)",
    )
    parser.add_argument(
        "--watch-timeout",
        type=float,
        default=None,
        help="max seconds to wait for each DAG run to finish (default: 3600)",
    )
    args = parser.parse_args(argv)

    try:
        result = upload(
            server_url=args.server,
            prefix=args.prefix,
            sha=args.sha,
            tables_dir=args.tables_dir,
            workflows_dir=args.workflows_dir,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
            max_wait=args.max_wait,
        )
    except (FileNotFoundError, httpx.HTTPError) as exc:
        print(f"✗ upload failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result.body, indent=2, default=str))
    if not result.ok:
        print(f"\n✗ server returned {result.status_code}", file=sys.stderr)
        return 1
    print(f"\n✓ uploaded ({result.status_code})")

    if args.watch:
        return _watch_after_upload(args, result)
    return 0


def _watch_after_upload(args: argparse.Namespace, result: "UploadResult") -> int:
    """Trigger + poll a manual DAG run per workflow in the upload response.

    Returns 0 only if every triggered run reaches state ``success``. Any
    other terminal state (``failed``, ``upstream_failed``, ``timeout``,
    ``error``) yields a non-zero exit so this is wired correctly into CI.
    """
    from .airflow.watch import (
        DEFAULT_AIRFLOW_URL,
        DEFAULT_POLL_INTERVAL_S,
        DEFAULT_TIMEOUT_S,
        attach_failed_task_logs,
        poll_run,
        render_run,
        trigger_dag_run,
    )

    airflow_url = args.airflow_url or DEFAULT_AIRFLOW_URL
    poll_interval = args.watch_poll_interval or DEFAULT_POLL_INTERVAL_S
    watch_timeout = args.watch_timeout or DEFAULT_TIMEOUT_S

    workflows = result.body.get("workflows") or []
    if not workflows:
        print("\n[watch] no workflows in upload response — nothing to watch.")
        return 0

    print(
        f"\n[watch] triggering {len(workflows)} DAG run(s) at {airflow_url} "
        f"(poll every {poll_interval:.0f}s, timeout {watch_timeout:.0f}s)"
    )
    triggered: list[tuple[str, str]] = []  # (dag_id, run_id)
    for wf in workflows:
        dag_id = f"{args.prefix}__{wf['name']}"
        try:
            run_id = trigger_dag_run(airflow_url, dag_id)
        except Exception as exc:  # noqa: BLE001 - surface every trigger error
            print(f"  ! failed to trigger {dag_id}: {exc}", file=sys.stderr)
            continue
        print(f"  - {dag_id} → {run_id}")
        triggered.append((dag_id, run_id))

    if not triggered:
        print("\n[watch] no runs triggered.", file=sys.stderr)
        return 1

    exit_code = 0
    for dag_id, run_id in triggered:

        def _tick(elapsed: float, state: str, _dag_id: str = dag_id) -> None:
            print(f"    {_dag_id}: {state}  ({elapsed:.0f}s)", flush=True)

        outcome = poll_run(
            airflow_url,
            dag_id,
            run_id,
            poll_interval=poll_interval,
            timeout=watch_timeout,
            on_tick=_tick,
        )
        if outcome.failed_tasks:
            attach_failed_task_logs(airflow_url, outcome)
        print("")
        print(render_run(outcome))
        if outcome.state != "success":
            exit_code = 1

    return exit_code


__all__ = ["UploadResult", "main", "upload"]


if __name__ == "__main__":
    sys.exit(main())
