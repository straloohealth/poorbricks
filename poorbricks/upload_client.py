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
    return 0


__all__ = ["UploadResult", "main", "upload"]
