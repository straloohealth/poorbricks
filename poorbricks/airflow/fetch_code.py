"""Init-container entry point: download the table-repo tarball into ``/workspace``.

Run as ``python -m poorbricks.airflow.fetch_code`` in a worker pod's init
container. Reads ``CODE_TARBALL_URL`` (the api-server's ``/v1/code/{prefix}``
endpoint) and extracts the gzip tarball into ``WORKSPACE_DIR`` (default
``/workspace``) so the main container sees ``/workspace/tables`` and the
framework resolves pipelines via ``TABLES_ROOT``.

This decouples worker pods from the shared ``airflow-dags`` PVC, letting them
run on any node.
"""

from __future__ import annotations

import io
import os
import sys
import tarfile
import time
import urllib.request

_MAX_ATTEMPTS = 5
_RETRY_DELAY_SECONDS = 5
_HTTP_TIMEOUT_SECONDS = 120


def _download(url: str) -> bytes:
    """Download ``url``, retrying transient failures with a fixed backoff."""
    last_error: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
                return bytes(resp.read())
        except Exception as exc:  # noqa: BLE001 — any failure here is retryable
            last_error = exc
            print(
                f"fetch_code: attempt {attempt}/{_MAX_ATTEMPTS} failed: {exc}",
                flush=True,
            )
            if attempt < _MAX_ATTEMPTS:
                time.sleep(_RETRY_DELAY_SECONDS)
    raise RuntimeError(f"could not download {url}: {last_error}")


def _extract(payload: bytes, dest: str) -> None:
    """Extract a gzip tarball into ``dest``, rejecting unsafe member paths."""
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        tar.extractall(dest, filter="data")


def main() -> int:
    """Fetch + extract the table code; return a process exit code."""
    url = os.environ.get("CODE_TARBALL_URL")
    if not url:
        print("fetch_code: CODE_TARBALL_URL is not set", flush=True)
        return 1
    dest = os.environ.get("WORKSPACE_DIR", "/workspace")
    try:
        payload = _download(url)
        _extract(payload, dest)
    except Exception as exc:  # noqa: BLE001 — surface any failure as exit code 1
        print(f"fetch_code: {exc}", flush=True)
        return 1
    print(f"fetch_code: extracted {len(payload)} bytes into {dest}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
