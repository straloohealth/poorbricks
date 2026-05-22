"""Tests for poorbricks.airflow.fetch_code (the worker init-container entry point)."""

from __future__ import annotations

import io
import tarfile
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from poorbricks.airflow import fetch_code


def _make_tarball() -> bytes:
    """Build a gzip tarball holding a ``tables/`` tree, as the api-server serves."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        content = b"print('hello from transform')\n"
        info = tarfile.TarInfo(name="tables/silver/example/transform.py")
        info.size = len(content)
        tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _serve(payload: bytes) -> tuple[HTTPServer, str]:
    """Start a localhost HTTP server returning ``payload`` for any GET."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 — name fixed by BaseHTTPRequestHandler
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args: object) -> None:
            pass  # keep the test output quiet

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    host, port = server.server_address
    return server, f"http://{host}:{port}/v1/code/test"


@pytest.fixture
def code_url() -> Iterator[str]:
    server, url = _serve(_make_tarball())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield url
    finally:
        server.shutdown()
        thread.join()


def test_main_downloads_and_extracts(
    code_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CODE_TARBALL_URL", code_url)
    monkeypatch.setenv("WORKSPACE_DIR", str(tmp_path))
    assert fetch_code.main() == 0
    extracted = tmp_path / "tables" / "silver" / "example" / "transform.py"
    assert extracted.read_text() == "print('hello from transform')\n"


def test_main_missing_url_returns_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CODE_TARBALL_URL", raising=False)
    assert fetch_code.main() == 1


def test_main_unreachable_url_returns_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Port 1 is privileged and unbound — the connection is refused at once.
    monkeypatch.setenv("CODE_TARBALL_URL", "http://127.0.0.1:1/v1/code/test")
    monkeypatch.setenv("WORKSPACE_DIR", str(tmp_path))
    # Drop the retry backoff so the 5 attempts do not slow the test down.
    monkeypatch.setattr(fetch_code, "_RETRY_DELAY_SECONDS", 0)
    assert fetch_code.main() == 1
