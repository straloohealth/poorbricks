"""Wheel-install boundary smoke test.

Builds the framework as a wheel, installs it into a clean virtualenv, and
runs ``poorbricks verify --mode local`` against ``test_table_repo/tables/``.
Proves the package import boundary works — i.e. the published wheel
exports everything a downstream repo needs to run ``poorbricks verify``.

``verify`` resolves contracts over HTTP from the poorbricks server, so the
test stands up a tiny stub server and points ``--contract-url`` at it. The
stub answers every ``/v1/contracts/*`` lookup with 404, so every fixture
pipeline reports a missing contract — the CLI must exit non-zero and name
the broken pipelines (including ``missing_contract``) in stdout. A zero
exit, or a non-zero exit without that token, is the actual failure.

This test needs neither MongoDB nor PostgreSQL — contract resolution is
fully served by the in-process stub — so it is marked ``slow`` rather
than ``integration``.

Run with:
    poetry run pytest tests/test_wheel_install_boundary.py \
        -m slow -n 0 -v
"""

from __future__ import annotations

import http.server
import shutil
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

REPO_ROOT = Path(__file__).resolve().parent.parent
TABLES_ROOT = REPO_ROOT / "test_table_repo" / "tables"


class _NotFoundContractHandler(http.server.BaseHTTPRequestHandler):
    """Answers every contract lookup with 404 — the framework reads that
    as a missing contract."""

    def do_GET(self) -> None:  # noqa: N802 — name fixed by BaseHTTPRequestHandler
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"detail": "contract not found"}')

    def log_message(self, *args: object) -> None:
        """Silence the default per-request stderr logging."""


@pytest.fixture
def contract_stub_url() -> Iterator[str]:
    """Run a stub poorbricks server that 404s every contract, yield its URL."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _NotFoundContractHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture(scope="session")
def built_wheel() -> Path:
    """Build the framework wheel once per session and return its path."""
    dist_dir = REPO_ROOT / "dist"
    if dist_dir.exists():
        shutil.rmtree(dist_dir)

    subprocess.run(
        ["poetry", "build", "-f", "wheel"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
    )

    wheels = list(dist_dir.glob("poorbricks_framework-*.whl"))
    assert wheels, f"no wheel produced under {dist_dir}"
    return wheels[0]


@pytest.fixture
def verify_cli(built_wheel: Path) -> Iterator[Path]:
    """Install the wheel into a temp venv and yield the path to the poorbricks CLI."""
    with tempfile.TemporaryDirectory(prefix="poorbricks-sim-") as tmp:
        venv = Path(tmp) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, text=True)
        pip = venv / "bin" / "pip"
        cli = venv / "bin" / "poorbricks"

        subprocess.run(
            [str(pip), "install", "--quiet", str(built_wheel)],
            check=True,
            text=True,
        )
        subprocess.run(
            [str(pip), "install", "--quiet", "pyspark==4.0.0", "pyyaml"],
            check=True,
            text=True,
        )

        yield cli


def test_verify_cli_runs_from_installed_wheel_and_reports_missing_contract(
    verify_cli: Path, contract_stub_url: str
) -> None:
    """The installed CLI must run, exit non-zero, and name the broken pipeline."""
    assert TABLES_ROOT.exists(), f"fixture directory missing: {TABLES_ROOT}"

    result = subprocess.run(
        [
            str(verify_cli),
            "verify",
            "--mode",
            "local",
            "--tables-root",
            str(TABLES_ROOT),
            "--contract-url",
            contract_stub_url,
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, (
        "poorbricks verify exited 0 — expected the missing_contract "
        "fixture to cause a non-zero exit.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "missing_contract" in result.stdout, (
        "expected 'missing_contract' in verify stdout.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
