"""Wheel-install boundary smoke test.

Builds the framework as a wheel, installs it into a clean virtualenv, and
runs ``poorbricks-verify --mode local`` against ``test_table_repo/tables/``.
Proves the package import boundary works — i.e. the published wheel
exports everything a downstream repo needs to run ``poorbricks-verify``.

The fixture repo ``test_table_repo/tables/`` intentionally includes a
``missing_contract`` pipeline, so the CLI is expected to exit non-zero with
``"missing_contract"`` in stdout. A non-zero exit without that token, or a
zero exit at all, is the actual failure.

This test does not need MongoDB or PostgreSQL at runtime (the underlying
``poorbricks-verify --mode local`` is offline), so it is marked ``slow``
rather than ``integration``.

Run with:
    poetry run pytest tests/test_wheel_install_boundary.py \
        -m slow -n 0 -v
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

REPO_ROOT = Path(__file__).resolve().parent.parent
TABLES_ROOT = REPO_ROOT / "test_table_repo" / "tables"


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
    """Install the wheel into a temp venv and yield the path to poorbricks-verify."""
    with tempfile.TemporaryDirectory(prefix="poorbricks-sim-") as tmp:
        venv = Path(tmp) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, text=True)
        pip = venv / "bin" / "pip"
        verify = venv / "bin" / "poorbricks-verify"

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

        yield verify


def test_verify_cli_runs_from_installed_wheel_and_reports_missing_contract(
    verify_cli: Path,
) -> None:
    """The installed CLI must run, exit non-zero, and name the broken pipeline."""
    assert TABLES_ROOT.exists(), f"fixture directory missing: {TABLES_ROOT}"

    result = subprocess.run(
        [str(verify_cli), "--mode", "local", "--tables-root", str(TABLES_ROOT)],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0, (
        "poorbricks-verify exited 0 — expected the missing_contract "
        "fixture to cause a non-zero exit.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "missing_contract" in result.stdout, (
        "expected 'missing_contract' in verify stdout.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
