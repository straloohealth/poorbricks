"""Simulate a downstream repo installing poorbricks-framework as a wheel.

Builds the framework as a wheel, creates a temp virtualenv, installs the
wheel into it, and runs ``poorbricks-verify --mode local`` against
``test_table_repo/tables/``. Proves the package import boundary works.

Usage::

    poetry run python scripts/simulate_table_repo.py

Exit code 0 if discovery + verify both function (the verify command
itself may exit non-zero because the test fixtures include intentional
failures; we only assert that the CLI ran and produced output).
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TABLES_ROOT = REPO_ROOT / "test_table_repo" / "tables"


def _run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    print(f"+ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, text=True, **kwargs)  # type: ignore[arg-type]


def main() -> int:
    if not TABLES_ROOT.exists():
        print(f"ERROR: {TABLES_ROOT} does not exist", file=sys.stderr)
        return 2

    dist_dir = REPO_ROOT / "dist"
    if dist_dir.exists():
        shutil.rmtree(dist_dir)

    _run(["poetry", "build", "-f", "wheel"], cwd=REPO_ROOT)

    wheels = list(dist_dir.glob("poorbricks_framework-*.whl"))
    if not wheels:
        print("ERROR: no wheel produced under dist/", file=sys.stderr)
        return 2
    wheel = wheels[0]
    print(f"built wheel: {wheel}")

    with tempfile.TemporaryDirectory(prefix="poorbricks-sim-") as tmp:
        venv = Path(tmp) / "venv"
        _run([sys.executable, "-m", "venv", str(venv)])
        pip = venv / "bin" / "pip"
        verify = venv / "bin" / "poorbricks-verify"

        _run([str(pip), "install", "--quiet", str(wheel)])
        _run([str(pip), "install", "--quiet", "pyspark==4.0.0", "pyyaml"])

        result = subprocess.run(
            [str(verify), "--mode", "local", "--tables-root", str(TABLES_ROOT)],
            text=True,
            capture_output=True,
        )
        print("--- poorbricks-verify stdout ---")
        print(result.stdout)
        print("--- poorbricks-verify stderr ---")
        print(result.stderr)
        print(f"exit code: {result.returncode}")

    if result.returncode == 0:
        print("\nNOTE: verify exited 0 — expected at least the missing_contract failure.")
    else:
        if "missing_contract" not in result.stdout:
            print("ERROR: expected missing_contract failure in output", file=sys.stderr)
            return 1
        print("\n✓ simulation passed — CLI ran from installed wheel and reported expected failure")
    return 0


if __name__ == "__main__":
    sys.exit(main())
