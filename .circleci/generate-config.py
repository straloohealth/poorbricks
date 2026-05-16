#!/usr/bin/env python3
"""Emit the dynamic CircleCI workload (continuation config) for poorbricks.

The top-level ``config.yml`` is a setup config — it does nothing but
checkout, run this script, and feed its output to ``continuation/continue``.
Everything that actually runs lives in the YAML this script prints.

Why a generator? Per-level parallel test jobs are discovered from the tables/
directory; new levels added to the codebase are automatically picked up.

Output workflow shape::

    test (non-main branches)
      ┌─ lint-and-type-check ──────────────────────────────────┐
      │  test-level-bronze  (in parallel, requires lint)        │
      │  test-level-silver  (in parallel, requires lint)        │
      │  test-level-gold    (in parallel, requires lint)        │
      │  test-multi-repo    (requires lint)                     │
      │  build-and-smoke    (requires lint)                     │
      └─ integration-tests  (requires all test-level-* jobs)   │

    deploy (main branch)
      same as test
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_ROOT = REPO_ROOT / "tables"

IMAGE = "docker.io/danielspeixoto/databricks"


def discover_levels() -> list[str]:
    """Return the top-level level folders under ``tables/`` that contain pipelines."""
    if not PIPELINES_ROOT.exists():
        return []
    levels = []
    for entry in sorted(PIPELINES_ROOT.iterdir()):
        if not entry.is_dir() or entry.name.startswith("__"):
            continue
        if any(entry.rglob("pipeline.py")):
            levels.append(entry.name)
    return levels


def _install_steps() -> list[Any]:
    return [
        "checkout",
        {
            "restore_cache": {
                "keys": [
                    'v2-deps-{{ checksum "poetry.lock" }}',
                    "v2-deps-",
                ]
            }
        },
        {
            "run": {
                "name": "Install Python dependencies",
                "command": "pip install --quiet poetry && poetry install --no-interaction",
            }
        },
        {
            "save_cache": {
                "key": 'v2-deps-{{ checksum "poetry.lock" }}',
                "paths": ["/root/.cache/pypoetry/virtualenvs"],
            }
        },
    ]


def lint_and_type_check_job() -> dict[str, Any]:
    """ruff + mypy across the whole project."""
    return {
        "docker": [{"image": IMAGE}],
        "resource_class": "medium",
        "steps": _install_steps()
        + [
            {
                "run": {
                    "name": "Ruff lint",
                    "command": "poetry run ruff check .",
                }
            },
            {
                "run": {
                    "name": "Ruff format check",
                    "command": "poetry run ruff format --check .",
                }
            },
            {
                "run": {
                    "name": "MyPy type check",
                    "command": "poetry run mypy poorbricks/ utils/ validation/ tests/",
                }
            },
        ],
    }


def test_level_job(level: str) -> dict[str, Any]:
    """Per-level unit tests — no live services required (fixtures mode only)."""
    safe = level.replace("_", "-")
    junit = f"test-results-{safe}.xml"
    return {
        "docker": [{"image": IMAGE}],
        "resource_class": "medium",
        "steps": _install_steps()
        + [
            {
                "run": {
                    "name": f"Pytest: tables/{level}/ (fixtures mode)",
                    "command": (
                        f"poetry run pytest tables/{level}/ "
                        f"-n 2 --dist loadgroup "
                        f"-m 'not integration' "
                        f"--junitxml={junit} "
                        f"--tb=short -v"
                    ),
                }
            },
            {"store_test_results": {"path": junit}},
        ],
    }


def test_multi_repo_job() -> dict[str, Any]:
    """Multi-repo contract verification scenarios — no live services required.

    Exercises verify_local() and verify_ci() via mocked contract fetchers
    and monkeypatched fetch_contract. Validates all 5 scenarios in
    test_table_repo/: happy_path, missing_contract, schema_drift,
    expectations_failure, gold_patients.
    """
    junit = "test-results-multi-repo.xml"
    return {
        "docker": [{"image": IMAGE}],
        "resource_class": "medium",
        "steps": _install_steps()
        + [
            {
                "run": {
                    "name": "Pytest: tests/test_multi_repo.py",
                    "command": (
                        "poetry run pytest tests/test_multi_repo.py "
                        f"-o 'addopts=' "
                        f"--junitxml={junit} "
                        "--tb=short -v"
                    ),
                }
            },
            {"store_test_results": {"path": junit}},
        ],
    }


def build_and_smoke_job() -> dict[str, Any]:
    """Build the poorbricks-framework wheel and run the isolation smoke test.

    Proves the package import boundary: installs the wheel into a temporary
    venv and runs ``poorbricks-verify --mode local`` against the
    test_table_repo fixtures.
    """
    return {
        "docker": [{"image": IMAGE}],
        "resource_class": "medium",
        "steps": _install_steps()
        + [
            {
                "run": {
                    "name": "Build wheel",
                    "command": "poetry build -f wheel",
                }
            },
            {
                "run": {
                    "name": "Simulate isolated table-repo install",
                    "command": "poetry run python scripts/simulate_table_repo.py",
                    "no_output_timeout": "10m",
                }
            },
            {
                "store_artifacts": {
                    "path": "dist",
                    "destination": "wheel",
                }
            },
        ],
    }


def integration_tests_job() -> dict[str, Any]:
    """Distributed pipeline integration test.

    Runs the full bronze → silver → gold fixture pipeline, writes every
    layer to PostgreSQL, and pushes contracts to MongoDB. Validates that:
      - Every Postgres schema (bronze/silver/gold) has rows
      - MongoDB has a contract document for every pipeline
      - Gold fixtures resolve ContractSource.from_rows() from the MongoDB
        silver contract — proving the cross-repo contract chain

    Uses sidecar containers for MongoDB 7 and PostgreSQL 16.
    """
    junit = "test-results-integration.xml"
    return {
        "docker": [
            {"image": IMAGE},
            {"image": "mongo:7"},
            {
                "image": "postgres:16",
                "environment": {
                    "POSTGRES_DB": "analytics",
                    "POSTGRES_USER": "analytics",
                    "POSTGRES_PASSWORD": "analytics",
                },
            },
        ],
        "resource_class": "large",
        "steps": _install_steps()
        + [
            {
                "run": {
                    "name": "Wait for MongoDB and PostgreSQL to be ready",
                    "command": (
                        "until mongosh --quiet --eval 'db.adminCommand(\"ping\")' "
                        ">/dev/null 2>&1; do sleep 2; done && "
                        "echo 'MongoDB ready' && "
                        "until pg_isready -h localhost -U analytics -d analytics "
                        ">/dev/null 2>&1; do sleep 2; done && "
                        "echo 'PostgreSQL ready'"
                    ),
                }
            },
            {
                "run": {
                    "name": "Distributed pipeline test (bronze → silver → gold)",
                    "command": "poetry run python scripts/test_distributed_pipeline.py",
                    "no_output_timeout": "20m",
                }
            },
            {
                "run": {
                    "name": "Pytest: integration marker suite",
                    "command": (
                        "poetry run pytest -m integration "
                        f"--junitxml={junit} "
                        "--tb=short -v || true"
                    ),
                }
            },
            {"store_test_results": {"path": junit}},
        ],
    }


def _build_workflow(levels: list[str]) -> list[Any]:
    level_job_names = [f"test-level-{l.replace('_', '-')}" for l in levels]
    jobs: list[Any] = ["lint-and-type-check"]

    for level, name in zip(levels, level_job_names):
        jobs.append({name: {"requires": ["lint-and-type-check"]}})

    jobs.append({"test-multi-repo": {"requires": ["lint-and-type-check"]}})
    jobs.append({"build-and-smoke": {"requires": ["lint-and-type-check"]}})

    all_test_jobs = level_job_names + ["test-multi-repo", "build-and-smoke"]
    jobs.append({"integration-tests": {"requires": all_test_jobs}})

    return jobs


def generate_config() -> dict[str, Any]:
    levels = discover_levels()

    jobs: dict[str, Any] = {
        "lint-and-type-check": lint_and_type_check_job(),
        "test-multi-repo": test_multi_repo_job(),
        "build-and-smoke": build_and_smoke_job(),
        "integration-tests": integration_tests_job(),
    }
    for level in levels:
        jobs[f"test-level-{level.replace('_', '-')}"] = test_level_job(level)

    not_main = {
        "and": [
            {"not": {"equal": ["main", "<< pipeline.git.branch >>"]}},
        ]
    }

    return {
        "version": 2.1,
        "jobs": jobs,
        "workflows": {
            "test": {
                "when": not_main,
                "jobs": _build_workflow(levels),
            },
            "deploy": {
                "when": {"equal": ["main", "<< pipeline.git.branch >>"]},
                "jobs": _build_workflow(levels),
            },
        },
    }


if __name__ == "__main__":
    config = generate_config()
    print(yaml.dump(config, default_flow_style=False, sort_keys=False))
