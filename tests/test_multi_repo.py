"""Integration tests for the two-repo model.

Each scenario in ``test_table_repo/tables/`` is verified against an
expected outcome (pass or specific failure reason). The contracts store
is mocked — tests do not require a live MongoDB.

These tests run serially under one xdist worker (registry is global state).
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from poorbricks.arch import check_architecture

REPO_ROOT = Path(__file__).resolve().parent.parent
TABLES_ROOT = REPO_ROOT / "test_table_repo" / "tables"

SCENARIO_NAMES = (
    "happy_path",
    "missing_contract",
    "schema_drift",
    "expectations_failure",
    "gold_patients",
)

pytestmark = pytest.mark.xdist_group("multi_repo")


def _evict_tables_modules() -> None:
    """Drop the ``tables`` package (and every submodule) from the import cache.

    ``tables`` is a *regular* package in both this repo and ``test_table_repo``,
    so Python caches it as a single module pointing at whichever root imported
    it first. Under xdist a sibling main-repo test can cache ``tables`` →
    framework repo; without evicting it, this repo's discovery would resolve
    ``tables.<scenario>`` against the wrong directory. Evicting on both entry
    and exit keeps each test hermetic regardless of worker scheduling.
    """
    for name in list(sys.modules):
        if name == "tables" or name.startswith("tables."):
            del sys.modules[name]


@pytest.fixture(autouse=True)
def _clear_registry_and_modules() -> Iterator[None]:
    """Give each test in this module a clean slate to discover ``test_table_repo``.

    The registry must be empty and the ``tables`` import cache evicted so
    ``discover_all_pipelines(test_table_repo)`` resolves to this repo's
    fixtures (not a sibling main-repo ``tables`` cached on the same xdist
    worker). The global ``conftest`` safety net rolls these mutations back
    after the test, so this fixture only needs to set up the clean slate.
    """
    from poorbricks import registry as _registry

    _registry._pipelines.clear()
    _registry._scenarios.clear()
    _evict_tables_modules()
    yield


def _smith_users_contract() -> dict[str, Any]:
    struct = StructType(
        [
            StructField("mongo_id", StringType(), nullable=True),
            StructField("external_id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("origin", StringType(), nullable=True),
            StructField("active", BooleanType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("birth_date", TimestampType(), nullable=True),
            StructField("cpf", StringType(), nullable=True),
        ]
    )
    return {"schema_json": struct.jsonValue()}


def _dim_patient_contract() -> dict[str, Any]:
    struct = StructType(
        [
            StructField("patient_id", StringType(), nullable=False),
            StructField("mongo_id", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("birth_date", TimestampType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=False),
            StructField("origin_slug", StringType(), nullable=True),
            StructField("is_active", BooleanType(), nullable=False),
            StructField("is_high_risk", BooleanType(), nullable=False),
            StructField("is_surgical", BooleanType(), nullable=False),
        ]
    )
    return {"schema_json": struct.jsonValue()}


def _make_fetcher(contracts: dict[str, dict[str, Any]]) -> Any:
    def fetcher(table_name: str) -> dict[str, Any]:
        if table_name not in contracts:
            raise KeyError(f"No contract for {table_name!r}")
        return contracts[table_name]

    return fetcher


# --- verify_local ----------------------------------------------------------


def test_verify_local_happy_path_passes() -> None:
    from poorbricks.verify import verify_local

    fetcher = _make_fetcher(
        {"smith.users": _smith_users_contract(), "dim_patient": _dim_patient_contract()}
    )
    errors = verify_local(tables_root=TABLES_ROOT, contract_fetcher=fetcher)
    happy_errors = [e for e in errors if e.pipeline_key == "postgres:happy_path"]
    assert happy_errors == [], f"happy_path should have no errors, got: {happy_errors}"


def test_verify_local_missing_contract_fails() -> None:
    from poorbricks.verify import verify_local

    fetcher = _make_fetcher(
        {"smith.users": _smith_users_contract(), "dim_patient": _dim_patient_contract()}
    )
    errors = verify_local(tables_root=TABLES_ROOT, contract_fetcher=fetcher)
    matching = [e for e in errors if e.pipeline_key == "postgres:missing_contract"]
    assert matching, "missing_contract pipeline must produce an error"
    assert any(e.reason == "missing_contract" for e in matching), matching
    assert any(e.upstream == "smith.nonexistent_table" for e in matching), matching


def test_verify_local_schema_drift_fails() -> None:
    from poorbricks.verify import verify_local

    fetcher = _make_fetcher(
        {"smith.users": _smith_users_contract(), "dim_patient": _dim_patient_contract()}
    )
    errors = verify_local(tables_root=TABLES_ROOT, contract_fetcher=fetcher)
    matching = [e for e in errors if e.pipeline_key == "postgres:schema_drift"]
    assert matching, "schema_drift pipeline must produce an error"
    drift_errors = [e for e in matching if e.reason == "schema_drift"]
    assert drift_errors, f"expected schema_drift reason, got: {matching}"
    details = " ".join(d for e in drift_errors for d in e.details)
    assert "nonexistent_local_field" in details, details


# --- verify_ci -------------------------------------------------------------


def _patch_fetch_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch utils.contracts.fetch_contract so from_rows resolves schemas."""
    contracts = {
        "smith.users": _smith_users_contract(),
        "dim_patient": _dim_patient_contract(),
    }

    def fake_fetch(table_name: str) -> dict[str, Any]:
        if table_name not in contracts:
            raise KeyError(table_name)
        return contracts[table_name]

    import utils.contracts as contracts_module

    monkeypatch.setattr(contracts_module, "fetch_contract", fake_fetch)


def test_verify_ci_expectations_failure_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Duplicate user_id rows violate UNIQUE_KEYS — expectation violation.

    MIN_ROWS is intentionally bypassed in verify_ci (enforce_min_rows=False),
    so the fixture instead trips UNIQUE_KEYS to exercise the expectation
    error path.
    """
    from poorbricks.verify import verify_ci

    _patch_fetch_contract(monkeypatch)
    errors = verify_ci(tables_root=TABLES_ROOT, mode="fixtures")
    matching = [e for e in errors if e.pipeline_key == "postgres:expectations_failure"]
    assert matching, "expectations_failure must produce an error"
    assert any(
        e.category == "expectation" and "UNIQUE_KEYS" in e.message for e in matching
    ), matching


def test_verify_ci_gold_patients_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    """gold_patients fixture passes schema, rules, and MIN_ROWS=1."""
    from poorbricks.verify import verify_ci

    _patch_fetch_contract(monkeypatch)
    errors = verify_ci(tables_root=TABLES_ROOT, mode="fixtures")
    gold_errors = [e for e in errors if e.pipeline_key == "postgres:gold_patients"]
    assert gold_errors == [], f"gold_patients should pass, got: {gold_errors}"


# --- check_architecture ----------------------------------------------------


def test_arch_check_catches_malformed_pipeline() -> None:
    """check_architecture() must report missing required files for the malformed_pipeline fixture."""
    errors = check_architecture(tables_root=TABLES_ROOT)
    malformed = [e for e in errors if "malformed_pipeline" in e.pipeline_dir]
    assert malformed, (
        "Expected arch errors for malformed_pipeline but got none. "
        f"All errors: {[e.format() for e in errors]}"
    )
    messages = " ".join(e.message for e in malformed)
    assert "fixtures.py" in messages or "transform.py" in messages, (
        f"Expected missing file message, got: {messages}"
    )


def test_arch_check_passes_for_well_formed_pipelines() -> None:
    """All well-formed test_table_repo pipelines must pass the architecture check."""
    errors = check_architecture(tables_root=TABLES_ROOT)
    well_formed_errors = [
        e for e in errors if "malformed_pipeline" not in e.pipeline_dir
    ]
    assert well_formed_errors == [], (
        "Well-formed pipelines should pass arch check:\n"
        + "\n".join(e.format() for e in well_formed_errors)
    )
