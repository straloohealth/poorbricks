"""Tests for cross-table contract verification (lineage-driven)."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from poorbricks import lineage_runtime
from poorbricks.pytest_plugin import run_captured_lineage_checks
from poorbricks.verify import _check_consumed_columns
from validation import ValidatedStruct


def _fetcher(contracts: dict[str, dict[str, Any]]) -> Callable[[str], dict[str, Any]]:
    def fetch(table: str) -> dict[str, Any]:
        if table not in contracts:
            raise KeyError(table)
        return contracts[table]

    return fetch


def _schema(*names_types: tuple[str, Any]) -> dict[str, Any]:
    return {
        "schema_json": StructType(
            [StructField(n, t()) for n, t in names_types]
        ).jsonValue()
    }


def test_lineage_break_when_consumed_column_removed() -> None:
    contracts = {
        "smith.users": _schema(("patient_id", LongType), ("email", StringType))
    }
    errors = _check_consumed_columns(
        "postgres:dim_patient",
        {"smith.users": ["patient_id", "name"]},  # 'name' was dropped upstream
        _fetcher(contracts),
        {},
    )
    assert len(errors) == 1
    assert errors[0].reason == "lineage_break"
    assert "name" in errors[0].details[0]


def test_no_error_when_all_consumed_columns_present() -> None:
    contracts = {
        "smith.users": _schema(
            ("patient_id", LongType), ("email", StringType), ("name", StringType)
        )
    }
    errors = _check_consumed_columns(
        "postgres:dim_patient",
        {"smith.users": ["patient_id", "name"]},
        _fetcher(contracts),
        {},
    )
    assert errors == []


def test_missing_upstream_contract() -> None:
    errors = _check_consumed_columns(
        "postgres:dim_patient",
        {"smith.users": ["patient_id"]},
        _fetcher({}),
        {},
    )
    assert len(errors) == 1
    assert errors[0].reason == "missing_contract"


def test_in_bundle_producer_resolved_locally_without_fetch() -> None:
    class _Users(ValidatedStruct):
        patient_id: int
        name: str | None

    # No published contract — the producer is in the same bundle, so its
    # declared model is used and no fetch happens.
    producer = SimpleNamespace(model=_Users)
    errors = _check_consumed_columns(
        "postgres:dim_patient",
        {"smith.users": ["patient_id", "name"]},
        _fetcher({}),  # would KeyError if called
        {"smith.users": producer},  # type: ignore[dict-item]
    )
    assert errors == []


def test_plugin_reads_captured_and_flags_break(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("poorbricks.registry.all_pipelines", lambda: {})
    lineage_runtime.clear_captured()
    lineage_runtime.record_capture(
        "dim_patient", {"consumed": {"smith.users": ["gone_column"]}}
    )
    contracts = {"smith.users": _schema(("patient_id", LongType))}
    errors = run_captured_lineage_checks(fetcher=_fetcher(contracts))
    lineage_runtime.clear_captured()
    assert len(errors) == 1
    assert errors[0].reason == "lineage_break"


def test_plugin_skips_when_store_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("poorbricks.registry.all_pipelines", lambda: {})
    lineage_runtime.clear_captured()
    lineage_runtime.record_capture("dim_patient", {"consumed": {"smith.users": ["x"]}})

    def _boom(table: str) -> dict[str, Any]:
        raise ConnectionError("no network")

    errors = run_captured_lineage_checks(fetcher=_boom)
    lineage_runtime.clear_captured()
    assert errors == []  # unreachable store → skipped, not failed
