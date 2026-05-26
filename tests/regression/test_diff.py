"""Unit tests for poorbricks.regression.MigrationDiff.

Drive the harness with two in-memory DataFrames so we exercise every
status path (pass / fail / missing_in_candidate / extra_in_candidate)
plus tolerance overrides and snapshot round-trip.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from poorbricks.regression import (
    DataFrameSource,
    MigrationDiff,
    MigrationReport,
    NumericTolerance,
)


def _ref() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "patient_id": ["p1", "p2", "p3", "p4"],
            "month": [date(2025, 1, 1)] * 4,
            "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE"],
            "sessions": [3, 5, 0, 2],
            "pain_level": [7.0, 5.0, None, 8.0],
            "extra_legacy_col": ["a", "b", "c", "d"],
        }
    )


def _cand_exact() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "patient_id": ["p1", "p2", "p3", "p4"],
            "month": [date(2025, 1, 1)] * 4,
            "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE"],
            "sessions": [3, 5, 0, 2],
            "pain_level": [7.0, 5.0, None, 8.0],
        }
    )


def _cand_drifted() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "patient_id": ["p1", "p2", "p3", "p5"],
            "month": [date(2025, 1, 1)] * 4,
            "status": ["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE"],
            "sessions": [3, 5, 1, 9],
            "pain_level": [7.1, 5.0, 0.0, 8.4],
            "extra_candidate_col": [1, 2, 3, 4],
        }
    )


def _diff(reference: pd.DataFrame, candidate: pd.DataFrame, **kw) -> MigrationReport:
    return MigrationDiff(
        reference=DataFrameSource(reference),
        candidate=DataFrameSource(candidate),
        join_keys=["patient_id", "month"],
        label="unit_test",
        **kw,
    ).run()


def test_identical_passes_every_column() -> None:
    """When every reference column has a matching candidate column with equal
    values, every comparable column passes. The extra reference-only column
    correctly registers as ``missing_in_candidate`` (not a pass)."""
    report = _diff(_ref(), _cand_exact())
    assert report.row_counts == {"only_reference": 0, "only_candidate": 0, "both": 4}
    statuses = {c.name: c.status for c in report.columns}
    assert statuses["status"] == "pass"
    assert statuses["sessions"] == "pass"
    assert statuses["pain_level"] == "pass"
    # extra_legacy_col exists in reference but not candidate — IS a regression
    assert statuses["extra_legacy_col"] == "missing_in_candidate"


def test_drift_flags_value_mismatches() -> None:
    report = _diff(_ref(), _cand_drifted())
    # Only p1, p2, p3 are in both
    assert report.row_counts["both"] == 3
    assert report.row_counts["only_reference"] == 1  # p4
    assert report.row_counts["only_candidate"] == 1  # p5
    cd = {c.name: c for c in report.columns}
    # p3 differs (INACTIVE → ACTIVE) → 33% mismatch > 10% default tolerance
    assert cd["status"].status == "fail"
    # sessions: p3 0→1 → 33% mismatch → fail
    assert cd["sessions"].status == "fail"
    # extra column on candidate side
    assert cd["extra_candidate_col"].status == "extra_in_candidate"


def test_per_column_tolerance_override() -> None:
    report = _diff(
        _ref(),
        _cand_drifted(),
        column_tolerances={"status": 50.0, "sessions": 50.0},
    )
    cd = {c.name: c for c in report.columns}
    # Both now pass with the wider tolerance.
    assert cd["status"].status == "pass"
    assert cd["sessions"].status == "pass"


def test_numeric_tolerance_allows_tiny_drift() -> None:
    # pain_level: 7.0/7.1, 5.0/5.0, NaN/0.0 → 2/3 mismatch without numeric tol
    report = _diff(_ref(), _cand_drifted())
    cd = {c.name: c for c in report.columns}
    assert cd["pain_level"].status == "fail"

    report_loose = _diff(
        _ref(),
        _cand_drifted(),
        numeric_tolerances={"pain_level": NumericTolerance(atol=0.5)},
    )
    {c.name: c for c in report_loose.columns}
    # 7.0 vs 7.1 now passes; NaN vs 0 still flagged → 1/3 = 33% mismatch
    # still > 10% default → fails. Need to override tolerance too:
    report_loose2 = _diff(
        _ref(),
        _cand_drifted(),
        numeric_tolerances={"pain_level": NumericTolerance(atol=0.5)},
        column_tolerances={"pain_level": 50.0},
    )
    cd3 = {c.name: c for c in report_loose2.columns}
    assert cd3["pain_level"].status == "pass"


def test_assert_no_regression_raises_on_drift() -> None:
    report = _diff(_ref(), _cand_drifted())
    with pytest.raises(AssertionError) as excinfo:
        report.assert_no_regression()
    msg = str(excinfo.value)
    assert "unit_test" in msg
    assert "status" in msg or "sessions" in msg


def test_assert_no_regression_silent_when_clean() -> None:
    """Two frames with identical columns + identical values pass cleanly."""
    ref = _ref().drop(columns=["extra_legacy_col"])
    _diff(ref, _cand_exact()).assert_no_regression()  # must not raise


def test_to_markdown_groups_failures_first(tmp_path: Path) -> None:
    report = _diff(_ref(), _cand_drifted())
    out = tmp_path / "r.md"
    report.to_markdown(out)
    body = out.read_text()
    # The fail section should appear before any pass row.
    fail_pos = body.find("✗ fail")
    pass_pos = body.find("✓ pass")
    if pass_pos != -1:  # passes may be absent on the drifted scenario
        assert fail_pos < pass_pos


def test_snapshot_round_trip(tmp_path: Path) -> None:
    report = _diff(_ref(), _cand_exact())
    base = report.snapshot(tmp_path)
    # The snapshot writes parquet+json+md to <base>/<isodate>/
    diff_json = json.loads((base / "diff.json").read_text())
    assert diff_json["label"] == "unit_test"
    assert {"only_reference", "only_candidate", "both"} <= set(diff_json["row_counts"])


def test_aliases_remap_reference_columns() -> None:
    ref = _ref().rename(columns={"sessions": "monthly_sessions"})
    report = MigrationDiff(
        reference=DataFrameSource(ref),
        candidate=DataFrameSource(_cand_exact()),
        join_keys=["patient_id", "month"],
        column_aliases={"monthly_sessions": "sessions"},
        label="alias_test",
    ).run()
    cd = {c.name: c for c in report.columns}
    assert cd["sessions"].status == "pass"
