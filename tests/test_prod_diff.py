"""Unit tests for the pure dev-vs-prod diff (poorbricks/prod_diff.py)."""

from __future__ import annotations

from typing import Any

from poorbricks.prod_diff import compute_prod_diff


def _snapshot(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "row_count": 10_000,
        "null_rates": {"name": 0.1, "id": 0.0},
        "enum_samples": {},
        "fields": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
        ],
        "expectations": {},
        "schema_hash": "abc",
        "anomaly": None,
    }
    base.update(over)
    return base


PROD_PROFILE = {"row_count": 10_000, "null_rates": {"name": 0.1, "id": 0.0}}
PROD_FIELDS = [
    {"name": "id", "type": "string", "nullable": False},
    {"name": "name", "type": "string", "nullable": True},
]


def test_identical_snapshot_is_in_sync() -> None:
    diff = compute_prod_diff(_snapshot(), PROD_PROFILE, PROD_FIELDS, {})
    assert diff["severity"] == "none"
    assert diff["row_count"]["delta_pct"] == 0.0
    assert diff["row_count"]["major"] is False
    assert diff["null_dist"] == []
    assert diff["fields"] == {"added": [], "removed": [], "retyped": []}


def test_large_row_count_drop_is_major() -> None:
    diff = compute_prod_diff(_snapshot(row_count=7_000), PROD_PROFILE, PROD_FIELDS, {})
    assert abs(diff["row_count"]["delta_pct"] + 0.3) < 1e-9  # -30%
    assert diff["row_count"]["major"] is True
    assert diff["severity"] == "major"


def test_small_row_count_change_is_not_major() -> None:
    diff = compute_prod_diff(_snapshot(row_count=10_500), PROD_PROFILE, PROD_FIELDS, {})
    assert diff["row_count"]["major"] is False
    # only a row-count wiggle, nothing else changed → none
    assert diff["severity"] == "none"


def test_zero_prod_baseline_any_nonzero_is_major() -> None:
    diff = compute_prod_diff(
        _snapshot(row_count=5), {"row_count": 0, "null_rates": {}}, PROD_FIELDS, {}
    )
    assert diff["row_count"]["delta_pct"] is None
    assert diff["row_count"]["major"] is True


def test_null_rate_jump_flagged_major() -> None:
    diff = compute_prod_diff(
        _snapshot(null_rates={"name": 0.35, "id": 0.0}), PROD_PROFILE, PROD_FIELDS, {}
    )
    nd = {n["column"]: n for n in diff["null_dist"]}
    assert "name" in nd
    assert abs(nd["name"]["delta"] - 0.25) < 1e-9
    assert nd["name"]["major"] is True
    assert diff["severity"] == "major"


def test_added_and_removed_fields() -> None:
    snap = _snapshot(
        fields=[
            {"name": "id", "type": "string"},
            {"name": "email", "type": "string"},  # added
        ]
    )
    diff = compute_prod_diff(snap, PROD_PROFILE, PROD_FIELDS, {})
    assert diff["fields"]["added"] == ["email"]
    assert diff["fields"]["removed"] == ["name"]  # removal → major
    assert diff["severity"] == "major"


def test_retyped_field_is_minor() -> None:
    snap = _snapshot(
        fields=[
            {"name": "id", "type": "long"},  # was string
            {"name": "name", "type": "string"},
        ]
    )
    diff = compute_prod_diff(snap, PROD_PROFILE, PROD_FIELDS, {})
    assert diff["fields"]["retyped"] == [
        {"column": "id", "from": "string", "to": "long"}
    ]
    assert diff["severity"] == "minor"


def test_dev_anomaly_adds_alert_and_is_major() -> None:
    snap = _snapshot(anomaly={"is_anomaly": True, "reason": "z=4.1"})
    diff = compute_prod_diff(snap, PROD_PROFILE, PROD_FIELDS, {})
    assert "row_count_anomaly" in diff["alerts"]["added"]
    assert diff["severity"] == "major"


def test_removed_field_drops_its_configured_check() -> None:
    snap = _snapshot(fields=[{"name": "id", "type": "string"}])  # 'name' gone
    exp = {"non_null_columns": ["name"], "min_rows": 100}
    diff = compute_prod_diff(snap, PROD_PROFILE, PROD_FIELDS, exp)
    assert "non_null:name" in diff["alerts"]["removed"]
    assert "min_rows" in diff["alerts"]["existing"]


def test_per_pipeline_pct_override_widens_band() -> None:
    # A pipeline tuned to a 50% band should not flag a 30% drop as major.
    diff = compute_prod_diff(
        _snapshot(row_count=7_000),
        PROD_PROFILE,
        PROD_FIELDS,
        {"row_count_anomaly_pct": 0.5},
    )
    assert diff["row_count"]["major"] is False
