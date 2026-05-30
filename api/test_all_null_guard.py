"""Tests for the all-NULL column guard in /v1/verification."""

from __future__ import annotations

from api.main import _all_null_columns


def test_flags_only_fully_null_columns() -> None:
    rates = {"patient_id": 1.0, "name": 0.2, "email": 0.0, "navigator_id": 1.0}
    assert _all_null_columns(rates, exclude=set()) == ["navigator_id", "patient_id"]


def test_excludes_already_reported_stubs() -> None:
    rates = {"a": 1.0, "b": 1.0, "c": 1.0}
    assert _all_null_columns(rates, exclude={"a", "c"}) == ["b"]


def test_empty_when_nothing_fully_null() -> None:
    assert _all_null_columns({"x": 0.99, "y": 0.0}, exclude=set()) == []
    assert _all_null_columns({}, exclude=set()) == []
