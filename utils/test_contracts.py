"""Tests for contract-store helpers."""

from __future__ import annotations

from datetime import date, datetime

from utils.contracts import _bson_safe


class TestBsonSafe:
    """_bson_safe makes contract documents safe for BSON storage."""

    def test_bare_date_promoted_to_datetime(self) -> None:
        """BSON has no date type — a bare date becomes a midnight datetime."""
        assert _bson_safe(date(2025, 1, 1)) == datetime(2025, 1, 1)

    def test_datetime_left_unchanged(self) -> None:
        """A datetime is already BSON-encodable and is returned as-is."""
        dt = datetime(2025, 1, 1, 12, 30, 45)
        assert _bson_safe(dt) == dt

    def test_nested_dates_in_dict_and_list(self) -> None:
        """Dates are converted recursively inside dicts and lists."""
        doc = {
            "example_rows": [{"month": date(2025, 3, 1), "count": 5}],
            "pushed_at": datetime(2024, 1, 1, 9, 0),
        }
        out = _bson_safe(doc)
        assert out["example_rows"][0]["month"] == datetime(2025, 3, 1)
        assert out["example_rows"][0]["count"] == 5
        assert out["pushed_at"] == datetime(2024, 1, 1, 9, 0)

    def test_scalars_left_unchanged(self) -> None:
        """Non-temporal scalars pass through untouched."""
        assert _bson_safe("text") == "text"
        assert _bson_safe(42) == 42
        assert _bson_safe(True) is True
        assert _bson_safe(None) is None
