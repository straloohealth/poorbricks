"""Unit tests for row-count anomaly detection."""

from __future__ import annotations

from poorbricks.anomaly import check_row_count


def test_insufficient_history_is_not_anomaly() -> None:
    v = check_row_count("k", 1000, [100, 100], min_samples=5)
    assert v.is_anomaly is False
    assert "insufficient history" in v.reason


def test_zscore_flags_spike() -> None:
    history = [100, 102, 98, 101, 99, 100]
    v = check_row_count("k", 1000, history, method="zscore", z=3.0)
    assert v.is_anomaly is True
    assert v.method == "zscore"
    assert "spike" in v.reason


def test_zscore_within_band_is_ok() -> None:
    history = [100, 102, 98, 101, 99, 100]
    v = check_row_count("k", 101, history, method="zscore", z=3.0)
    assert v.is_anomaly is False


def test_pct_fallback_when_zero_variance() -> None:
    # All identical → stddev 0 → auto falls back to the percentage band.
    history = [100, 100, 100, 100, 100]
    v = check_row_count("k", 200, history, method="auto", pct=0.5)
    assert v.method == "pct"
    assert v.is_anomaly is True
    # A small change stays within the band.
    assert (
        check_row_count("k", 110, history, method="auto", pct=0.5).is_anomaly is False
    )


def test_off_disables_detection() -> None:
    v = check_row_count("k", 999999, [1, 1, 1, 1, 1], method="off")
    assert v.is_anomaly is False
    assert v.method == "off"


def test_zero_baseline_flags_nonzero() -> None:
    # All-zero history has no scale; a jump to a large count must still alert.
    v = check_row_count("k", 1_000_000, [0, 0, 0, 0, 0])
    assert v.is_anomaly is True


def test_zero_baseline_ok_when_still_zero() -> None:
    v = check_row_count("k", 0, [0, 0, 0, 0, 0])
    assert v.is_anomaly is False
