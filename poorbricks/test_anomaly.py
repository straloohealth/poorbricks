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


# --- trend-aware path (the watson_events false-positive class) ---------------

# A steadily growing table, newest-first (+2 rows/run). A flat-mean z-score
# flags its own normal growth; the trend path must not.
_GROWING = [5786, 5784, 5782, 5780, 5778, 5776]


def test_trend_normal_growth_not_flagged() -> None:
    v = check_row_count("k", 5792, _GROWING)  # auto
    assert v.method == "trend"
    assert v.is_anomaly is False
    assert v.predicted is not None and 5786 <= v.predicted <= 5790


def test_trend_flags_real_drop_below_trend() -> None:
    v = check_row_count("k", 2900, _GROWING)
    assert v.method == "trend"
    assert v.is_anomaly is True
    assert "below" in v.reason


def test_trend_flags_real_spike_above_trend() -> None:
    v = check_row_count("k", 9000, _GROWING)
    assert v.method == "trend"
    assert v.is_anomaly is True
    assert "above" in v.reason


def test_declining_trend_predicts_lower_baseline() -> None:
    history = [100, 110, 120, 130, 140, 150]  # newest-first → declining -10/run
    assert check_row_count("k", 88, history).is_anomaly is False  # ~90 predicted
    big = check_row_count("k", 40, history)
    assert big.method == "trend" and big.is_anomaly is True


def test_noisy_series_stays_on_zscore() -> None:
    # No clean line → the fit doesn't beat the mean 2×, so it is NOT treated as
    # a trend and the existing z-score behavior applies.
    history = [100, 130, 95, 140, 90, 135]
    v = check_row_count("k", 120, history)  # auto, well within the swing
    assert v.method == "zscore"
    assert v.is_anomaly is False


def test_explicit_zscore_ignores_trend_even_when_growing() -> None:
    # An explicit method override must not silently switch to the trend path.
    v = check_row_count("k", 5792, _GROWING, method="zscore", z=3.0)
    assert v.method == "zscore"
