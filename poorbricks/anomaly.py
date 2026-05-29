"""Row-count anomaly detection against recent run history.

The comparison is deliberately NOT a static threshold. After a pipeline writes,
its row count is compared to that pipeline's OWN recent successful runs (from
``poorbricks_meta.run_history``), so "how much change is too much" is learned
per-pipeline rather than hardcoded. A stable ~10k-row table dropping to 8k is
many standard deviations from its mean and alerts; a naturally bursty table
with the same nominal drop stays within its historical band and does not.

Two methods, configurable per pipeline via ``Expectations`` attributes:

* ``zscore`` — relative to the pipeline's own variance: flag when
  ``|current - mean| > k * stddev`` (default k=3). This is the adaptive default.
* ``pct``    — relative to the pipeline's own mean: flag when
  ``|current - mean| / mean > pct`` (default 0.25 = 25%). Used as the fallback
  when the history has no variance yet (e.g. an always-identical count), where a
  z-score is undefined.

``auto`` (default) is **trend-aware**: when a linear fit explains the history
materially better than its flat mean (a steadily growing/shrinking table), the
current count is judged against the trend-*predicted* next value within a
relative band — so an append-only table that gains a few rows each run does not
trip a flat-mean z-score on its own normal growth, while a real break from the
trend (a drop, a stall, a spike) still alerts. Absent a clear trend it uses
z-score when there is non-zero variance, falling back to the percentage band.
``off`` disables it. Both knobs are per-pipeline overridable, and an explicit
``zscore``/``pct`` method bypasses the trend path entirely.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

DEFAULT_METHOD = "auto"
DEFAULT_Z = 3.0
# Relative band for the no-variance fallback — a 25% swing from the historical
# mean is flagged. Per-pipeline tunable via Expectations.ROW_COUNT_ANOMALY_PCT.
DEFAULT_PCT = 0.25
DEFAULT_MIN_SAMPLES = 5
# A series is treated as "trending" (judged against a fitted line rather than a
# flat mean) when a linear fit's residual spread is at most this fraction of the
# flat standard deviation — i.e. the line explains the data at least ~2× better
# than the mean. Keeps a steadily-growing append-only table from tripping a
# z-score on its own normal growth.
TREND_FIT_RATIO = 0.5


def _linfit(ys: list[float]) -> tuple[float, float, float, float]:
    """Least-squares line over (index, value); return slope, intercept,
    residual stddev, and the value predicted for the NEXT index."""
    n = len(ys)
    xs = list(range(n))
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    slope = sxy / sxx if sxx else 0.0
    intercept = my - slope * mx
    resid = [ys[i] - (slope * xs[i] + intercept) for i in range(n)]
    resid_std = math.sqrt(sum(r * r for r in resid) / n)
    predicted = slope * n + intercept  # value expected at the next (current) run
    return slope, intercept, resid_std, predicted


@dataclass
class RowCountAnomaly:
    pipeline_key: str
    current: int
    baseline_mean: float
    baseline_stddev: float
    n_samples: int
    method: str
    threshold: float
    is_anomaly: bool
    reason: str
    predicted: float | None = None  # trend-predicted count (method="trend" only)

    def to_dict(self) -> dict[str, object]:
        return {
            "current": self.current,
            "baseline_mean": round(self.baseline_mean, 2),
            "baseline_stddev": round(self.baseline_stddev, 2),
            "n_samples": self.n_samples,
            "method": self.method,
            "threshold": self.threshold,
            "is_anomaly": self.is_anomaly,
            "reason": self.reason,
            "predicted": round(self.predicted, 2)
            if self.predicted is not None
            else None,
        }


def check_row_count(
    pipeline_key: str,
    current: int,
    history: list[int],
    *,
    method: str | None = None,
    z: float | None = None,
    pct: float | None = None,
    min_samples: int | None = None,
) -> RowCountAnomaly:
    """Compare ``current`` row count to recent successful counts in ``history``."""
    method = (method or DEFAULT_METHOD).lower()
    z = z if z is not None else DEFAULT_Z
    pct = pct if pct is not None else DEFAULT_PCT
    min_samples = min_samples if min_samples is not None else DEFAULT_MIN_SAMPLES

    n = len(history)
    mean = sum(history) / n if n else 0.0
    stddev = math.sqrt(sum((x - mean) ** 2 for x in history) / n) if n else 0.0

    def verdict(
        is_anomaly: bool,
        used: str,
        threshold: float,
        reason: str,
        predicted: float | None = None,
    ) -> RowCountAnomaly:
        return RowCountAnomaly(
            pipeline_key=pipeline_key,
            current=current,
            baseline_mean=mean,
            baseline_stddev=stddev,
            n_samples=n,
            method=used,
            threshold=threshold,
            is_anomaly=is_anomaly,
            reason=reason,
            predicted=predicted,
        )

    if method == "off":
        return verdict(False, "off", 0.0, "anomaly detection disabled")
    if n < min_samples:
        return verdict(
            False, method, 0.0, f"insufficient history ({n} < {min_samples} samples)"
        )

    # Trend-aware path (default ``auto`` only): a steadily growing/shrinking
    # table has a tiny variance *around its trend line* but a large variance
    # around its flat mean, so a plain z-score flags its own normal growth. When
    # a line fits the history materially better than the mean, judge the
    # deviation from the trend-PREDICTED next value, gated by the relative band
    # so a few extra rows on a large stable table is never an "anomaly".
    if method == "auto" and stddev > 0:
        chron = list(reversed(history))  # recent_successful is newest-first
        slope, _intercept, resid_std, predicted = _linfit([float(x) for x in chron])
        if resid_std <= TREND_FIT_RATIO * stddev and slope != 0.0:
            deviation = abs(current - predicted)
            rel = deviation / max(abs(predicted), 1.0)
            is_anom = rel > pct
            direction = "above" if current > predicted else "below"
            return verdict(
                is_anom,
                "trend",
                pct,
                (
                    f"row count {current} is {rel:.0%} {direction} the trend-predicted "
                    f"{predicted:.0f} (> {pct:.0%}; slope {slope:+.0f}/run over {n} runs)"
                    if is_anom
                    else (
                        f"within {pct:.0%} of trend-predicted {predicted:.0f} "
                        f"(slope {slope:+.0f}/run)"
                    )
                ),
                predicted=predicted,
            )

    use_zscore = method == "zscore" or (method == "auto" and stddev > 0)
    if use_zscore and stddev > 0:
        score = abs(current - mean) / stddev
        is_anom = score > z
        direction = "spike" if current > mean else "drop"
        return verdict(
            is_anom,
            "zscore",
            z,
            (
                f"row count {direction}: {current} vs mean {mean:.0f}±{stddev:.0f} "
                f"(z={score:.2f} > {z})"
                if is_anom
                else f"within {z}σ of mean {mean:.0f}"
            ),
        )

    # Percentage band (also the fallback when variance is degenerate).
    if mean == 0:
        # A history of all-zeros has no relative scale: any non-zero current is
        # a regime change worth flagging (e.g. a table that produced 0 rows
        # suddenly producing 1M, or vice-versa).
        is_anom = current != 0
        return verdict(
            is_anom,
            "pct",
            pct,
            f"row count went from a 0-row baseline to {current}"
            if is_anom
            else "still 0 rows (matches baseline)",
        )
    rel = abs(current - mean) / mean
    is_anom = rel > pct
    direction = "spike" if current > mean else "drop"
    return verdict(
        is_anom,
        "pct",
        pct,
        (
            f"row count {direction}: {current} vs mean {mean:.0f} "
            f"({rel:.0%} change > {pct:.0%})"
            if is_anom
            else f"within {pct:.0%} of mean {mean:.0f}"
        ),
    )


__all__ = [
    "DEFAULT_METHOD",
    "DEFAULT_MIN_SAMPLES",
    "DEFAULT_PCT",
    "DEFAULT_Z",
    "RowCountAnomaly",
    "check_row_count",
]
