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

``auto`` (default) uses z-score when there is enough history with non-zero
variance, falling back to the percentage band otherwise. ``off`` disables it.
Both knobs are per-pipeline overridable so a high-variance table can widen the
band and a tightly-controlled table can narrow it.
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
        is_anomaly: bool, used: str, threshold: float, reason: str
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
        )

    if method == "off":
        return verdict(False, "off", 0.0, "anomaly detection disabled")
    if n < min_samples:
        return verdict(
            False, method, 0.0, f"insufficient history ({n} < {min_samples} samples)"
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
