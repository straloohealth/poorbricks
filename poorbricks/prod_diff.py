"""Diff a dev run's profile snapshot against the current production baseline.

A dev run records a ``profile_snapshot`` (row_count, null_rates, fields,
expectations, anomaly) into ``poorbricks_meta.run_history``; the prod baseline
is the published Mongo contract (``profile``/``fields``/``expectations``). This
module compares the two and rolls the differences into a severity so the UI can
flag "how far has this dev run drifted from prod".

Thresholds are **relative** (a percentage swing), never static counts — a 25%
row-count change matters regardless of table size (see ``poorbricks/anomaly.py``
``DEFAULT_PCT``). Pure functions only (no DB/Mongo), so it is trivially testable.
"""

from __future__ import annotations

from typing import Any

# Relative row-count change beyond which the diff is "major" — mirrors the
# row-count anomaly default so the two checks agree.
ROW_COUNT_MAJOR_PCT = 0.25
# Absolute jump in a column's null *rate* (0-1) that counts as major — mirrors
# ``drift.check_drift``'s ``null_rate_threshold``.
NULL_RATE_MAJOR_DELTA = 0.10


def _row_count_diff(
    dev_rc: int | None, prod_rc: int | None, pct_threshold: float
) -> dict[str, Any]:
    delta_pct: float | None = None
    major = False
    if prod_rc is None or dev_rc is None:
        delta_pct = None
    elif prod_rc == 0:
        # No baseline volume: any non-zero dev count is a major change (same
        # rule anomaly.check_row_count uses for a zero baseline).
        delta_pct = None
        major = dev_rc != 0
    else:
        delta_pct = (dev_rc - prod_rc) / prod_rc
        major = abs(delta_pct) > pct_threshold
    return {"dev": dev_rc, "prod": prod_rc, "delta_pct": delta_pct, "major": major}


def _null_dist_diff(
    dev_rates: dict[str, float], prod_rates: dict[str, float]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for col in sorted(set(dev_rates) | set(prod_rates)):
        dev = float(dev_rates.get(col, 0.0))
        prod = float(prod_rates.get(col, 0.0))
        delta = dev - prod
        if abs(delta) < 1e-9:
            continue  # only surface columns whose null rate actually changed
        out.append(
            {
                "column": col,
                "dev": dev,
                "prod": prod,
                "delta": delta,
                "major": abs(delta) > NULL_RATE_MAJOR_DELTA,
            }
        )
    return out


def _field_map(fields: list[dict[str, Any]]) -> dict[str, str]:
    """``{name: type}`` from a flattened field list (tolerant of missing keys)."""
    out: dict[str, str] = {}
    for f in fields or []:
        name = f.get("name")
        if name:
            out[str(name)] = str(f.get("type", ""))
    return out


def _fields_diff(
    dev_fields: list[dict[str, Any]], prod_fields: list[dict[str, Any]]
) -> dict[str, Any]:
    dev = _field_map(dev_fields)
    prod = _field_map(prod_fields)
    added = sorted(set(dev) - set(prod))
    removed = sorted(set(prod) - set(dev))
    retyped = [
        {"column": c, "from": prod[c], "to": dev[c]}
        for c in sorted(set(dev) & set(prod))
        if dev[c] and prod[c] and dev[c] != prod[c]
    ]
    return {"added": added, "removed": removed, "retyped": retyped}


def _alerts_diff(
    dev_snapshot: dict[str, Any],
    prod_expectations: dict[str, Any],
    dev_rates: dict[str, float],
    fields: dict[str, Any],
) -> dict[str, list[str]]:
    """Which configured checks newly trip / no longer apply / still hold.

    Derived from the dev run's own verdicts + the prod-configured expectations.
    Conservative and string-labelled (the UI just lists them).
    """
    existing: list[str] = []
    added: list[str] = []
    removed: list[str] = []

    exp = prod_expectations or {}
    null_rate_max = exp.get("null_rate_max")
    enum_values = exp.get("enum_values") or {}
    non_null = exp.get("non_null_columns") or []

    # Columns prod configures a check on, scoped by whether the dev run still
    # has that field (removed fields lose coverage → "removed").
    dev_field_names = set(_field_map(fields["dev"]) if isinstance(fields, dict) else {})
    for col in list(non_null) + list(enum_values.keys()):
        label = f"non_null:{col}" if col in non_null else f"enum:{col}"
        if col in dev_field_names:
            existing.append(label)
        else:
            removed.append(label)
    if exp.get("min_rows"):
        existing.append("min_rows")

    # The dev run's row-count anomaly verdict newly trips vs the prod baseline.
    anomaly = dev_snapshot.get("anomaly") or {}
    if anomaly.get("is_anomaly"):
        added.append("row_count_anomaly")

    # A column whose dev null rate breaches prod's configured ceiling.
    if isinstance(null_rate_max, (int, float)):
        for col, rate in dev_rates.items():
            if rate > null_rate_max:
                added.append(f"null_rate_exceeded:{col}")

    return {
        "added": sorted(set(added)),
        "removed": sorted(set(removed)),
        "existing": sorted(set(existing)),
    }


def _severity(
    row_count: dict[str, Any],
    null_dist: list[dict[str, Any]],
    fields: dict[str, Any],
    alerts: dict[str, list[str]],
) -> str:
    major = (
        bool(row_count.get("major"))
        or any(n["major"] for n in null_dist)
        or bool(fields["removed"])
        or bool(alerts["added"])
    )
    if major:
        return "major"
    minor = (
        bool(fields["added"])
        or bool(fields["retyped"])
        or bool(null_dist)
        or bool(alerts["removed"])
    )
    return "minor" if minor else "none"


def compute_prod_diff(
    dev_snapshot: dict[str, Any],
    prod_profile: dict[str, Any],
    prod_fields: list[dict[str, Any]],
    prod_expectations: dict[str, Any],
) -> dict[str, Any]:
    """Compare a dev run's profile snapshot to the prod contract baseline.

    Returns a structured, JSON-serializable diff with a ``severity`` rollup.
    """
    pct_threshold = ROW_COUNT_MAJOR_PCT
    cfg_pct = (prod_expectations or {}).get("row_count_anomaly_pct")
    if isinstance(cfg_pct, (int, float)) and cfg_pct > 0:
        pct_threshold = float(cfg_pct)

    dev_rates: dict[str, float] = dict(dev_snapshot.get("null_rates") or {})
    prod_rates: dict[str, float] = dict((prod_profile or {}).get("null_rates") or {})

    row_count = _row_count_diff(
        dev_snapshot.get("row_count"),
        (prod_profile or {}).get("row_count"),
        pct_threshold,
    )
    null_dist = _null_dist_diff(dev_rates, prod_rates)
    fields = _fields_diff(dev_snapshot.get("fields") or [], prod_fields or [])
    alerts = _alerts_diff(
        dev_snapshot,
        prod_expectations or {},
        dev_rates,
        {"dev": dev_snapshot.get("fields") or []},
    )
    severity = _severity(row_count, null_dist, fields, alerts)

    return {
        "row_count": row_count,
        "null_dist": null_dist,
        "fields": fields,
        "alerts": alerts,
        "severity": severity,
    }


__all__ = [
    "ROW_COUNT_MAJOR_PCT",
    "NULL_RATE_MAJOR_DELTA",
    "compute_prod_diff",
]
