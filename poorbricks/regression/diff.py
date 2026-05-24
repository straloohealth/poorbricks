"""Agnostic two-source DataFrame regression diff.

Compare any two result sets row-by-row, column-by-column. Used to confirm
a migrated pipeline matches the legacy artefact it replaces; reusable for
any side-by-side comparison.

```
diff = MigrationDiff(
    reference=MongoSource(...),
    candidate=PostgresSource(...),
    join_keys=["patient_id", "month"],
    default_tolerance_pct=10.0,
    column_tolerances={"patient_id": 0.0},
    label="aon_monthly_report",
)
report = diff.run()
report.to_markdown("/tmp/regression_aon.md")
report.snapshot("/path/to/.regression/aon")
report.assert_no_regression()
```
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from .sources import Source


# ---- Tolerance --------------------------------------------------------------


@dataclass(frozen=True)
class NumericTolerance:
    """Optional numeric closeness rule for a column.

    A pair of values is considered equal when
    ``abs(L - R) <= atol + rtol * abs(R)``. Defaults to strict equality.
    """

    atol: float = 0.0
    rtol: float = 0.0

    def close(self, l: float, r: float) -> bool:
        return abs(l - r) <= self.atol + self.rtol * abs(r)


# ---- Per-column result ------------------------------------------------------


@dataclass
class ColumnDiff:
    """Outcome for one column.

    ``status``:
      - ``pass``                  — mismatch_pct ≤ tolerance, both sides had data
      - ``fail``                  — mismatch_pct > tolerance
      - ``missing_in_candidate``  — present in reference, absent in candidate
      - ``extra_in_candidate``    — present in candidate, absent in reference

    ``reference_null_pct`` and ``candidate_null_pct`` are computed over the
    JOINED rows (i.e. only rows present on both sides). ``mismatch_pct`` is
    the fraction of joined rows where reference != candidate (NaN-safe).
    """

    name: str
    status: str
    reference_null_pct: float = 0.0
    candidate_null_pct: float = 0.0
    mismatch_pct: float = 0.0
    tolerance: float = 0.0
    top_mismatches: list[tuple[Any, Any, int]] = field(default_factory=list)
    note: str = ""

    @property
    def passing(self) -> bool:
        return self.status == "pass"

    def to_row(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "reference_null_pct": round(self.reference_null_pct, 2),
            "candidate_null_pct": round(self.candidate_null_pct, 2),
            "mismatch_pct": round(self.mismatch_pct, 2),
            "tolerance": round(self.tolerance, 2),
            "top_mismatches": self.top_mismatches[:5],
            "note": self.note,
        }


# ---- Aggregate report -------------------------------------------------------


@dataclass
class MigrationReport:
    """Full diff result + serialisers."""

    label: str
    join_keys: list[str]
    row_counts: dict[str, int]
    columns: list[ColumnDiff]
    reference_df: Any  # pd.DataFrame
    candidate_df: Any  # pd.DataFrame

    # ---- predicates -----

    def regressions(self) -> list[ColumnDiff]:
        return [c for c in self.columns if not c.passing]

    def assert_no_regression(self) -> None:
        bad = self.regressions()
        if not bad:
            return
        lines = [f"[{self.label}] {len(bad)} column(s) failed regression:"]
        for c in bad:
            lines.append(
                f"  {c.name}: status={c.status} "
                f"mismatch={c.mismatch_pct:.2f}% "
                f"(tolerance={c.tolerance:.2f}%)"
            )
        raise AssertionError("\n".join(lines))

    # ---- serialisers -----

    def to_markdown(self, path: str | Path | None = None) -> str:
        cnt = self.row_counts
        lines = [
            f"# Migration diff — {self.label}",
            "",
            f"- join keys: `{', '.join(self.join_keys)}`",
            f"- rows in reference only: **{cnt.get('only_reference', 0):,}**",
            f"- rows in candidate only: **{cnt.get('only_candidate', 0):,}**",
            f"- rows in both: **{cnt.get('both', 0):,}**",
            "",
            "| status | column | mismatch | tol | ref_null% | cand_null% | note |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
        order = {"fail": 0, "missing_in_candidate": 1, "extra_in_candidate": 2, "pass": 3}
        for c in sorted(self.columns, key=lambda r: (order.get(r.status, 9), -r.mismatch_pct)):
            badge = {
                "pass": "✓",
                "fail": "✗",
                "missing_in_candidate": "—",
                "extra_in_candidate": "+",
            }.get(c.status, "?")
            lines.append(
                f"| {badge} {c.status} | `{c.name}` | "
                f"{c.mismatch_pct:.2f}% | {c.tolerance:.2f}% | "
                f"{c.reference_null_pct:.1f} | {c.candidate_null_pct:.1f} | {c.note} |"
            )
        text = "\n".join(lines) + "\n"
        if path is not None:
            Path(path).write_text(text)
        return text

    def to_json(self, path: str | Path | None = None) -> str:
        payload = {
            "label": self.label,
            "join_keys": self.join_keys,
            "row_counts": self.row_counts,
            "columns": [c.to_row() for c in self.columns],
        }
        text = json.dumps(payload, default=str, indent=2)
        if path is not None:
            Path(path).write_text(text)
        return text

    def snapshot(self, directory: str | Path) -> Path:
        """Persist reference+candidate parquet, diff json, and a markdown report
        under ``<directory>/<isodate>/`` so future runs can re-compare.
        """
        base = Path(directory) / datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        base.mkdir(parents=True, exist_ok=True)
        try:
            self.reference_df.to_parquet(base / "reference.parquet")
            self.candidate_df.to_parquet(base / "candidate.parquet")
        except Exception:
            # parquet may not be available; fall back to CSV
            self.reference_df.to_csv(base / "reference.csv", index=False)
            self.candidate_df.to_csv(base / "candidate.csv", index=False)
        self.to_json(base / "diff.json")
        self.to_markdown(base / "report.md")
        return base


# ---- Diff orchestrator ------------------------------------------------------


@dataclass
class MigrationDiff:
    """Compare two result-set sources.

    The framework is symmetric in shape but asymmetric in semantic: by
    convention ``reference`` is the side you want to converge TO (e.g. a
    legacy artefact); ``candidate`` is the side you are CONVERGING (e.g.
    the new pipeline's output).
    """

    reference: Source
    candidate: Source
    join_keys: list[str]
    default_tolerance_pct: float = 10.0
    column_tolerances: dict[str, float] = field(default_factory=dict)
    numeric_tolerances: dict[str, NumericTolerance] = field(default_factory=dict)
    column_aliases: dict[str, str] = field(default_factory=dict)
    # Columns to drop from the comparison entirely — useful for fields the
    # candidate intentionally redacts (e.g. anonymized PII) where regression
    # would always fail by design. They appear in neither pass, fail,
    # missing, nor extra counts.
    ignore_columns: list[str] = field(default_factory=list)
    label: str = "migration"

    def run(self) -> MigrationReport:
        import pandas as pd

        ref = self.reference.load()
        can = self.candidate.load()

        # Apply alias map: rename reference columns to their candidate-side names
        # (callers can also remap the other way by reversing the dict beforehand).
        if self.column_aliases:
            ref = ref.rename(columns=self.column_aliases)

        for k in self.join_keys:
            if k not in ref.columns:
                raise ValueError(f"join key {k!r} missing from reference frame")
            if k not in can.columns:
                raise ValueError(f"join key {k!r} missing from candidate frame")

        # Normalise join keys: coerce date/timestamp-like keys to plain `date`
        # so a tz-aware datetime joins with a naive one cleanly.
        for k in self.join_keys:
            ref[k] = _normalise_key(ref[k])
            can[k] = _normalise_key(can[k])

        ref_keys = ref[self.join_keys].drop_duplicates()
        can_keys = can[self.join_keys].drop_duplicates()
        only_ref = ref_keys.merge(can_keys, how="left", indicator=True)
        only_ref = only_ref[only_ref["_merge"] == "left_only"]
        only_can = can_keys.merge(ref_keys, how="left", indicator=True)
        only_can = only_can[only_can["_merge"] == "left_only"]
        in_both = ref_keys.merge(can_keys, how="inner")

        joined = ref.merge(can, on=self.join_keys, how="inner", suffixes=("__ref", "__can"))

        ignored = set(self.ignore_columns)
        ref_cols = set(ref.columns) - set(self.join_keys) - ignored
        can_cols = set(can.columns) - set(self.join_keys) - ignored

        cols: list[ColumnDiff] = []
        for c in sorted(ref_cols):
            tol = self.column_tolerances.get(c, self.default_tolerance_pct)
            if c not in can_cols:
                cols.append(
                    ColumnDiff(
                        name=c,
                        status="missing_in_candidate",
                        tolerance=tol,
                        note="present in reference, absent in candidate",
                    )
                )
                continue
            ref_col = f"{c}__ref" if f"{c}__ref" in joined.columns else c
            can_col = f"{c}__can" if f"{c}__can" in joined.columns else c
            cols.append(
                _compare_column(
                    c,
                    joined[ref_col],
                    joined[can_col],
                    tolerance=tol,
                    numeric_tol=self.numeric_tolerances.get(c),
                )
            )

        for c in sorted(can_cols - ref_cols):
            cols.append(
                ColumnDiff(
                    name=c,
                    status="extra_in_candidate",
                    tolerance=self.column_tolerances.get(c, self.default_tolerance_pct),
                    note="present in candidate, absent in reference",
                )
            )

        return MigrationReport(
            label=self.label,
            join_keys=list(self.join_keys),
            row_counts={
                "only_reference": int(len(only_ref)),
                "only_candidate": int(len(only_can)),
                "both": int(len(in_both)),
            },
            columns=cols,
            reference_df=ref,
            candidate_df=can,
        )


# ---- Per-column comparison primitives --------------------------------------


def _compare_column(name: str, l, r, *, tolerance: float, numeric_tol: NumericTolerance | None):
    """Score one column. Returns a ColumnDiff."""
    import pandas as pd

    n = len(l)
    if n == 0:
        return ColumnDiff(
            name=name, status="pass", tolerance=tolerance, note="no rows in both"
        )

    l_null_pct = l.isna().mean() * 100
    r_null_pct = r.isna().mean() * 100

    both_null = l.isna() & r.isna()
    only_one_null = l.isna() ^ r.isna()

    # Coerce numeric strings to numbers for the equality check (legacy mongo
    # often stores ints as strings).
    l_norm, r_norm = _coerce_pair(l, r)

    if numeric_tol is not None and _is_numeric_like(l_norm) and _is_numeric_like(r_norm):
        delta = (l_norm.astype(float) - r_norm.astype(float)).abs()
        thresh = numeric_tol.atol + numeric_tol.rtol * r_norm.astype(float).abs()
        equal = (delta <= thresh) | both_null
    else:
        equal = (l_norm == r_norm) | both_null

    mismatch_pct = float((~equal & ~only_one_null).mean() * 100 + only_one_null.mean() * 100)

    # Capture top 5 disagreeing value pairs for debugging.
    if mismatch_pct > 0:
        disagreement = pd.DataFrame({"l": l, "r": r})[~equal]
        try:
            top = (
                disagreement.fillna("<null>")
                .astype(str)
                .groupby(["l", "r"])
                .size()
                .sort_values(ascending=False)
                .head(5)
            )
            top_mismatches = [(idx[0], idx[1], int(cnt)) for idx, cnt in top.items()]
        except Exception:
            top_mismatches = []
    else:
        top_mismatches = []

    status = "pass" if mismatch_pct <= tolerance else "fail"
    return ColumnDiff(
        name=name,
        status=status,
        reference_null_pct=float(l_null_pct),
        candidate_null_pct=float(r_null_pct),
        mismatch_pct=mismatch_pct,
        tolerance=tolerance,
        top_mismatches=top_mismatches,
    )


def _coerce_pair(l, r):
    """Best-effort numeric coercion when one side is string-y digits."""
    import pandas as pd

    def to_num(s):
        try:
            converted = pd.to_numeric(s, errors="coerce")
            if converted.notna().sum() >= s.notna().sum() * 0.9:
                return converted
        except Exception:
            pass
        return s

    return to_num(l), to_num(r)


def _is_numeric_like(s) -> bool:
    import pandas as pd

    return pd.api.types.is_numeric_dtype(s)


def _normalise_key(series):
    """Coerce a join-key column to a comparable form (date for datetimes)."""
    import pandas as pd

    if pd.api.types.is_datetime64_any_dtype(series):
        if getattr(series.dt, "tz", None) is not None:
            series = series.dt.tz_convert(None)
        return series.dt.date
    if series.dtype == object and len(series) and isinstance(series.iloc[0], (str,)):
        # Try parsing dates encoded as strings.
        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().mean() > 0.95:
            return parsed.dt.date
    return series
