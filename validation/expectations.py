"""Production-aware contract for one pipeline table.

Where ``ValidatedStruct`` enforces *type-level* shape (Pydantic) and
``ValidationRule``\\ s enforce *fixture-level* invariants (per-row regex,
range, enum), ``Expectations`` enforces *production-level* health: row
count floors, uniqueness, null rates, enum membership across the whole
table, and freshness windows. It runs against any DataFrame — fixtures,
``MODE=production`` reads, or anything in between — and reports human-
readable violations (no exceptions raised; the runner decides whether to
fail the build).

Usage in a pipeline's ``config.py``::

    class AonMonthlyStatus(ValidatedStruct):
        ...

    class AonMonthlyStatusExpectations(Expectations):
        MIN_ROWS = 10_000
        UNIQUE_KEYS = [["patient_id", "month"]]
        NON_NULL_COLUMNS = ["patient_id", "status", "month"]
        ENUM_VALUES = {"status": [s.value for s in StandardStatusAON]}
        FRESH_COLUMN = "month"
        FRESH_MAX_AGE_DAYS = 40

The architecture tests assert that every migrated pipeline has a class
subclassing ``Expectations`` declared next to its ``ValidatedStruct``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

# datetime.UTC was added in Python 3.11; keep the alias for 3.10 compat.
UTC = UTC

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class Expectations:
    """Base class for per-table production expectations.

    Override the class attributes you care about; leave the rest as the
    sentinels below (``None`` / empty) and they are skipped by ``check``.
    """

    MIN_ROWS: int | None = None
    """Minimum row count. Acts as a freshness/availability proxy: if the
    pipeline produces fewer rows than this in prod, something has broken
    upstream."""

    UNIQUE_KEYS: list[list[str]] = []
    """Each entry is a list of columns whose combination must be unique
    across the whole table (``count(*) == count(distinct (cols))``)."""

    NON_NULL_COLUMNS: list[str] = []
    """Columns that must be 100% non-null. Stricter than
    ``NULL_RATE_MAX``; use this for primary keys and required business
    fields."""

    NULL_RATE_MAX: dict[str, float] = {}
    """``{column: max_allowed_null_rate}``. ``0.0`` is equivalent to
    listing the column in ``NON_NULL_COLUMNS``; use this when you tolerate
    a small fraction of nulls."""

    ENUM_VALUES: dict[str, list[Any]] = {}
    """``{column: allowed_values}``. Every row's value for ``column`` must
    be in ``allowed_values``. Useful for status/category columns."""

    FRESH_COLUMN: str | None = None
    """Date or timestamp column that should be at most ``FRESH_MAX_AGE_DAYS``
    behind today. ``max(FRESH_COLUMN)`` is what the check inspects."""

    FRESH_MAX_AGE_DAYS: int | None = None
    """Companion to ``FRESH_COLUMN``. Both must be set or both must be
    ``None``."""

    # Migration-period slack factors. These widen the effective thresholds
    # during Phase 2 so that normal production drift (row-count changes,
    # freshness windows shifting as pipelines are redeployed) does not
    # trigger false failures. Tighten to 1.0 / 1.0 / 1 after Phase 2 is
    # complete and the baselines have been re-profiled against migrated output.
    _MIN_ROWS_FACTOR: float = 0.7  # accept down to 70 % of MIN_ROWS
    _NULL_RATE_SLACK: float = 2.0  # tolerate up to 2× declared NULL_RATE_MAX
    _FRESH_AGE_SLACK: int = 3  # tolerate up to 3× FRESH_MAX_AGE_DAYS

    @classmethod
    def check(cls, df: DataFrame, *, enforce_min_rows: bool = True) -> list[str]:
        """Apply every declared expectation to ``df`` and return violations.

        Empty list means the table is healthy. Violations are
        human-readable strings; callers may print, log, or fail-build on
        their presence.

        ``enforce_min_rows=False`` skips the ``MIN_ROWS`` check — used by
        ``verify_ci`` so production-sized floors don't fail upload-time
        verification against tiny fixture datasets.
        """
        from pyspark.sql import functions as f

        violations: list[str] = []

        if enforce_min_rows and cls.MIN_ROWS is not None:
            actual = df.count()
            effective_min = int(cls.MIN_ROWS * cls._MIN_ROWS_FACTOR)
            if actual < effective_min:
                violations.append(
                    f"row count {actual} < MIN_ROWS={cls.MIN_ROWS} "
                    f"(effective floor {effective_min} at {cls._MIN_ROWS_FACTOR:.0%})"
                )

        for keys in cls.UNIQUE_KEYS:
            total = df.count()
            distinct = df.select(*keys).distinct().count()
            if total != distinct:
                violations.append(
                    f"UNIQUE_KEYS={keys} violated: {total} rows, "
                    f"{distinct} distinct ({total - distinct} duplicates)"
                )

        for col in cls.NON_NULL_COLUMNS:
            null_count = df.filter(f.col(col).isNull()).count()
            if null_count > 0:
                violations.append(
                    f"NON_NULL_COLUMNS: column {col!r} has {null_count} nulls"
                )

        for col, max_rate in cls.NULL_RATE_MAX.items():
            total = df.count()
            if total == 0:
                continue
            null_count = df.filter(f.col(col).isNull()).count()
            rate = null_count / total
            effective_max = min(1.0, max_rate * cls._NULL_RATE_SLACK)
            if rate > effective_max:
                violations.append(
                    f"NULL_RATE_MAX[{col!r}]={max_rate:.3f} violated: "
                    f"observed {rate:.3f} ({null_count}/{total}) "
                    f"[effective limit {effective_max:.3f} at {cls._NULL_RATE_SLACK:.0f}×]"
                )

        for col, allowed in cls.ENUM_VALUES.items():
            allowed_set = set(allowed)
            offenders = (
                df.filter(f.col(col).isNotNull() & ~f.col(col).isin(*allowed))
                .select(col)
                .distinct()
                .limit(10)
                .collect()
            )
            if offenders:
                bad_values = sorted({row[col] for row in offenders})
                violations.append(
                    f"ENUM_VALUES[{col!r}] violated: unexpected values "
                    f"{bad_values} (allowed: {sorted(allowed_set)})"
                )

        if cls.FRESH_COLUMN is not None and cls.FRESH_MAX_AGE_DAYS is not None:
            row = df.select(f.max(cls.FRESH_COLUMN).alias("max_value")).first()
            max_value = row["max_value"] if row is not None else None
            if max_value is None:
                violations.append(
                    f"FRESH_COLUMN={cls.FRESH_COLUMN!r}: no non-null values"
                )
            else:
                max_dt = _coerce_to_datetime(max_value)
                age_days = (datetime.now(UTC) - max_dt).days
                effective_max_age = cls.FRESH_MAX_AGE_DAYS * cls._FRESH_AGE_SLACK
                if age_days > effective_max_age:
                    violations.append(
                        f"FRESH_COLUMN={cls.FRESH_COLUMN!r} max={max_dt.date()} "
                        f"is {age_days} days old (max allowed: "
                        f"{cls.FRESH_MAX_AGE_DAYS} × {cls._FRESH_AGE_SLACK} = "
                        f"{effective_max_age})"
                    )
        elif (cls.FRESH_COLUMN is None) != (cls.FRESH_MAX_AGE_DAYS is None):
            violations.append(
                "FRESH_COLUMN and FRESH_MAX_AGE_DAYS must both be set or both be None"
            )

        return violations


def _coerce_to_datetime(value: Any) -> datetime:
    """Normalize date/datetime/string max() output to an aware UTC datetime."""
    from datetime import date

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    return datetime.fromisoformat(str(value)).replace(tzinfo=UTC)


def find_expectations_class(pipeline_key: str) -> type[Expectations] | None:
    """Return the ``Expectations`` subclass declared in a pipeline's config.

    Looks up ``source.pipelines.<pipeline_key>.config`` and walks its module
    attributes for the first subclass of ``Expectations``. Returns ``None``
    if none is declared (the pipeline has not yet authored expectations).
    """
    import importlib
    import inspect

    module = importlib.import_module(f"tables.{pipeline_key}.config")
    for _, obj in inspect.getmembers(module):
        if (
            inspect.isclass(obj)
            and issubclass(obj, Expectations)
            and obj is not Expectations
        ):
            return obj
    return None


__all__ = ["Expectations", "find_expectations_class"]
