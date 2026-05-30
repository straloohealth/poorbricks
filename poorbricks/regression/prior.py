"""Regression-vs-prior: compare a fresh pipeline output to its previous run.

Because the writer overwrites the live table each run, the previous output must
be snapshotted to survive into the next run. After a successful write we copy
the table to ``poorbricks_meta."<schema>__<table>__prev"`` (a single server-side
``CREATE TABLE ... AS SELECT`` — no Spark, no driver collect). On the *next* run,
``diff_against_prior`` compares the current table to that snapshot using the
existing :class:`MigrationDiff` harness and reports any column regressions.

First run (no snapshot yet) returns ``None`` — nothing to compare against.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .diff import MigrationDiff, MigrationReport
from .sources import PostgresSource

if TYPE_CHECKING:
    from utils.postgres import PostgresLoader

META_SCHEMA = "poorbricks_meta"


def _prev_table(schema: str, table: str) -> str:
    return f"{schema}__{table}__prev"


def _dsn(loader: PostgresLoader) -> str:
    return (
        f"host={loader.host} port={loader.port} dbname={loader.db} "
        f"user={loader.user} password={loader.password}"
    )


def _prev_exists(loader: PostgresLoader, prev_table: str) -> bool:
    conn = loader._connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'{META_SCHEMA}."{prev_table}"',))
            row = cur.fetchone()
            return row is not None and row[0] is not None
    finally:
        conn.close()


def snapshot_prior(loader: PostgresLoader, schema: str, table: str) -> None:
    """Refresh the prior-run snapshot of ``<schema>.<table>`` (best-effort)."""
    prev = _prev_table(schema, table)
    conn = loader._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{META_SCHEMA}"')
            cur.execute(f'DROP TABLE IF EXISTS "{META_SCHEMA}"."{prev}"')
            cur.execute(
                f'CREATE TABLE "{META_SCHEMA}"."{prev}" AS '
                f'SELECT * FROM "{schema}"."{table}"'
            )
        conn.commit()
    finally:
        conn.close()


def drop_prior(loader: PostgresLoader, schema: str, table: str) -> None:
    """Drop a prior-run snapshot (best-effort).

    Used when a run is too large to diff: a stale snapshot must not linger, or a
    later under-cap run would compare against an ancient frozen copy and raise a
    false regression.
    """
    prev = _prev_table(schema, table)
    conn = loader._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{META_SCHEMA}"."{prev}"')
        conn.commit()
    finally:
        conn.close()


def diff_against_prior(
    loader: PostgresLoader,
    schema: str,
    table: str,
    join_keys: list[str],
    *,
    tolerance_pct: float = 10.0,
    ignore_columns: list[str] | None = None,
    label: str | None = None,
) -> MigrationReport | None:
    """Diff the current table against its prior snapshot, or ``None`` if first run."""
    if not join_keys:
        return None
    prev = _prev_table(schema, table)
    if not _prev_exists(loader, prev):
        return None
    dsn = _dsn(loader)
    report = MigrationDiff(
        reference=PostgresSource(
            dsn=dsn, sql=f'SELECT * FROM "{META_SCHEMA}"."{prev}"'
        ),
        candidate=PostgresSource(dsn=dsn, sql=f'SELECT * FROM "{schema}"."{table}"'),
        join_keys=list(join_keys),
        default_tolerance_pct=tolerance_pct,
        ignore_columns=list(ignore_columns or []),
        label=label or f"{schema}.{table}",
    ).run()
    return report


def regression_summary(report: MigrationReport) -> dict[str, Any]:
    """Compact, BSON/JSON-safe summary of a regression diff for run history."""
    regressions = report.regressions()
    return {
        "row_counts": report.row_counts,
        "regression_count": len(regressions),
        "regressed_columns": [
            {
                "name": c.name,
                "status": c.status,
                "mismatch_pct": round(c.mismatch_pct, 2),
                "tolerance": round(c.tolerance, 2),
            }
            for c in regressions
        ],
    }


__all__ = [
    "diff_against_prior",
    "drop_prior",
    "regression_summary",
    "snapshot_prior",
]
