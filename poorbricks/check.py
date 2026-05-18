"""Row-count gate for postgres-backed pipelines.

CLI:

    poorbricks check --pipeline <dotted-key>

Looks up the registered pipeline, queries
``<settings.postgres_db>.<meta.level>.<table>`` for ``COUNT(*)``, and
compares it to ``Expectations.MIN_ROWS``. Emits a single log line that
Airflow surfaces in the task view::

    [CHECK] gold.patients: 5142 rows (min=5000) OK

Exits non-zero on threshold violation so the Airflow task fails.
"""

from __future__ import annotations

import argparse
import importlib
import sys

import psycopg2

from poorbricks.registry import PipelineMeta, all_pipelines
from poorbricks.settings import settings
from validation.expectations import find_expectations_class


def _resolve_meta(pipeline_key: str) -> PipelineMeta:
    """Import ``tables.<key>.pipeline`` and return its registered meta."""
    importlib.import_module(f"tables.{pipeline_key}.pipeline")
    target_module = f"tables.{pipeline_key}.pipeline"
    for meta in all_pipelines().values():
        if meta.module == target_module:
            return meta
    raise KeyError(
        f"Pipeline {pipeline_key!r} did not register a PipelineMeta at "
        f"module {target_module!r}."
    )


def _pg_table_name(table_name: str) -> str:
    return table_name.replace(".", "_")


def _count_rows(schema: str, table: str) -> int:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise RuntimeError(f"COUNT(*) returned no row for {schema}.{table}")
    return int(row[0])


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="poorbricks check")
    parser.add_argument(
        "--pipeline",
        required=True,
        help="pipeline key, dotted form (e.g. 'gold.patients')",
    )
    args = parser.parse_args(argv)
    pipeline_key: str = args.pipeline

    meta = _resolve_meta(pipeline_key)
    if meta.target_storage != "postgres":
        print(
            f"[CHECK] {pipeline_key}: storage={meta.target_storage!r} "
            f"is not 'postgres' — nothing to check",
            file=sys.stderr,
        )
        return 1

    schema = meta.level
    table = _pg_table_name(meta.table_name)
    fq = f"{schema}.{table}"

    expectations = find_expectations_class(pipeline_key)
    min_rows: int = 0
    if expectations is not None and expectations.MIN_ROWS is not None:
        min_rows = expectations.MIN_ROWS

    count = _count_rows(schema, table)
    status = "OK" if count >= min_rows else "FAIL"
    print(f"[CHECK] {fq}: {count} rows (min={min_rows}) {status}")
    return 0 if status == "OK" else 1


__all__ = ["main"]
