"""Distributed pipeline integration test.

Simulates three isolated repos running in sequence:
  "Bronze repo"  — framework-repo/tables/bronze/
  "Silver repo"  — simulated separate repo consuming bronze contracts
  "Gold repo"    — simulated separate repo consuming silver contracts

Each phase discovers its level's pipelines, runs fixtures, writes to PostgreSQL,
and pushes contracts to MongoDB.  The gold phase proves cross-repo contract
resolution: gold fixtures call ContractSource.from_rows() which reads the
dim_patient schema from MongoDB — this step fails if the silver phase did not
push contracts first.

Prerequisites:
    docker-compose up -d   (MongoDB + PostgreSQL)

Usage:
    poetry run python scripts/test_distributed_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import psycopg2
import pymongo

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from poorbricks.discovery import discover_all_pipelines
from poorbricks.registry import PipelineMeta, all_pipelines
from poorbricks.runner import run
from utils.contracts import list_contracts, profile_dataframe, push_contract
from utils.postgres import PostgresLoader

LEVELS = ("bronze", "silver", "gold")

# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


def _reset_postgres(loader: PostgresLoader) -> None:
    conn = loader._connect()
    try:
        with conn.cursor() as cur:
            for schema in LEVELS:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                cur.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()
    finally:
        conn.close()
    print("  PostgreSQL: bronze / silver / gold schemas reset")


def _reset_contracts() -> None:
    from poorbricks.settings import settings

    uri = settings.contracts_mongo_uri or settings.mongo_uri
    client: pymongo.MongoClient[Any] = pymongo.MongoClient(uri)
    col = client[settings.contracts_db][settings.contracts_collection]
    deleted = col.delete_many({}).deleted_count
    client.close()
    print(f"  MongoDB: {deleted} contract(s) cleared")


# ---------------------------------------------------------------------------
# Per-pipeline helpers
# ---------------------------------------------------------------------------


def _flatten_fields(schema_json: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for field in schema_json.get("fields", []):
        spark_type = field.get("type")
        type_label = spark_type.get("type", "struct") if isinstance(spark_type, dict) else str(spark_type)
        fields.append({"name": field["name"], "type": type_label, "nullable": field.get("nullable", True)})
    return fields


def _run_pipeline(meta: PipelineMeta) -> Any:
    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")
    result = run(pipeline_key, mode="fixtures")
    if result.df is None:
        raise ValueError(f"Pipeline {meta.table_name!r} returned no DataFrame in fixtures mode")
    return result.df


def _pg_table_name(table_name: str) -> str:
    """Map logical table name to a PostgreSQL-safe table name (no dots)."""
    return table_name.replace(".", "_")


def _write_to_postgres(df: Any, meta: PipelineMeta, loader: PostgresLoader) -> int:
    loader.ensure_schema(meta.level)
    return loader.write(df, meta.level, _pg_table_name(meta.table_name))


def _push_pipeline_contract(df: Any, meta: PipelineMeta) -> None:
    schema = meta.model.to_struct()
    schema_json = schema.jsonValue()
    example_rows = [r.asDict(recursive=True) for r in df.limit(5).collect()]
    profile = profile_dataframe(df)

    push_contract(
        table_name=meta.table_name,
        schema=schema,
        example_rows=example_rows,
        pipeline_key=f"{meta.target_storage}:{meta.table_name}",
        level=meta.level,
        profile=profile,
        storage=meta.target_storage,
        comment=meta.comment,
        module=meta.module,
        fields=_flatten_fields(schema_json),
    )


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------


def _run_level(level: str, loader: PostgresLoader) -> dict[str, int]:
    pipelines = {k: v for k, v in all_pipelines().items() if v.level == level}
    if not pipelines:
        print(f"  [warn] no pipelines found for level={level!r}")
        return {}

    row_counts: dict[str, int] = {}
    for key, meta in sorted(pipelines.items()):
        print(f"  running {key} ...", end=" ", flush=True)
        df = _run_pipeline(meta)
        rows = _write_to_postgres(df, meta, loader)
        _push_pipeline_contract(df, meta)
        row_counts[meta.table_name] = rows
        print(f"{rows} rows  ✓")

    return row_counts


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _count_postgres_rows(loader: PostgresLoader, schema: str, table: str) -> int:
    conn = loader._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            result = cur.fetchone()
            return int(result[0]) if result else 0
    except psycopg2.errors.UndefinedTable:
        return -1
    finally:
        conn.close()


def _verify(row_counts: dict[str, dict[str, int]], loader: PostgresLoader) -> bool:
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # PostgreSQL check
    print("\nPostgreSQL row counts:")
    all_ok = True
    for level in LEVELS:
        counts = row_counts.get(level, {})
        for table, expected in counts.items():
            actual = _count_postgres_rows(loader, level, _pg_table_name(table))
            status = "✓" if actual == expected and actual >= 0 else "✗"
            if actual != expected or actual < 0:
                all_ok = False
            print(f"  {level}.{table:<30s} {actual:>5d} rows  {status}")

    # MongoDB contracts check
    print("\nMongoDB contracts:")
    contracts = list_contracts()
    for doc in sorted(contracts, key=lambda d: (d.get("level", ""), d.get("table_name", ""))):
        table = doc.get("table_name", "?")
        level = doc.get("level", "?")
        pushed_at = str(doc.get("pushed_at", ""))[:19]
        print(f"  [{level}] {table:<30s}  pushed {pushed_at}")

    if not contracts:
        print("  [none]")
        all_ok = False

    expected_tables = {m.table_name for m in all_pipelines().values()}
    pushed_tables = {d.get("table_name") for d in contracts}
    missing = expected_tables - pushed_tables
    if missing:
        print(f"\n  [warn] contracts missing for: {sorted(missing)}")
        all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    print("Discovering all pipelines ...")
    discover_all_pipelines()
    pipelines = all_pipelines()
    print(f"  Found {len(pipelines)} pipeline(s): {sorted(pipelines)}")

    loader = PostgresLoader()

    # Phase 0: reset
    print("\n[Phase 0] Reset")
    _reset_postgres(loader)
    _reset_contracts()

    all_row_counts: dict[str, dict[str, int]] = {}

    # Phase 1–3: one level at a time (simulates isolated repos)
    for level in LEVELS:
        print(f"\n[Phase {LEVELS.index(level) + 1}] {level.capitalize()} repo")
        print(f"  TABLES_ROOT (simulated): tables/{level}/")
        try:
            counts = _run_level(level, loader)
            all_row_counts[level] = counts
        except Exception as exc:
            print(f"  [FAIL] {type(exc).__name__}: {exc}")
            return 1

    ok = _verify(all_row_counts, loader)

    print("\n" + ("=" * 60))
    if ok:
        print("RESULT: ALL CHECKS PASSED")
    else:
        print("RESULT: SOME CHECKS FAILED — see output above")
    print("=" * 60)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
