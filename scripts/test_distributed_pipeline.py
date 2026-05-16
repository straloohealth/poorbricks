"""Distributed pipeline integration test.

Simulates three isolated repos running in sequence:
  "Bronze repo"  — framework-repo/tables/bronze/
  "Silver repo"  — simulated separate repo consuming bronze contracts
  "Gold repo"    — simulated separate repo consuming silver contracts

Each phase discovers its level's pipelines, computes fixtures, writes to PostgreSQL,
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
from poorbricks.persist import run_and_persist
from poorbricks.registry import all_pipelines
from utils.contracts import list_contracts
from utils.postgres import PostgresLoader

LEVELS = ("bronze", "silver", "gold")


def _pg_table_name(table_name: str) -> str:
    """Map logical table name to a PostgreSQL-safe table name (no dots)."""
    return table_name.replace(".", "_")


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


def _reset_postgres() -> None:
    loader = PostgresLoader()
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
# Phase runner
# ---------------------------------------------------------------------------


def _run_level(level: str) -> dict[str, int]:
    pipelines = {k: v for k, v in all_pipelines().items() if v.level == level}
    if not pipelines:
        print(f"  [warn] no pipelines found for level={level!r}")
        return {}

    row_counts: dict[str, int] = {}
    for key, meta in sorted(pipelines.items()):
        print(f"  running {key} ...", end=" ", flush=True)
        result = run_and_persist(key, mode="fixtures")
        row_counts[meta.table_name] = result.rows or 0
        print(f"{result.rows} rows  ✓")

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


def _verify(row_counts: dict[str, dict[str, int]]) -> bool:
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    loader = PostgresLoader()

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

    # Phase 0: reset
    print("\n[Phase 0] Reset")
    _reset_postgres()
    _reset_contracts()

    all_row_counts: dict[str, dict[str, int]] = {}

    # Phase 1–3: one level at a time (simulates isolated repos)
    for level in LEVELS:
        print(f"\n[Phase {LEVELS.index(level) + 1}] {level.capitalize()} repo")
        print(f"  TABLES_ROOT (simulated): tables/{level}/")
        try:
            counts = _run_level(level)
            all_row_counts[level] = counts
        except Exception as exc:
            print(f"  [FAIL] {type(exc).__name__}: {exc}")
            return 1

    ok = _verify(all_row_counts)

    print("\n" + ("=" * 60))
    if ok:
        print("RESULT: ALL CHECKS PASSED")
    else:
        print("RESULT: SOME CHECKS FAILED — see output above")
    print("=" * 60)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
