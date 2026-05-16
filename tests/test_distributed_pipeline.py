"""Distributed pipeline integration test.

Simulates three isolated repos running in sequence:
  "Bronze repo"  — framework-repo/tables/bronze/
  "Silver repo"  — simulated separate repo consuming bronze contracts
  "Gold repo"    — simulated separate repo consuming silver contracts

Each phase discovers its level's pipelines, computes fixtures, writes to
PostgreSQL, and pushes contracts to MongoDB. The gold phase proves
cross-repo contract resolution: gold fixtures call
``ContractSource.from_rows()`` which reads the ``dim_patient`` schema from
MongoDB — this step fails if the silver phase did not push contracts first.

Tests share module-scope state (the populated registry, the freshly reset
PostgreSQL schemas, the cleared MongoDB contracts collection) and rely on
pytest's collection-order execution within the module: bronze runs first,
silver next, gold last, and only then do the contract assertions fire.

Prerequisites:
    docker-compose up -d   (MongoDB + PostgreSQL)

Run with:
    poetry run pytest tests/test_distributed_pipeline.py \
        -m integration -n 0 -v
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg2
import pymongo
import pytest

from poorbricks.discovery import discover_all_pipelines
from poorbricks.persist import run_and_persist
from poorbricks.registry import all_pipelines
from utils.contracts import list_contracts
from utils.postgres import PostgresLoader

pytestmark = [
    pytest.mark.integration,
    pytest.mark.xdist_group("distributed_pipeline"),
]

LEVELS = ("bronze", "silver", "gold")


def _pg_table_name(table_name: str) -> str:
    """Map logical table name to a PostgreSQL-safe table name (no dots)."""
    return table_name.replace(".", "_")


@pytest.fixture(scope="module", autouse=True)
def _registry_and_state() -> Iterator[None]:
    """Snapshot the registry, populate it once, and reset Postgres + Mongo.

    Module-scope (not function-scope, unlike ``test_multi_repo.py``) because
    discovery must run once and persist across the bronze/silver/gold tests:
    silver's ContractSource resolution depends on bronze contracts already
    being in Mongo, and gold's depends on silver's.
    """
    from poorbricks import registry as _registry
    from poorbricks.settings import settings

    saved_pipelines = dict(_registry._pipelines)
    saved_scenarios = {k: dict(v) for k, v in _registry._scenarios.items()}

    _registry._pipelines.clear()
    _registry._scenarios.clear()
    discover_all_pipelines()

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

    uri = settings.contracts_mongo_uri or settings.mongo_uri
    client: pymongo.MongoClient[Any] = pymongo.MongoClient(uri)
    try:
        client[settings.contracts_db][settings.contracts_collection].delete_many({})
    finally:
        client.close()

    try:
        yield
    finally:
        _registry._pipelines.clear()
        _registry._pipelines.update(saved_pipelines)
        _registry._scenarios.clear()
        _registry._scenarios.update(saved_scenarios)


@pytest.fixture(scope="module")
def pg_loader() -> PostgresLoader:
    return PostgresLoader()


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


def _run_level_and_verify(level: str, loader: PostgresLoader) -> None:
    """Run every pipeline at ``level`` and assert PG row counts match."""
    pipelines = {k: v for k, v in all_pipelines().items() if v.level == level}
    assert pipelines, f"no pipelines registered for level={level!r}"

    failures: list[str] = []
    for key, meta in sorted(pipelines.items()):
        result = run_and_persist(key, mode="fixtures")
        expected = result.rows or 0
        actual = _count_postgres_rows(loader, level, _pg_table_name(meta.table_name))
        if actual != expected or actual < 0:
            failures.append(
                f"{level}.{meta.table_name}: expected {expected} rows, "
                f"got {actual} (table missing if -1)"
            )

    if failures:
        pytest.fail("\n".join(failures))


# ---------------------------------------------------------------------------
# Tests — collection order = execution order within this module
# ---------------------------------------------------------------------------


def test_pipeline_discovery_finds_all_levels() -> None:
    """Sanity: registry must contain pipelines for bronze, silver, and gold."""
    levels_found = {meta.level for meta in all_pipelines().values()}
    missing = set(LEVELS) - levels_found
    assert not missing, f"no pipelines registered for levels: {sorted(missing)}"


def test_bronze_pipelines_persist_rows(pg_loader: PostgresLoader) -> None:
    """Run every bronze pipeline, verify rows landed in Postgres."""
    _run_level_and_verify("bronze", pg_loader)


def test_silver_pipelines_persist_rows(pg_loader: PostgresLoader) -> None:
    """Run every silver pipeline — requires bronze contracts in Mongo."""
    _run_level_and_verify("silver", pg_loader)


def test_gold_pipelines_persist_rows(pg_loader: PostgresLoader) -> None:
    """Run every gold pipeline — requires silver contracts in Mongo."""
    _run_level_and_verify("gold", pg_loader)


def test_all_expected_contracts_pushed_to_mongo() -> None:
    """Every registered pipeline must have pushed its contract to Mongo."""
    contracts = list_contracts()
    assert contracts, "no contracts found in MongoDB after all levels ran"

    expected = {m.table_name for m in all_pipelines().values()}
    pushed = {d.get("table_name") for d in contracts}
    missing = expected - pushed
    assert not missing, f"contracts missing for: {sorted(missing)}"
