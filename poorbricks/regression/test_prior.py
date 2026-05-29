"""Integration tests for regression-vs-prior (req 6) against local Postgres.

Validates the snapshot → diff → drop lifecycle, including the fix for the
large-table skip path (a dropped snapshot must not leave a stale baseline).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from poorbricks.regression.prior import (
    _prev_exists,
    _prev_table,
    diff_against_prior,
    drop_prior,
    regression_summary,
    snapshot_prior,
)
from utils.postgres import PostgresLoader

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("regression_prior")]

_SCHEMA = "regr_demo"
_TABLE = "orders"
_DDL = "id long, name string, amount long"


@pytest.fixture
def loader() -> Iterator[PostgresLoader]:
    ld = PostgresLoader()
    yield ld
    conn = ld._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE')
            cur.execute(
                f'DROP TABLE IF EXISTS "poorbricks_meta"."{_prev_table(_SCHEMA, _TABLE)}"'
            )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.spark
def test_first_run_has_no_prior(loader: PostgresLoader, spark: SparkSession) -> None:
    loader.write(spark.createDataFrame([(1, "A", 100)], _DDL), _SCHEMA, _TABLE)
    # No snapshot taken yet → nothing to compare against.
    assert diff_against_prior(loader, _SCHEMA, _TABLE, ["id"]) is None


@pytest.mark.spark
def test_value_drift_is_detected(loader: PostgresLoader, spark: SparkSession) -> None:
    loader.write(
        spark.createDataFrame([(1, "A", 100), (2, "B", 200)], _DDL), _SCHEMA, _TABLE
    )
    snapshot_prior(loader, _SCHEMA, _TABLE)
    # Next run: amount for id=2 changes 200 → 999.
    loader.write(
        spark.createDataFrame([(1, "A", 100), (2, "B", 999)], _DDL), _SCHEMA, _TABLE
    )
    report = diff_against_prior(loader, _SCHEMA, _TABLE, ["id"])
    assert report is not None
    summary = regression_summary(report)
    regressed = {c["name"] for c in summary["regressed_columns"]}
    assert "amount" in regressed


@pytest.mark.spark
def test_drop_prior_removes_snapshot(
    loader: PostgresLoader, spark: SparkSession
) -> None:
    loader.write(spark.createDataFrame([(1, "A", 100)], _DDL), _SCHEMA, _TABLE)
    snapshot_prior(loader, _SCHEMA, _TABLE)
    assert _prev_exists(loader, _prev_table(_SCHEMA, _TABLE)) is True
    # The large-table skip path drops the snapshot so a later run can't compare
    # against a stale baseline (regression bug H1).
    drop_prior(loader, _SCHEMA, _TABLE)
    assert _prev_exists(loader, _prev_table(_SCHEMA, _TABLE)) is False
    assert diff_against_prior(loader, _SCHEMA, _TABLE, ["id"]) is None
