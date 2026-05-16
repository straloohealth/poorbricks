"""Unit tests for the Expectations base class."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from validation.expectations import Expectations


def _df(spark: SparkSession, rows: list[dict[str, Any]]) -> Any:
    schema = StructType(
        [
            StructField("id", StringType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("month", TimestampType(), nullable=True),
        ]
    )
    return spark.createDataFrame(rows, schema)


@pytest.mark.spark
class TestExpectations:
    def test_clean_check_returns_empty(self, spark: SparkSession) -> None:
        rows = [
            {"id": "a", "status": "ACTIVE", "month": datetime.now()},
            {"id": "b", "status": "INACTIVE", "month": datetime.now()},
        ]

        class E(Expectations):
            MIN_ROWS = 1
            UNIQUE_KEYS = [["id"]]
            NON_NULL_COLUMNS = ["id", "status"]
            ENUM_VALUES = {"status": ["ACTIVE", "INACTIVE"]}

        assert E.check(_df(spark, rows)) == []

    def test_min_rows_violation(self, spark: SparkSession) -> None:
        class E(Expectations):
            MIN_ROWS = 5

        violations = E.check(_df(spark, [{"id": "a", "status": "X", "month": None}]))
        assert len(violations) == 1
        assert "MIN_ROWS=5" in violations[0]

    def test_unique_keys_violation(self, spark: SparkSession) -> None:
        rows = [
            {"id": "dup", "status": "X", "month": None},
            {"id": "dup", "status": "Y", "month": None},
        ]

        class E(Expectations):
            UNIQUE_KEYS = [["id"]]

        violations = E.check(_df(spark, rows))
        assert any("UNIQUE_KEYS=['id']" in v for v in violations)

    def test_non_null_columns_violation(self, spark: SparkSession) -> None:
        rows = [
            {"id": "a", "status": None, "month": None},
        ]

        class E(Expectations):
            NON_NULL_COLUMNS = ["status"]

        violations = E.check(_df(spark, rows))
        assert any("NON_NULL_COLUMNS" in v and "'status'" in v for v in violations)

    def test_null_rate_max_violation(self, spark: SparkSession) -> None:
        rows = [
            {"id": "a", "status": None, "month": None},
            {"id": "b", "status": None, "month": None},
            {"id": "c", "status": "X", "month": None},
        ]

        class E(Expectations):
            NULL_RATE_MAX = {"status": 0.1}

        violations = E.check(_df(spark, rows))
        assert any("NULL_RATE_MAX" in v and "'status'" in v for v in violations)

    def test_enum_values_violation(self, spark: SparkSession) -> None:
        rows = [
            {"id": "a", "status": "WAT", "month": None},
            {"id": "b", "status": "ACTIVE", "month": None},
        ]

        class E(Expectations):
            ENUM_VALUES = {"status": ["ACTIVE", "INACTIVE"]}

        violations = E.check(_df(spark, rows))
        assert any("ENUM_VALUES" in v and "WAT" in v for v in violations)

    def test_freshness_violation(self, spark: SparkSession) -> None:
        old = datetime.utcnow() - timedelta(days=120)
        rows = [{"id": "a", "status": "X", "month": old}]

        class E(Expectations):
            FRESH_COLUMN = "month"
            FRESH_MAX_AGE_DAYS = 30

        violations = E.check(_df(spark, rows))
        assert any("FRESH_COLUMN" in v for v in violations)

    def test_freshness_clean(self, spark: SparkSession) -> None:
        rows = [{"id": "a", "status": "X", "month": datetime.utcnow()}]

        class E(Expectations):
            FRESH_COLUMN = "month"
            FRESH_MAX_AGE_DAYS = 5

        assert E.check(_df(spark, rows)) == []

    def test_partial_freshness_config_is_violation(self, spark: SparkSession) -> None:
        class E(Expectations):
            FRESH_COLUMN = "month"
            # FRESH_MAX_AGE_DAYS unset

        violations = E.check(_df(spark, [{"id": "a", "status": "X", "month": None}]))
        assert any("must both be set" in v for v in violations)
