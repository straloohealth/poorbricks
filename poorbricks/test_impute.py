"""Tests for the impute-with-warn feature:

* ``MongoSource.read_schema`` relaxes only the declared ``nullable_columns`` so
  a null/missing source value never aborts the read.
* ``persist._apply_imputations`` coalesces those columns' nulls to their
  ``Expectations.IMPUTE_DEFAULTS`` default, keeps the rows, and reports the
  imputed-row count (surfaced as a non-critical warning by /v1/verification).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from poorbricks.inputs import MongoSource
from poorbricks.runner import _coalesce_defaults


def _contract_schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("is_active", BooleanType(), nullable=False),
        ]
    )


def _nullable_df(spark: SparkSession, rows: list[tuple]):  # -> DataFrame
    return spark.createDataFrame(
        rows,
        StructType(
            [
                StructField("id", StringType(), nullable=True),
                StructField("is_active", BooleanType(), nullable=True),
            ]
        ),
    )


def test_read_schema_relaxes_only_declared_columns() -> None:
    src = MongoSource(
        db="d",
        collection="c",
        schema=_contract_schema(),
        nullable_columns=("is_active",),
    )
    nullable = {f.name: f.nullable for f in src.read_schema.fields}
    assert nullable == {"id": False, "is_active": True}


def test_read_schema_is_strict_when_nothing_declared() -> None:
    src = MongoSource(db="d", collection="c", schema=_contract_schema())
    assert all(not f.nullable for f in src.read_schema.fields)


def test_coalesce_defaults_fills_nulls_and_counts(spark: SparkSession) -> None:
    df = _nullable_df(spark, [("a", True), ("b", None), ("c", None)])
    out, imputed = _coalesce_defaults(df, {"is_active": False})
    assert imputed == {"is_active": 2}
    rows = {r["id"]: r["is_active"] for r in out.collect()}
    assert rows == {"a": True, "b": False, "c": False}  # rows kept, nulls -> default
    assert out.filter(out.is_active.isNull()).count() == 0


def test_coalesce_defaults_records_nothing_when_no_nulls(spark: SparkSession) -> None:
    df = _nullable_df(spark, [("a", True), ("b", False)])
    _out, imputed = _coalesce_defaults(df, {"is_active": False})
    assert imputed == {}  # nothing was defaulted


def test_coalesce_defaults_noop_without_declared_defaults(spark: SparkSession) -> None:
    df = _nullable_df(spark, [("a", None)])
    out, imputed = _coalesce_defaults(df, {})
    assert imputed == {}
    assert out is df  # untouched DataFrame


def test_coalesce_defaults_ignores_undeclared_columns(spark: SparkSession) -> None:
    # A default for a column not in the DataFrame is simply skipped.
    df = _nullable_df(spark, [("a", None)])
    out, imputed = _coalesce_defaults(df, {"missing_col": 0, "is_active": False})
    assert imputed == {"is_active": 1}
    assert out.filter(out.is_active.isNull()).count() == 0
