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
from poorbricks.persist import _apply_imputations
from validation.expectations import Expectations


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


class _Exp(Expectations):
    IMPUTE_DEFAULTS = {"is_active": False}


def test_apply_imputations_coalesces_nulls_and_counts(spark: SparkSession) -> None:
    df = _nullable_df(spark, [("a", True), ("b", None), ("c", None)])
    out, imputed = _apply_imputations(df, _Exp)
    assert imputed == {"is_active": 2}
    rows = {r["id"]: r["is_active"] for r in out.collect()}
    assert rows == {"a": True, "b": False, "c": False}  # rows kept, nulls -> default
    assert out.filter(out.is_active.isNull()).count() == 0


def test_apply_imputations_records_nothing_when_no_nulls(spark: SparkSession) -> None:
    df = _nullable_df(spark, [("a", True), ("b", False)])
    _out, imputed = _apply_imputations(df, _Exp)
    assert imputed == {}  # nothing was defaulted


def test_apply_imputations_noop_without_declared_defaults(spark: SparkSession) -> None:
    class _NoExp(Expectations):
        pass

    df = _nullable_df(spark, [("a", None)])
    out, imputed = _apply_imputations(df, _NoExp)
    assert imputed == {}
    assert out is df  # untouched DataFrame


def test_apply_imputations_ignores_undeclared_columns(spark: SparkSession) -> None:
    # A default for a column not in the DataFrame is simply skipped.
    class _Exp2(Expectations):
        IMPUTE_DEFAULTS = {"missing_col": 0, "is_active": False}

    df = _nullable_df(spark, [("a", None)])
    out, imputed = _apply_imputations(df, _Exp2)
    assert imputed == {"is_active": 1}
    assert out.filter(out.is_active.isNull()).count() == 0
