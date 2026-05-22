"""Tests for struct/array column survival across the Postgres round-trip."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from poorbricks.runner import _parse_complex_columns
from utils.postgres import _jsonify_complex_columns

_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField(
            "personal_info",
            StructType(
                [
                    StructField("gender", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField("tags", ArrayType(StringType()), True),
    ]
)


class TestComplexColumnRoundTrip:
    """_jsonify_complex_columns (write) ↔ _parse_complex_columns (read)."""

    @pytest.mark.spark
    def test_struct_and_array_serialise_to_json_text(self, spark: SparkSession) -> None:
        """The writer side turns struct/array columns into JSON strings."""
        df = spark.createDataFrame(
            [("p1", {"gender": "F", "age": 30}, ["a", "b"])], _SCHEMA
        )
        jsonified = _jsonify_complex_columns(df)
        dtypes = dict(jsonified.dtypes)
        assert dtypes["personal_info"] == "string"
        assert dtypes["tags"] == "string"
        assert dtypes["id"] == "string"

    @pytest.mark.spark
    def test_round_trip_restores_struct_and_array_values(
        self, spark: SparkSession
    ) -> None:
        """Write-then-read restores the declared complex types and values."""
        df = spark.createDataFrame(
            [("p1", {"gender": "F", "age": 30}, ["a", "b"])], _SCHEMA
        )
        restored = _parse_complex_columns(
            _jsonify_complex_columns(df), _SCHEMA.jsonValue()
        )
        assert isinstance(restored.schema["personal_info"].dataType, StructType)
        assert isinstance(restored.schema["tags"].dataType, ArrayType)
        row = restored.collect()[0]
        assert row["id"] == "p1"
        assert row["personal_info"]["gender"] == "F"
        assert row["personal_info"]["age"] == 30
        assert row["tags"] == ["a", "b"]

    @pytest.mark.spark
    def test_null_complex_value_round_trips_as_null(self, spark: SparkSession) -> None:
        """A null struct/array stays null through the round-trip."""
        df = spark.createDataFrame([("p1", None, None)], _SCHEMA)
        restored = _parse_complex_columns(
            _jsonify_complex_columns(df), _SCHEMA.jsonValue()
        )
        row = restored.collect()[0]
        assert row["personal_info"] is None
        assert row["tags"] is None

    @pytest.mark.spark
    def test_parse_without_schema_json_is_passthrough(
        self, spark: SparkSession
    ) -> None:
        """With no schema_json the read is returned untouched."""
        df = spark.createDataFrame([("p1",)], "id string")
        assert _parse_complex_columns(df, None).collect() == df.collect()
