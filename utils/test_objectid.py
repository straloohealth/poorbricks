"""Tests for the ObjectId timestamp helper."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from utils.objectid import objectid_to_timestamp


class TestObjectIdToTimestamp:
    """Test cases for objectid_to_timestamp."""

    @pytest.mark.spark
    def test_derives_creation_epoch_from_objectid(self, spark: SparkSession) -> None:
        """The first 8 hex chars decode to the creation time in Unix seconds."""
        # 0x507f1f77 == 1350844791 (2012-10-21 21:19:51 UTC)
        objectid = "507f1f77" + "0" * 16
        expected_epoch = int("507f1f77", 16)
        df = spark.createDataFrame([{"id": objectid}])
        result = df.select(
            f.unix_timestamp(objectid_to_timestamp(f.col("id"))).alias("ts")
        )
        assert result.collect()[0]["ts"] == expected_epoch

    @pytest.mark.spark
    def test_epoch_zero_objectid(self, spark: SparkSession) -> None:
        """An all-zero ObjectId decodes to the Unix epoch."""
        df = spark.createDataFrame([{"id": "0" * 24}])
        result = df.select(
            f.unix_timestamp(objectid_to_timestamp(f.col("id"))).alias("ts")
        )
        assert result.collect()[0]["ts"] == 0

    @pytest.mark.spark
    def test_null_input_returns_null(self, spark: SparkSession) -> None:
        """A null ObjectId yields a null timestamp rather than raising."""
        df = spark.createDataFrame(
            [{"id": None}],
            "id string",
        )
        result = df.select(objectid_to_timestamp(f.col("id")).alias("ts"))
        assert result.collect()[0]["ts"] is None

    @pytest.mark.spark
    def test_malformed_objectid_returns_null(self, spark: SparkSession) -> None:
        """A non-24-hex-char string yields null rather than a bogus timestamp."""
        df = spark.createDataFrame([{"id": "not-an-objectid"}])
        result = df.select(objectid_to_timestamp(f.col("id")).alias("ts"))
        assert result.collect()[0]["ts"] is None

    @pytest.mark.spark
    def test_uppercase_hex_is_accepted(self, spark: SparkSession) -> None:
        """Upper-case hex is normalised and decoded like lower-case."""
        objectid = "507F1F77" + "0" * 16
        expected_epoch = int("507f1f77", 16)
        df = spark.createDataFrame([{"id": objectid}])
        result = df.select(
            f.unix_timestamp(objectid_to_timestamp(f.col("id"))).alias("ts")
        )
        assert result.collect()[0]["ts"] == expected_epoch
