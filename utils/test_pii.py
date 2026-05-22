"""Tests for the PII hashing helper."""

from __future__ import annotations

import hashlib

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from poorbricks.settings import settings
from utils.pii import hash_pii


def _expected(value: str) -> str:
    normalized = value.strip().lower()
    return hashlib.sha256(
        (normalized + settings.pii_hash_salt).encode("utf-8")
    ).hexdigest()


class TestHashPii:
    """Test cases for hash_pii."""

    @pytest.mark.spark
    def test_hashes_value_with_salt(self, spark: SparkSession) -> None:
        """A value hashes to the salted SHA-256 of its normalised form."""
        df = spark.createDataFrame([{"cpf": "12345678901"}])
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        assert result.collect()[0]["h"] == _expected("12345678901")

    @pytest.mark.spark
    def test_same_value_hashes_equally(self, spark: SparkSession) -> None:
        """Equal inputs hash equally — so the hash works as a join key."""
        df = spark.createDataFrame([{"cpf": "12345678901"}, {"cpf": "12345678901"}])
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        hashes = {row["h"] for row in result.collect()}
        assert len(hashes) == 1

    @pytest.mark.spark
    def test_casing_and_whitespace_normalised(self, spark: SparkSession) -> None:
        """Values differing only in case/whitespace hash to the same digest."""
        df = spark.createDataFrame([{"cpf": "  ABC123  "}, {"cpf": "abc123"}])
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        hashes = {row["h"] for row in result.collect()}
        assert len(hashes) == 1

    @pytest.mark.spark
    def test_different_values_hash_differently(self, spark: SparkSession) -> None:
        """Distinct inputs produce distinct hashes."""
        df = spark.createDataFrame([{"cpf": "11111111111"}, {"cpf": "22222222222"}])
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        hashes = {row["h"] for row in result.collect()}
        assert len(hashes) == 2

    @pytest.mark.spark
    def test_null_input_returns_null(self, spark: SparkSession) -> None:
        """A null value yields a null hash rather than hashing the salt alone."""
        df = spark.createDataFrame([{"cpf": None}], "cpf string")
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        assert result.collect()[0]["h"] is None

    @pytest.mark.spark
    def test_empty_string_returns_null(self, spark: SparkSession) -> None:
        """An empty/whitespace value yields a null hash."""
        df = spark.createDataFrame([{"cpf": "   "}])
        result = df.select(hash_pii(f.col("cpf")).alias("h"))
        assert result.collect()[0]["h"] is None
