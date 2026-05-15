"""Per-scenario tests for analytics.bronze.smith_users."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.bronze.smith.users.config import SmithUserBronze
from tables.bronze.smith.users.fixtures import empty, smoke
from tables.bronze.smith.users.transform import compute


class TestSmithUsersBronzeTransform:
    @pytest.mark.spark
    def test_empty_inputs_produce_empty_output(self, spark: SparkSession) -> None:
        result = compute(empty())
        assert result.count() == 0

    @pytest.mark.spark
    def test_smoke_row_passes_through_with_bronze_schema(
        self, spark: SparkSession
    ) -> None:
        result = compute(smoke())
        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["patient_id"] == "patient-1"
        assert row["origin"] == "aon"
        assert row["gender"] == "FEMALE"
        expected_columns = {f.name for f in SmithUserBronze.to_struct().fields}
        assert set(result.columns) == expected_columns
