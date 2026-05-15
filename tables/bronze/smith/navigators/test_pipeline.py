"""Per-scenario tests for analytics.bronze.smith_navigators."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.bronze.smith.navigators.config import SmithNavigatorBronze
from tables.bronze.smith.navigators.fixtures import empty, smoke
from tables.bronze.smith.navigators.transform import compute


class TestSmithNavigatorsBronzeTransform:
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
        assert row["navigator_id"] == "navigator-1"
        assert row["email"] == "maria.silva@straloo.com.br"
        expected_columns = {f.name for f in SmithNavigatorBronze.to_struct().fields}
        assert set(result.columns) == expected_columns
