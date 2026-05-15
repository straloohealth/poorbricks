"""Per-scenario tests for analytics.bronze.smith_tags."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.bronze.smith.tags.config import SmithTagBronze
from tables.bronze.smith.tags.fixtures import empty, smoke
from tables.bronze.smith.tags.transform import compute


class TestSmithTagsBronzeTransform:
    @pytest.mark.spark
    def test_empty_inputs_produce_empty_output(self, spark: SparkSession) -> None:
        result = compute(empty())
        assert result.count() == 0

    @pytest.mark.spark
    def test_smoke_rows_pass_through_with_bronze_schema(
        self, spark: SparkSession
    ) -> None:
        result = compute(smoke())
        rows = result.collect()
        assert len(rows) == 2
        tag_names = {r["tags_name"] for r in rows}
        assert tag_names == {"high_risk", "churned"}
        expected_columns = {f.name for f in SmithTagBronze.to_struct().fields}
        assert set(result.columns) == expected_columns
