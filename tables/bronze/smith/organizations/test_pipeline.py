"""Per-scenario tests for analytics.bronze.smith_organizations."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.bronze.smith.organizations.config import (
    SmithOrganizationBronze,
)
from tables.bronze.smith.organizations.fixtures import empty, smoke
from tables.bronze.smith.organizations.transform import compute


class TestSmithOrganizationsBronzeTransform:
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
        slugs = {r["slug"] for r in rows}
        assert slugs == {"aon", "sepaco"}
        expected_columns = {f.name for f in SmithOrganizationBronze.to_struct().fields}
        assert set(result.columns) == expected_columns
