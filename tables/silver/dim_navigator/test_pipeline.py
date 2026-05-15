"""Per-scenario tests for silver.dim_navigator."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.silver.dim_navigator.config import DimNavigator
from tables.silver.dim_navigator.fixtures import empty, smoke
from tables.silver.dim_navigator.transform import compute


class TestDimNavigatorTransform:
    @pytest.mark.spark
    def test_empty_inputs_produce_empty_output(self, spark: SparkSession) -> None:
        assert compute(empty()).count() == 0

    @pytest.mark.spark
    def test_smoke_three_navigators(self, spark: SparkSession) -> None:
        result = compute(smoke()).collect()
        assert len(result) == 3
        ids = {r["navigator_id"] for r in result}
        assert ids == {"n1", "n2", "n3"}

    @pytest.mark.spark
    def test_output_columns_match_dim_navigator_schema(
        self, spark: SparkSession
    ) -> None:
        result = compute(smoke())
        expected = {f.name for f in DimNavigator.to_struct().fields}
        assert set(result.columns) == expected
        DimNavigator.verify(result, strict=True)
