"""Tests for bronze.example_items transform."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.bronze.example.items.fixtures import empty, smoke
from tables.bronze.example.items.transform import compute


@pytest.mark.spark
class TestExampleItemsBronzeTransform:
    def test_empty_inputs_produce_empty_output(self, spark: SparkSession) -> None:
        result = compute(empty())
        assert result.count() == 0

    def test_smoke_rows_pass_through_with_bronze_schema(
        self, spark: SparkSession
    ) -> None:
        result = compute(smoke())
        rows = result.collect()
        assert len(rows) == 2
        ids = {r["item_id"] for r in rows}
        assert ids == {"item-001", "item-002"}

    def test_output_columns_match_item_schema(self, spark: SparkSession) -> None:
        from tables.bronze.example.items.config import Item

        expected = {f.name for f in Item.to_struct().fields}
        result = compute(smoke())
        assert set(result.columns) == expected
