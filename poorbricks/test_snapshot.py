"""Roundtrip + drift tests for the JSON snapshot format."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from .snapshot import (
    SAMPLE_ROWS,
    diff_against_snapshot,
    snapshot_path,
    write_snapshot,
)


def _df(spark: SparkSession, rows: list[dict[str, object]]) -> object:
    schema = StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("count", IntegerType(), nullable=True),
        ]
    )
    return spark.createDataFrame(rows, schema)


@pytest.fixture
def isolated_snapshots_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect SNAPSHOTS_DIR so tests don't pollute the real dir."""
    monkeypatch.setattr(".snapshot.SNAPSHOTS_DIR", tmp_path)
    return tmp_path


@pytest.mark.spark
class TestSnapshotRoundtrip:
    def test_write_and_read_clean(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        rows = [{"id": "b", "count": 2}, {"id": "a", "count": 1}]
        df = _df(spark, rows)

        write_snapshot(df, "test.example", source="fixtures", sort_keys=["id"])
        path = snapshot_path("test.example")
        assert path.exists()
        payload = json.loads(path.read_text())
        assert payload["row_count"] == 2
        assert payload["sort_keys"] == ["id"]
        assert payload["sample_rows"][0]["id"] == "a"  # sorted

        # Same df re-runs as clean.
        assert diff_against_snapshot(df, "test.example") == []

    def test_row_count_drift(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        write_snapshot(_df(spark, [{"id": "a", "count": 1}]), "t", source="fixtures")
        drift = diff_against_snapshot(
            _df(spark, [{"id": "a", "count": 1}, {"id": "b", "count": 2}]), "t"
        )
        assert any("row count drift" in line for line in drift)

    def test_value_drift(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        write_snapshot(_df(spark, [{"id": "a", "count": 1}]), "t", source="fixtures")
        drift = diff_against_snapshot(_df(spark, [{"id": "a", "count": 99}]), "t")
        assert any("content hash drift" in line for line in drift)

    def test_schema_drift(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        write_snapshot(_df(spark, [{"id": "a", "count": 1}]), "t", source="fixtures")
        # New df with a different schema.
        other = spark.createDataFrame(
            [{"id": "a"}],
            StructType([StructField("id", StringType(), nullable=False)]),
        )
        drift = diff_against_snapshot(other, "t")
        assert any("Schema drift" in line for line in drift)

    def test_missing_snapshot_is_drift(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        drift = diff_against_snapshot(_df(spark, [{"id": "a", "count": 1}]), "missing")
        assert drift and "snapshot file missing" in drift[0]

    def test_sample_rows_capped(
        self, spark: SparkSession, isolated_snapshots_dir: Path
    ) -> None:
        rows = [{"id": f"id_{i:04d}", "count": i} for i in range(SAMPLE_ROWS + 30)]
        write_snapshot(_df(spark, rows), "t", source="fixtures", sort_keys=["id"])
        payload = json.loads(snapshot_path("t").read_text())
        assert len(payload["sample_rows"]) == SAMPLE_ROWS
        assert payload["row_count"] == SAMPLE_ROWS + 30
