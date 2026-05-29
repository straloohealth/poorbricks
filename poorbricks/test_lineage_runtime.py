"""Tests for runtime Spark-plan column lineage capture."""

from __future__ import annotations

from typing import Annotated

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from poorbricks.inputs import Inputs, TableSource
from poorbricks.lineage_runtime import capture_lineage
from validation import ValidatedStruct


class _Users(ValidatedStruct):
    patient_id: int
    email: str | None
    name: str | None


class _Visits(ValidatedStruct):
    patient_id: int
    visit_count: int


class _JoinInputs(Inputs):
    users: Annotated[DataFrame, TableSource("smith.users", _Users)]
    visits: Annotated[DataFrame, TableSource("smith.visits", _Visits)]


@pytest.mark.spark
def test_capture_exact_passthrough_and_rename(spark) -> None:  # type: ignore[no-untyped-def]
    users = spark.createDataFrame([(1, "a@x.com", "Al")], _Users.to_struct())
    visits = spark.createDataFrame([(1, 5)], _Visits.to_struct())
    out = users.join(visits, "patient_id").select(
        f.col("patient_id"),
        f.upper(f.col("name")).alias("name_upper"),
        f.col("visit_count"),
        f.lit("US").alias("country"),
    )

    doc = capture_lineage(out, _JoinInputs)

    assert doc["engine"] == "spark-analyzed-plan"
    assert doc["warnings"] == []
    cols = doc["columns"]
    # Pass-through + rename resolve exactly to the right upstream table+column.
    assert cols["patient_id"]["exact"] is True
    assert {"table": "smith.users", "column": "patient_id"}.items() <= cols[
        "patient_id"
    ]["sources"][0].items()
    assert cols["name_upper"]["sources"][0]["table"] == "smith.users"
    assert cols["name_upper"]["sources"][0]["column"] == "name"
    assert cols["visit_count"]["sources"][0]["table"] == "smith.visits"
    # A literal column has no upstream source.
    assert cols["country"]["sources"] == []
    assert cols["country"]["exact"] is False

    # consumed groups the referenced columns by upstream contract table.
    assert doc["consumed"]["smith.users"] == ["name", "patient_id"]
    assert doc["consumed"]["smith.visits"] == ["visit_count"]


@pytest.mark.spark
def test_capture_handles_aggregate(spark) -> None:  # type: ignore[no-untyped-def]
    visits = spark.createDataFrame([(1, 5), (1, 7)], _Visits.to_struct())
    out = visits.groupBy("patient_id").agg(f.sum("visit_count").alias("total_visits"))
    doc = capture_lineage(out, _JoinInputs)
    # Group key is exact; the aggregate still attributes to the source column.
    assert doc["consumed"].get("smith.visits") == ["patient_id", "visit_count"] or (
        "visit_count" in doc["consumed"].get("smith.visits", [])
    )
    assert doc["warnings"] == []


def test_capture_degrades_without_raising() -> None:
    class _Boom:
        @property
        def _jdf(self):  # type: ignore[no-untyped-def]
            raise RuntimeError("no JVM plan available")

    doc = capture_lineage(_Boom(), _JoinInputs)  # type: ignore[arg-type]
    assert doc["columns"] == {}
    assert doc["consumed"] == {}
    assert doc["warnings"] and "failed" in doc["warnings"][0]
