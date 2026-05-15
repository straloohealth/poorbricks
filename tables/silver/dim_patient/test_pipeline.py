"""Per-scenario tests for silver.dim_patient."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from tables.silver.dim_patient.config import DimPatient
from tables.silver.dim_patient.fixtures import (
    duplicate_patient_id,
    empty,
    smoke,
)
from tables.silver.dim_patient.transform import compute


class TestDimPatientTransform:
    @pytest.mark.spark
    def test_empty_inputs_produce_empty_output(self, spark: SparkSession) -> None:
        result = compute(empty())
        assert result.count() == 0

    @pytest.mark.spark
    def test_smoke_three_patients(self, spark: SparkSession) -> None:
        result = compute(smoke()).collect()
        assert len(result) == 3
        ids = {r["patient_id"] for r in result}
        assert ids == {"p1", "p2", "p3"}
        by_id = {r["patient_id"]: r for r in result}
        # external_id is mapped to mongo_id (legacy ObjectId analog).
        assert by_id["p1"]["mongo_id"] == "mongo-p1"
        # origin → origin_slug for p3 (set to 'ge' in the fixture).
        assert by_id["p3"]["origin_slug"] == "ge"
        # is_active reflects bronze.active for the inactive patient.
        assert by_id["p2"]["is_active"] is False
        # name/email/phone are null-padded until bronze grows those columns.
        assert by_id["p1"]["name"] is None

    @pytest.mark.spark
    def test_duplicate_patient_id_keeps_latest(self, spark: SparkSession) -> None:
        result = compute(duplicate_patient_id()).collect()
        assert len(result) == 1
        assert result[0]["patient_id"] == "p1"
        # Latest row wins on the external_id → mongo_id mapping.
        assert result[0]["mongo_id"] == "mongo-p1-new"

    @pytest.mark.spark
    def test_output_columns_match_dim_patient_schema(self, spark: SparkSession) -> None:
        result = compute(smoke())
        expected = {f.name for f in DimPatient.to_struct().fields}
        assert set(result.columns) == expected
        DimPatient.verify(result, strict=True)
