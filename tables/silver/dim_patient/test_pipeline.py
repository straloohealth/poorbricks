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
        assert ids == {
            "507f1f77bcf86cd799439011",
            "507f1f77bcf86cd799439012",
            "507f1f77bcf86cd799439013",
        }
        by_id = {r["patient_id"]: r for r in result}
        assert (
            by_id["507f1f77bcf86cd799439011"]["mongo_id"] == "507f1f77bcf86cd799439011"
        )
        assert by_id["507f1f77bcf86cd799439013"]["origin_slug"] == "ge"
        assert by_id["507f1f77bcf86cd799439012"]["is_active"] is False
        assert by_id["507f1f77bcf86cd799439011"]["name"] == "Test Patient"

    @pytest.mark.spark
    def test_duplicate_patient_id_keeps_latest(self, spark: SparkSession) -> None:
        from datetime import datetime

        result = compute(duplicate_patient_id()).collect()
        assert len(result) == 1
        assert result[0]["patient_id"] == "507f1f77bcf86cd799439011"
        # Latest row wins — created_at must be from the newer fixture row.
        assert result[0]["created_at"] == datetime(2026, 1, 15, 12, 0, 0)

    @pytest.mark.spark
    def test_output_columns_match_dim_patient_schema(self, spark: SparkSession) -> None:
        result = compute(smoke())
        expected = {f.name for f in DimPatient.to_struct().fields}
        assert set(result.columns) == expected
        DimPatient.verify(result, strict=True)
