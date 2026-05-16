"""Per-scenario tests for gold.patients (silver.dim_patient → projection)."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

from tables.gold.patients.config import Patients
from tables.gold.patients.fixtures import smoke
from tables.gold.patients.pipeline import PatientsGoldInputs
from tables.gold.patients.transform import compute
from tables.silver.dim_patient.config import DimPatient


@pytest.fixture(autouse=True)
def _mock_dim_patient_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid hitting the contracts store: serve dim_patient's schema locally."""
    import utils.contracts as contracts_module

    contract = {"schema_json": DimPatient.to_struct().jsonValue()}

    def fake_fetch(table_name: str) -> dict[str, Any]:
        if table_name != "dim_patient":
            raise KeyError(table_name)
        return contract

    monkeypatch.setattr(contracts_module, "fetch_contract", fake_fetch)


class TestPatientsGoldTransform:
    @pytest.mark.spark
    def test_smoke_passes_through_one_row(self, spark: SparkSession) -> None:
        result = compute(smoke())
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["patient_id"] == "p1"

    @pytest.mark.spark
    def test_empty_inputs_yields_empty_df(self, spark: SparkSession) -> None:
        empty_inputs = PatientsGoldInputs.from_rows({"dim_patient": []})
        assert compute(empty_inputs).count() == 0

    @pytest.mark.spark
    def test_output_columns_match_schema(self, spark: SparkSession) -> None:
        result = compute(smoke())
        expected = [f.name for f in Patients.to_struct().fields]
        assert result.columns == expected

    @pytest.mark.spark
    def test_silver_columns_propagate(self, spark: SparkSession) -> None:
        """Patient identity, origin slug, and clinical flags surface end-to-end."""
        row = compute(smoke()).collect()[0]
        assert row["name"] == "Maria Silva"
        assert row["email"] == "maria@example.com"
        assert row["origin_slug"] == "rede_sc"
        assert row["is_active"] is True
        assert row["is_deleted"] is False
        assert row["is_high_risk"] is False
        assert row["is_surgical"] is True
