"""Gold patients pipeline — migrated from framework-repo/tables/gold/patients/."""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import ContractSource, Inputs, pipeline
from tables.gold_patients.config import PatientGold
from tables.gold_patients.transform import compute


class GoldPatientsInputs(Inputs):
    dim_patient: Annotated[DataFrame, ContractSource("dim_patient")]


@pipeline(
    name="gold_patients",
    model=PatientGold,
    level="gold",
    storage="postgres",
    comment="Test migration — gold patients projecting silver.dim_patient.",
)
def gold_patients(inputs: GoldPatientsInputs) -> DataFrame:
    return compute(inputs)
