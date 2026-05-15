"""Pipeline wiring for silver.dim_patient.

Reads smith_users schema from MongoDB contracts store, materializes
analytics.silver.dim_patient via PostgresLoader.
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import ContractSource, Inputs, pipeline
from tables.silver.dim_patient.config import (
    DIM_PATIENT_TABLE_NAME,
    DimPatient,
)
from tables.silver.dim_patient.transform import compute


class DimPatientInputs(Inputs):
    """Single upstream: smith_users fetched from contracts store."""

    smith_users: Annotated[DataFrame, ContractSource("smith.users")]


@pipeline(
    name=DIM_PATIENT_TABLE_NAME,
    model=DimPatient,
    level="silver",
    storage="postgres",
    comment=(
        "Silver patient dimension — one row per patient, deduplicated and "
        "cleansed from bronze.smith_users. Joined to by every fact_* table."
    ),
)
def dim_patient(inputs: DimPatientInputs) -> DataFrame:
    return compute(inputs)
