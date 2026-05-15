"""DLT/Postgres wiring for gold.patients.

Reads ``poorbricks_dev.silver.dim_patient`` and projects the columns
overseer consumes into ``analytics.gold.patients`` via the medallion
driver. Replaces the legacy ``poorbricks.patients`` Postgres mirror.
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from framework import ContractSource, Inputs, pipeline
from tables.gold.patients.config import (
    PATIENTS_TABLE_NAME,
    PatientGold,
)
from tables.gold.patients.transform import compute


class PatientsGoldInputs(Inputs):
    """Single upstream: dim_patient via contracts store."""

    dim_patient: Annotated[
        DataFrame,
        ContractSource("dim_patient"),
    ]


@pipeline(
    name=PATIENTS_TABLE_NAME,
    model=PatientGold,
    level="gold",
    storage="postgres",
    comment=(
        "Gold patient dataset — projects poorbricks_dev.silver.dim_patient "
        "into the shape overseer's PostgresQuery classes expect. Replaces "
        "the legacy poorbricks.patients Postgres mirror that passthrough'd "
        "poorbricks_dev.master.patients."
    ),
)
def patients(inputs: PatientsGoldInputs) -> DataFrame:
    return compute(inputs)
