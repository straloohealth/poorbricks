"""Transform: silver.dim_patient -> gold.patients.

Projects the silver patient dimension into the column set overseer
consumes. ``create_dataframe`` realigns to ``Patients.to_struct()`` so
schema drift in the upstream surfaces here as a validation error
instead of a silent shape mismatch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from tables.gold.patients.config import PatientGold
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.gold.patients.pipeline import PatientsGoldInputs


def compute(inputs: PatientsGoldInputs) -> DataFrame:
    projected = inputs.dim_patient.select(
        f.col("patient_id"),
        f.col("name"),
        f.col("email"),
        f.col("phone"),
        f.col("birth_date"),
        f.col("origin_slug"),
        f.col("is_active"),
        f.col("is_deleted"),
        f.col("is_high_risk"),
        f.col("is_surgical"),
        f.col("created_at"),
    )
    return create_dataframe(projected, PatientGold.to_struct())
