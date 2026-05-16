"""Project silver.dim_patient into the gold patient column set."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from tables.gold_patients.config import PatientGold
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.gold_patients.pipeline import GoldPatientsInputs


def compute(inputs: GoldPatientsInputs) -> DataFrame:
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
