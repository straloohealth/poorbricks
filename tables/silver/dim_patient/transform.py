"""Transform: bronze.smith_users → silver.dim_patient.

Pure function. Trims whitespace on string fields, deduplicates by
``patient_id`` (keeps the row with the latest ``created_at`` as
tie-break — Smith never re-creates a patient with the same id but a
defensive guard avoids accidental duplication when bronze is partitioned
in flight).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

from tables.silver.dim_patient.config import DimPatient
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.silver.dim_patient.pipeline import DimPatientInputs


# Column-mapping reality, May 2026:
# bronze.smith_users (actual production shape from MongoDB) carries:
#   externalId, name, email, phone, origin, active, createdAt,
#   birth_date, cpf, extraFields
# Silver dimension maps these to lowercase snake_case for consistency.
_TRIMMABLE_PRESENT_COLS = ("externalId", "name", "email", "phone", "origin")


def compute(inputs: DimPatientInputs) -> DataFrame:
    users = inputs.smith_users

    trimmed = users
    for col in _TRIMMABLE_PRESENT_COLS:
        trimmed = trimmed.withColumn(
            col,
            f.when(f.col(col).isNotNull(), f.trim(f.col(col).cast("string"))).otherwise(
                f.lit(None).cast("string")
            ),
        )

    # Defensive dedup on mongo_id (MongoDB ObjectId), the true unique key.
    # Filter out rows with null mongo_id — they have no valid key.
    with_key = trimmed.filter(f.col("mongo_id").isNotNull())
    window = Window.partitionBy("mongo_id").orderBy(
        f.col("createdAt").desc_nulls_last()
    )
    deduped = (
        with_key.withColumn("_rn", f.row_number().over(window))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    projected = deduped.select(
        f.col("mongo_id").alias("patient_id"),
        f.col("mongo_id").alias("mongo_id"),
        f.col("name"),
        f.col("email"),
        f.col("phone"),
        f.col("birth_date"),
        f.col("createdAt").alias("created_at"),
        f.col("origin").alias("origin_slug"),
        f.coalesce(f.col("active"), f.lit(False)).alias("is_active"),
        f.lit(False).alias("is_high_risk"),
        f.lit(False).alias("is_surgical"),
    )

    return create_dataframe(projected, DimPatient.to_struct())
