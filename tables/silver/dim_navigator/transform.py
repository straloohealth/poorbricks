"""Transform: bronze.smith_navigators → silver.dim_navigator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

from tables.silver.dim_navigator.config import DimNavigator
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.silver.dim_navigator.pipeline import DimNavigatorInputs


def compute(inputs: DimNavigatorInputs) -> DataFrame:
    navs = inputs.smith_navigators

    # Filter out rows with null navigator_id — they have no valid key.
    with_key = navs.filter(f.col("navigator_id").isNotNull())

    cleaned = with_key.select(
        f.trim(f.col("navigator_id").cast("string")).alias("navigator_id"),
        f.when(f.col("name").isNotNull(), f.trim(f.col("name").cast("string")))
        .otherwise(f.lit(None).cast("string"))
        .alias("name"),
        f.when(f.col("role").isNotNull(), f.trim(f.col("role").cast("string")))
        .otherwise(f.lit(None).cast("string"))
        .alias("role"),
        f.coalesce(f.col("is_active"), f.lit(False)).alias("is_active"),
        f.col("started_at"),
    )

    # Defensive dedup: keep the row with the earliest started_at (most
    # authoritative join date).
    window = Window.partitionBy("navigator_id").orderBy(
        f.col("started_at").asc_nulls_last()
    )
    deduped = (
        cleaned.withColumn("_rn", f.row_number().over(window))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
    )

    return create_dataframe(deduped, DimNavigator.to_struct())
