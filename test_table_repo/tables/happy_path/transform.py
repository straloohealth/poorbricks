"""Project user_id + is_active from smith.users."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from tables.happy_path.config import HappyPath
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.happy_path.pipeline import HappyPathInputs


def compute(inputs: HappyPathInputs) -> DataFrame:
    projected = inputs.smith_users.select(
        f.col("mongo_id").alias("user_id"),
        f.coalesce(f.col("active"), f.lit(False)).alias("is_active"),
    )
    return create_dataframe(projected, HappyPath.to_struct())
