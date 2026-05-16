"""Pass-through transform — never executes; verify --mode local fails first."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from tables.schema_drift.config import SchemaDrift
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.schema_drift.pipeline import SchemaDriftInputs


def compute(inputs: SchemaDriftInputs) -> DataFrame:
    projected = inputs.smith_users.select(f.col("mongo_id").alias("out"))
    return create_dataframe(projected, SchemaDrift.to_struct())
