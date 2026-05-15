"""Pipeline wiring for silver.dim_navigator.

Reads smith_navigators schema from MongoDB contracts store.
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import ContractSource, Inputs, pipeline
from tables.silver.dim_navigator.config import (
    DIM_NAVIGATOR_TABLE_NAME,
    DimNavigator,
)
from tables.silver.dim_navigator.transform import compute


class DimNavigatorInputs(Inputs):
    """Single upstream: smith_navigators fetched from contracts store."""

    smith_navigators: Annotated[DataFrame, ContractSource("smith.navigators")]


@pipeline(
    name=DIM_NAVIGATOR_TABLE_NAME,
    model=DimNavigator,
    level="silver",
    storage="postgres",
    comment=(
        "Silver navigator dimension — one row per care-team member, "
        "deduplicated and cleansed from bronze.smith_navigators."
    ),
)
def dim_navigator(inputs: DimNavigatorInputs) -> DataFrame:
    return compute(inputs)
