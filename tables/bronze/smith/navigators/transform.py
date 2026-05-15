"""Pass-through transform for analytics.bronze.smith_navigators."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.bronze.smith.navigators.config import SmithNavigatorBronze
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.bronze.smith.navigators.pipeline import (
        SmithNavigatorsInputs,
    )


def compute(inputs: SmithNavigatorsInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, SmithNavigatorBronze.to_struct())
