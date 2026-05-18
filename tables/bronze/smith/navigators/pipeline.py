"""Wiring for analytics.bronze.smith_navigators.

Reads smith_navigators collection from MongoDB (mongo_smith.navigators).
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, MongoSource, pipeline
from tables.bronze.smith.navigators.config import (
    SMITH_NAVIGATORS_BRONZE_TABLE_NAME,
    SmithNavigatorBronze,
)
from tables.bronze.smith.navigators.transform import compute


class SmithNavigatorsInputs(Inputs):
    """Single MongoDB upstream: smith.navigators."""

    upstream: Annotated[
        DataFrame,
        MongoSource(
            db="smith",
            collection="navigators",
            schema=SmithNavigatorBronze.to_struct(),
        ),
    ]


@pipeline(
    name=SMITH_NAVIGATORS_BRONZE_TABLE_NAME,
    model=SmithNavigatorBronze,
    level="bronze",
    storage="postgres",
    comment=(
        "Mirror of poorbricks_dev.master.navigators — navigator identity "
        "master sourced from mongo_smith.navigators. Lands in "
        "analytics.bronze.smith_navigators."
    ),
)
def smith_navigators_bronze(inputs: SmithNavigatorsInputs) -> DataFrame:
    return compute(inputs)
