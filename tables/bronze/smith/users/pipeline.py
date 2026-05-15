"""Wiring for analytics.bronze.smith_users.

Reads smith_users collection from MongoDB (mongo_smith.users).
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from framework import Inputs, MongoSource, pipeline
from tables.bronze.smith.users.config import (
    SMITH_USERS_BRONZE_TABLE_NAME,
    SmithUserBronze,
)
from tables.bronze.smith.users.transform import compute


class SmithUsersInputs(Inputs):
    """Single MongoDB upstream: smith.users."""

    upstream: Annotated[
        DataFrame,
        MongoSource(
            db="smith",
            collection="users",
            schema=SmithUserBronze.to_struct(),
        ),
    ]


@pipeline(
    name=SMITH_USERS_BRONZE_TABLE_NAME,
    model=SmithUserBronze,
    level="bronze",
    comment=(
        "Mirror of poorbricks_dev.master.patients — the patient-identity "
        "master expanded from mongo_smith.users. Lands in "
        "analytics.bronze.smith_users."
    ),
)
def smith_users_bronze(inputs: SmithUsersInputs) -> DataFrame:
    return compute(inputs)
