"""Pass-through transform for analytics.bronze.smith_users.

Renames MongoDB's _id field to mongo_id for consistency with schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.bronze.smith.users.config import SmithUserBronze
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.bronze.smith.users.pipeline import SmithUsersInputs


def compute(inputs: SmithUsersInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, SmithUserBronze.to_struct())
