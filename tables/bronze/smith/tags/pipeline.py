"""Wiring for analytics.bronze.smith_tags.

Reads directly from MongoDB (``smith.tags``). Bronze is shape-only;
silver builds dim_tag on top.
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, MongoSource, pipeline
from tables.bronze.smith.tags.config import (
    SMITH_TAGS_BRONZE_TABLE_NAME,
    SmithTagBronze,
)
from tables.bronze.smith.tags.transform import compute

DB_NAME = "smith"
COLLECTION_NAME = "tags"


class SmithTagsInputs(Inputs):
    """Single Mongo upstream feeding analytics.bronze.smith_tags."""

    upstream: Annotated[
        DataFrame,
        MongoSource(
            db=DB_NAME,
            collection=COLLECTION_NAME,
            schema=SmithTagBronze.to_struct(),
        ),
    ]


@pipeline(
    name=SMITH_TAGS_BRONZE_TABLE_NAME,
    model=SmithTagBronze,
    level="bronze",
    comment=(
        "Mirror of mongo_smith.tags — patient-level tag assignments "
        "from the Smith user-store. Lands in analytics.bronze.smith_tags "
        "so silver dim_tag can derive the canonical tag lookup."
    ),
)
def smith_tags_bronze(inputs: SmithTagsInputs) -> DataFrame:
    return compute(inputs)
