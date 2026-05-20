"""Wiring for bronze.example_items.

Reads the 'items' collection from the example MongoDB database.
Used to exercise the framework's pipeline mechanics in integration tests.
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, MongoSource, pipeline
from tables.bronze.example.items.config import (
    EXAMPLE_ITEMS_TABLE_NAME,
    Item,
)
from tables.bronze.example.items.transform import compute


class ItemInputs(Inputs):
    """Single MongoDB upstream: example.items."""

    upstream: Annotated[
        DataFrame,
        MongoSource(
            db="example",
            collection="items",
            schema=Item.to_struct(),
        ),
    ]


@pipeline(
    name=EXAMPLE_ITEMS_TABLE_NAME,
    model=Item,
    level="bronze",
    storage="postgres",
    comment="Example items table used by the framework's own integration tests.",
)
def example_items_bronze(inputs: ItemInputs) -> DataFrame:
    return compute(inputs)
