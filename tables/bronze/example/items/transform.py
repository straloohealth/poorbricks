"""Transform: example.items → bronze.example_items.

Pass-through: the upstream document already matches the Item schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.bronze.example.items.config import Item
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.bronze.example.items.pipeline import ItemInputs


def compute(inputs: ItemInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, Item.to_struct())
