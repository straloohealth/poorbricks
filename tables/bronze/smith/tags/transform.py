"""Transform: pass through mongo_smith.tags into the
analytics.bronze.smith_tags contract. Bronze is shape-only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.bronze.smith.tags.config import SmithTagBronze
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.bronze.smith.tags.pipeline import SmithTagsInputs


def compute(inputs: SmithTagsInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, SmithTagBronze.to_struct())
