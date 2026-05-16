"""Happy-path pipeline — declares a ContractSource that resolves cleanly."""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import ContractSource, Inputs, pipeline
from tables.happy_path.config import HappyPath
from tables.happy_path.transform import compute


class HappyPathInputs(Inputs):
    smith_users: Annotated[DataFrame, ContractSource("smith.users")]


@pipeline(
    name="happy_path",
    model=HappyPath,
    level="silver",
    storage="postgres",
    comment="Test scenario — contract present, schema matches.",
)
def happy_path(inputs: HappyPathInputs) -> DataFrame:
    return compute(inputs)
