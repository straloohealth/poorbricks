"""Pipeline that depends on a contract that doesn't exist in the store."""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import ContractSource, Inputs, pipeline
from tables.missing_contract.config import MissingContract
from tables.missing_contract.transform import compute


class MissingContractInputs(Inputs):
    ghost: Annotated[DataFrame, ContractSource("smith.nonexistent_table")]


@pipeline(
    name="missing_contract",
    model=MissingContract,
    level="silver",
    storage="postgres",
    comment="Test scenario — ContractSource references a non-existent contract.",
)
def missing_contract(inputs: MissingContractInputs) -> DataFrame:
    return compute(inputs)
