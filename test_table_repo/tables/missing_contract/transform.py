"""Pass-through transform — never executes; verify --mode local fails first."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.missing_contract.config import MissingContract
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.missing_contract.pipeline import MissingContractInputs


def compute(inputs: MissingContractInputs) -> DataFrame:
    return create_dataframe(inputs.ghost, MissingContract.to_struct())
