"""Identity transform — output has same rows as upstream."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.expectations_failure.config import ExpectationsFailure
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.expectations_failure.pipeline import ExpectationsFailureInputs


def compute(inputs: ExpectationsFailureInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, ExpectationsFailure.to_struct())
