"""Pipeline that always produces too few rows to satisfy MIN_ROWS."""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, TableSource, pipeline
from tables.expectations_failure.config import ExpectationsFailure, TinyUpstream
from tables.expectations_failure.transform import compute


class ExpectationsFailureInputs(Inputs):
    upstream: Annotated[DataFrame, TableSource("tiny_upstream", TinyUpstream)]


@pipeline(
    name="expectations_failure",
    model=ExpectationsFailure,
    level="silver",
    storage="postgres",
    comment="Test scenario — output passes schema but violates MIN_ROWS.",
)
def expectations_failure(inputs: ExpectationsFailureInputs) -> DataFrame:
    return compute(inputs)
