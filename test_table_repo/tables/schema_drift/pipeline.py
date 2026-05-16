"""Pipeline declaring a TableSource with a local model that drifts from published schema."""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, TableSource, pipeline
from tables.schema_drift.config import LocalSmithUsers, SchemaDrift
from tables.schema_drift.transform import compute


class SchemaDriftInputs(Inputs):
    smith_users: Annotated[DataFrame, TableSource("smith.users", LocalSmithUsers)]


@pipeline(
    name="schema_drift",
    model=SchemaDrift,
    level="silver",
    storage="postgres",
    comment="Test scenario — local model has a field not in the published contract.",
)
def schema_drift(inputs: SchemaDriftInputs) -> DataFrame:
    return compute(inputs)
