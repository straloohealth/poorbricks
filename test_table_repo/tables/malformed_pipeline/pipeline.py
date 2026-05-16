"""Intentionally malformed pipeline — missing fixtures.py, transform.py, test_pipeline.py."""

from __future__ import annotations

from pyspark.sql import DataFrame

from poorbricks import Inputs, pipeline
from tables.malformed_pipeline.config import MalformedModel


class MalformedInputs(Inputs):
    pass


@pipeline(
    name="malformed_pipeline",
    model=MalformedModel,
    level="silver",
    storage="postgres",
    comment="Intentionally malformed — used to verify arch check catches missing files.",
)
def malformed_pipeline(inputs: MalformedInputs) -> DataFrame:
    raise NotImplementedError("This pipeline is intentionally incomplete.")
