"""Metatron Framework — public API.

Re-exports the main components for use in table-repo and other consuming projects.
"""

from __future__ import annotations

from framework import (
    Inputs,
    MongoSource,
    PostgresTableSource,
    TableSource,
    pipeline,
)
from framework.discovery import discover_all_pipelines
from framework.inputs import ContractSource
from framework.registry import get_pipeline, list_pipelines, list_scenarios, scenario
from framework.runner import RunResult, run
from framework.snapshot import diff_against_snapshot, write_snapshot
from validation import (
    Expectations,
    ValidatedStruct,
    mock_model,
    verify_with_model,
)

__all__ = [
    "ContractSource",
    "Expectations",
    "Inputs",
    "MongoSource",
    "PostgresTableSource",
    "RunResult",
    "TableSource",
    "ValidatedStruct",
    "diff_against_snapshot",
    "discover_all_pipelines",
    "get_pipeline",
    "list_pipelines",
    "list_scenarios",
    "mock_model",
    "pipeline",
    "run",
    "scenario",
    "verify_with_model",
    "write_snapshot",
]
