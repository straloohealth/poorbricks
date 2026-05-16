"""Pipeline framework: declarative inputs, multi-mode verification.

Public API:
    @pipeline(name=..., model=..., level=..., comment=...,
              storage="delta" | "postgres")
        — registers a pipeline with schema validation. Default storage is
          "delta" (Spark memory, test/fixture mode); pass storage="postgres"
          to materialize via PostgresLoader into analytics.<level>.<name>.
          Pipelines can mix storage modes independently.
    Inputs — base class for typed upstream declarations
    TableSource(table_name, model) — declare a registered upstream table
    PostgresTableSource(schema, table) — declare a Postgres upstream
    MongoSource(db, collection, schema) — declare a MongoDB upstream
    @scenario(name) — register a fixture scenario
    list_scenarios(pipeline_name) — discover scenarios for a pipeline
    list_pipelines() — discover registered pipelines
    check_architecture(tables_root) — portable architecture compliance check
"""

from .arch import ArchError, check_architecture
from .decorator import pipeline
from .inputs import (
    ContractSource,
    Inputs,
    MongoSource,
    PostgresTableSource,
    TableSource,
)
from .persist import run_and_persist
from .registry import (
    get_pipeline,
    list_pipelines,
    list_scenarios,
    scenario,
)

__all__ = [
    "ArchError",
    "ContractSource",
    "Inputs",
    "MongoSource",
    "PostgresTableSource",
    "TableSource",
    "check_architecture",
    "get_pipeline",
    "list_pipelines",
    "list_scenarios",
    "pipeline",
    "run_and_persist",
    "scenario",
]
