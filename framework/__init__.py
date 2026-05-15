"""Pipeline framework: declarative inputs, multi-mode verification.

Public API:
    @pipeline(name=..., model=..., level=..., comment=...,
              storage="delta" | "postgres")
        — replaces @dlt.table + @verify_with_model. Default storage is
          "delta"; pass storage="postgres" to materialize via
          PostgresLoader into analytics.<level>.<name> instead of a
          Delta table. Pipelines can stay in either mode independently.
    Inputs — base class for typed upstream declarations
    TableSource(table_name, model) — declare a Delta-table upstream
    PostgresTableSource(schema, table, model) — declare a Postgres upstream
    MongoSource(db, collection, schema) — declare a MongoDB upstream
    @scenario(name) — register a fixture scenario
    list_scenarios(pipeline_name) — discover scenarios for a pipeline
    list_pipelines() — discover registered pipelines

Read /home/danielspeixoto/.claude/plans/etl-jobs-are-overly-delightful-valiant.md
for the original Delta design; the medallion-Postgres extension is in
/home/danielspeixoto/.claude/plans/create-a-web-ui-proud-sunrise.md.
"""

from .decorator import pipeline
from .inputs import (
    ContractSource,
    Inputs,
    MongoSource,
    PostgresTableSource,
    TableSource,
)
from .registry import (
    get_pipeline,
    list_pipelines,
    list_scenarios,
    scenario,
)

__all__ = [
    "ContractSource",
    "Inputs",
    "MongoSource",
    "PostgresTableSource",
    "TableSource",
    "get_pipeline",
    "list_pipelines",
    "list_scenarios",
    "pipeline",
    "scenario",
]
