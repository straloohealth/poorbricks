"""Persistence layer: compute + write to Postgres + push contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .inputs import Inputs
from .registry import PipelineMeta, all_pipelines
from .runner import RunResult, run

if TYPE_CHECKING:
    from validation import ValidatedStruct


def _pg_table_name(table_name: str) -> str:
    """Map logical table name to a PostgreSQL-safe table name (no dots)."""
    return table_name.replace(".", "_")


def _flatten_fields(schema_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull out `[{name, type, nullable}]` from a StructType.jsonValue() dict."""
    fields: list[dict[str, Any]] = []
    for field in schema_json.get("fields", []):
        spark_type = field.get("type")
        if isinstance(spark_type, dict):
            type_label = spark_type.get("type", "struct")
        else:
            type_label = str(spark_type)
        fields.append(
            {
                "name": field["name"],
                "type": type_label,
                "nullable": field.get("nullable", True),
            }
        )
    return fields


def _serialize_rules(model_cls: type[ValidatedStruct]) -> list[dict[str, Any]]:
    """Serialize a model's per-row validation rules to plain dicts."""
    rules: list[dict[str, Any]] = []
    for rule in model_cls.rules():
        payload: dict[str, Any] = {"rule": type(rule).__name__}
        for attr in ("column", "description", "min_length", "max_length"):
            value = getattr(rule, attr, None)
            if value is not None:
                payload[attr] = value
        rules.append(payload)
    return rules


def _serialize_expectations(expectations_cls: type[Any] | None) -> dict[str, Any]:
    """Dump the class attributes of an Expectations subclass."""
    if expectations_cls is None:
        return {}
    return {
        "class_name": expectations_cls.__name__,
        "min_rows": expectations_cls.MIN_ROWS,
        "unique_keys": expectations_cls.UNIQUE_KEYS,
        "non_null_columns": expectations_cls.NON_NULL_COLUMNS,
        "null_rate_max": expectations_cls.NULL_RATE_MAX,
        "enum_values": {
            col: list(values) for col, values in expectations_cls.ENUM_VALUES.items()
        },
        "fresh_column": expectations_cls.FRESH_COLUMN,
        "fresh_max_age_days": expectations_cls.FRESH_MAX_AGE_DAYS,
    }


def _serialize_inputs(inputs_cls: type[Inputs]) -> list[dict[str, Any]]:
    """Render each declared input as a serializable dict."""
    from .inputs import ContractSource, MongoSource, PostgresTableSource, TableSource

    serialized: list[dict[str, Any]] = []
    for attr_name, spec in inputs_cls.sources().items():
        entry: dict[str, Any] = {
            "name": attr_name,
            "kind": type(spec).__name__,
        }
        if isinstance(spec, TableSource):
            entry["table_name"] = spec.table_name
            entry["model"] = spec.model.__name__
            entry["schema"] = spec.model.to_struct().jsonValue()
        elif isinstance(spec, MongoSource):
            entry["db"] = spec.db
            entry["collection"] = spec.collection
            entry["schema"] = spec.schema.jsonValue()
        elif isinstance(spec, ContractSource):
            entry["table_name"] = spec.table_name
        elif isinstance(spec, PostgresTableSource):
            entry["schema_name"] = spec.schema
            entry["table"] = spec.table
        serialized.append(entry)
    return serialized


def _serialize_fixtures(meta: PipelineMeta) -> list[dict[str, Any]]:
    """For each scenario, capture per-source input rows (up to 50)."""
    from .registry import list_scenarios

    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")
    scenarios = list_scenarios(pipeline_key)
    fixtures: list[dict[str, Any]] = []
    source_names = list(meta.inputs_cls.sources().keys())
    for scenario_name, scenario_fn in scenarios.items():
        try:
            inputs = scenario_fn()
        except Exception:
            continue
        rows_by_source: dict[str, list[dict[str, Any]]] = {}
        for src in source_names:
            df = getattr(inputs, src, None)
            if df is None:
                rows_by_source[src] = []
                continue
            try:
                collected = df.limit(50).collect()
            except Exception:
                rows_by_source[src] = []
                continue
            rows_by_source[src] = [r.asDict(recursive=True) for r in collected]
        fixtures.append({"scenario": scenario_name, "rows_by_source": rows_by_source})
    return fixtures


def run_and_persist(
    pipeline_key: str,
    mode: str = "fixtures",
    scenario_name: str | None = None,
) -> RunResult:
    """Compute pipeline, write to Postgres if storage='postgres', push contract to MongoDB.

    Arch and contract-source checks are enforced by run() — not repeated here.
    """
    result = run(pipeline_key, mode, scenario_name)
    if result.df is None:
        return result

    meta = all_pipelines().get(f"{pipeline_key}") or all_pipelines().get(
        f"postgres:{pipeline_key.split(':')[-1]}"
    )
    if meta is None:
        # Fallback: try to find by table_name
        for m in all_pipelines().values():
            if m.table_name == pipeline_key or m.module.endswith(pipeline_key):
                meta = m
                break
    if meta is None:
        raise ValueError(f"Pipeline metadata not found for {pipeline_key!r}")

    # Write to Postgres if applicable
    if meta.target_storage == "postgres":
        from utils.postgres import PostgresLoader

        loader = PostgresLoader()
        rows = loader.write(result.df, meta.level, _pg_table_name(meta.table_name))
        result.rows = rows

    # Get example rows
    if mode == "fixtures":
        example_rows = [r.asDict(recursive=True) for r in result.df.limit(5).collect()]
    else:
        fixtures_result = run(pipeline_key, mode="fixtures", scenario_name=None)
        if fixtures_result.df is not None:
            example_rows = [
                r.asDict(recursive=True) for r in fixtures_result.df.limit(5).collect()
            ]
        else:
            example_rows = []

    # Profile and push contract
    from utils.contracts import profile_dataframe, push_contract
    from validation.expectations import find_expectations_class

    profile = profile_dataframe(result.df)
    schema = meta.model.to_struct()  # type: ignore[attr-defined]
    schema_json = schema.jsonValue()

    push_contract(
        table_name=meta.table_name,
        schema=schema,
        example_rows=example_rows,
        pipeline_key=f"{meta.target_storage}:{meta.table_name}",
        level=meta.level,
        profile=profile,
        storage=meta.target_storage,
        comment=meta.comment,
        module=meta.module,
        fields=_flatten_fields(schema_json),
        validation_rules=_serialize_rules(meta.model),
        expectations=_serialize_expectations(find_expectations_class(pipeline_key)),
        inputs=_serialize_inputs(meta.inputs_cls),
        fixtures=_serialize_fixtures(meta),
    )

    return result


__all__ = ["run_and_persist"]
