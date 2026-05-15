"""Push a pipeline's full contract (schema, expectations, inputs, fixtures, sample data, profile) to MongoDB.

Usage:
    poetry run python scripts/push_contract.py --pipeline smith.users
    poetry run python scripts/push_contract.py --pipeline smith.users --mode production
    poetry run python scripts/push_contract.py --all

The contract document is the single source of truth for the Streamlit
explorer (`streamlit_app/app.py`). Everything the UI renders — fields,
expectations, inputs, fixtures, sample data — is read from this document,
so the app does not need to import any pipeline code to *browse* contracts.
It only imports pipeline code when the user clicks "Run" in the test runner.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# Ensure the project root is in sys.path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from poorbricks import list_scenarios
from poorbricks.discovery import discover_all_pipelines
from poorbricks.inputs import (
    ContractSource,
    MongoSource,
    PostgresTableSource,
    TableSource,
)
from poorbricks.registry import PipelineMeta, all_pipelines
from poorbricks.runner import run
from utils.contracts import profile_dataframe, push_contract
from validation.expectations import Expectations, find_expectations_class


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


def _serialize_rules(model_cls: type) -> list[dict[str, Any]]:
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


def _serialize_expectations(
    expectations_cls: type[Expectations] | None,
) -> dict[str, Any]:
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
            col: list(values)
            for col, values in expectations_cls.ENUM_VALUES.items()
        },
        "fresh_column": expectations_cls.FRESH_COLUMN,
        "fresh_max_age_days": expectations_cls.FRESH_MAX_AGE_DAYS,
    }


def _serialize_inputs(inputs_cls: type) -> list[dict[str, Any]]:
    """Render each declared input as a serializable dict."""
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
    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")
    scenarios = list_scenarios(pipeline_key)
    fixtures: list[dict[str, Any]] = []
    source_names = list(meta.inputs_cls.sources().keys())
    for scenario_name, scenario_fn in scenarios.items():
        try:
            inputs = scenario_fn()
        except Exception as exc:
            print(
                f"  [skip] scenario {scenario_name!r}: {type(exc).__name__}: {exc}"
            )
            continue
        rows_by_source: dict[str, list[dict[str, Any]]] = {}
        for src in source_names:
            df = getattr(inputs, src, None)
            if df is None:
                rows_by_source[src] = []
                continue
            try:
                collected = df.limit(50).collect()
            except Exception as exc:
                print(
                    f"  [skip] scenario {scenario_name!r} source {src!r}: "
                    f"{type(exc).__name__}: {exc}"
                )
                rows_by_source[src] = []
                continue
            rows_by_source[src] = [r.asDict(recursive=True) for r in collected]
        fixtures.append(
            {"scenario": scenario_name, "rows_by_source": rows_by_source}
        )
    return fixtures


def _push_one(meta: PipelineMeta, profile_mode: str) -> None:
    """Build and push a complete contract for one pipeline."""
    table_name = meta.table_name
    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")
    registry_key = f"{meta.target_storage}:{table_name}"

    fixtures_result = run(pipeline_key, mode="fixtures")
    if fixtures_result.df is None:
        raise ValueError(
            f"Pipeline {table_name!r} returned no DataFrame in fixtures mode"
        )
    example_rows = [
        r.asDict(recursive=True) for r in fixtures_result.df.limit(5).collect()
    ]

    profile_result = run(pipeline_key, mode=profile_mode)
    if profile_result.df is None:
        raise ValueError(
            f"Pipeline {table_name!r} returned no DataFrame in {profile_mode} mode"
        )
    profile = profile_dataframe(profile_result.df)

    schema = meta.model.to_struct()
    schema_json = schema.jsonValue()

    push_contract(
        table_name=table_name,
        schema=schema,
        example_rows=example_rows,
        pipeline_key=registry_key,
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
    print(
        f"Contract pushed for {table_name!r}: {profile['row_count']} rows, "
        f"{len(example_rows)} example rows, {len(profile['enum_samples'])} enum fields"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push a pipeline's contract to MongoDB"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--pipeline",
        help="Logical table name (e.g., smith.users)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Push contracts for every registered pipeline",
    )
    parser.add_argument(
        "--mode",
        default="fixtures",
        choices=["fixtures", "production"],
        help="Data source for profiling (fixtures=controlled test data, production=real data)",
    )
    args = parser.parse_args()

    discover_all_pipelines()
    pipelines = all_pipelines()

    if args.all:
        targets = list(pipelines.values())
        if not targets:
            raise ValueError("No pipelines registered")
        failures: list[tuple[str, str]] = []
        for meta in targets:
            try:
                _push_one(meta, profile_mode=args.mode)
            except Exception as exc:
                failures.append((meta.table_name, f"{type(exc).__name__}: {exc}"))
                print(f"  [error] {meta.table_name}: {exc}")
        if failures:
            print(f"\n{len(failures)} pipelines failed to push:")
            for name, err in failures:
                print(f"  - {name}: {err}")
        return

    meta = next(
        (m for m in pipelines.values() if m.table_name == args.pipeline),
        None,
    )
    if meta is None:
        # Fall back to the legacy "last dotted segment" lookup for compatibility.
        legacy_name = args.pipeline.split(".")[-1]
        meta = next(
            (m for m in pipelines.values() if m.table_name == legacy_name),
            None,
        )
    if meta is None:
        raise ValueError(
            f"No pipeline found with table_name {args.pipeline!r}. "
            f"Known pipelines: {[m.table_name for m in pipelines.values()]}"
        )
    _push_one(meta, profile_mode=args.mode)


if __name__ == "__main__":
    main()
