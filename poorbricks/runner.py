"""Local runner: build an Inputs instance for a pipeline and call its transform.

Modes:
    fixtures     — union of all @scenario functions from fixtures.py
    scenario     — single named @scenario function
    production   — all upstreams resolved from MongoDB or upstream pipelines
    fault        — fixtures + a named fault injected into inputs

Public API:
    run(pipeline_key, mode, scenario_name=None, fault_name=None) -> RunResult

A ``RunResult`` exposes ``.df`` (output DataFrame) and ``.errors`` (list of
human-readable issues — validation failures, missing contracts, etc.).
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast

from .faults import apply_fault
from .inputs import (
    ContractSource,
    Inputs,
    MongoSource,
    PostgresTableSource,
    TableSource,
)
from .registry import (
    PipelineMeta,
    all_pipelines,
    get_pipeline,
    list_scenarios,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class RunResult:
    df: DataFrame | None
    rows: int | None = None
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Discovery / resolution
# ---------------------------------------------------------------------------


def _import_pipeline_module(pipeline_key: str) -> None:
    """Import the pipeline + its fixtures so registry decorators run.

    Registry-key form (``"<storage>:<table>"``) is skipped: the colon means
    the module was already discovered (otherwise the registry would not
    have the entry) and is not a valid dotted import path.
    """
    if ":" in pipeline_key:
        return
    base = f"tables.{pipeline_key}"
    importlib.import_module(f"{base}.pipeline")
    try:
        importlib.import_module(f"{base}.fixtures")
    except ImportError:
        pass


def _resolve_meta(pipeline_key: str) -> PipelineMeta:
    """Find the registered pipeline whose module matches the given dotted key."""
    target_module = f"tables.{pipeline_key}.pipeline"
    pipelines = all_pipelines()
    for meta in pipelines.values():
        if meta.module == target_module:
            return meta
    try:
        return get_pipeline(pipeline_key)
    except KeyError:
        registered_modules = {meta.module for meta in pipelines.values()}
        raise KeyError(
            f"Pipeline {pipeline_key!r} not found. Looked for module "
            f"{target_module!r}. Registered modules: {sorted(registered_modules)}"
        ) from None


# ---------------------------------------------------------------------------
# Fixture-based input construction
# ---------------------------------------------------------------------------


def _merge_scenarios(scenarios: Iterable[Inputs], inputs_cls: type[Inputs]) -> Inputs:
    """Union the rows from multiple scenario instances into one Inputs."""
    scenarios_list = list(scenarios)
    if not scenarios_list:
        raise ValueError("No scenarios to merge.")
    if len(scenarios_list) == 1:
        return scenarios_list[0]

    merged: dict[str, DataFrame] = {}
    for source_name in inputs_cls.sources():
        first = getattr(scenarios_list[0], source_name)
        df = first
        for scenario_inputs in scenarios_list[1:]:
            df = df.unionByName(getattr(scenario_inputs, source_name))
        merged[source_name] = df
    return inputs_cls.from_dataframes(merged)


def _build_fixtures_inputs(
    pipeline_key: str, inputs_cls: type[Inputs], scenario_name: str | None
) -> Inputs:
    scenarios = list_scenarios(pipeline_key)
    if not scenarios:
        raise ValueError(
            f"No scenarios registered for pipeline {pipeline_key!r}. "
            f"Add a fixtures.py with @scenario(...) functions."
        )
    if scenario_name is not None:
        if scenario_name not in scenarios:
            raise ValueError(
                f"Scenario {scenario_name!r} not found for {pipeline_key!r}. "
                f"Known: {sorted(scenarios)}"
            )
        return scenarios[scenario_name]()
    return _merge_scenarios((fn() for fn in scenarios.values()), inputs_cls)


# ---------------------------------------------------------------------------
# Production-source resolution
# ---------------------------------------------------------------------------


def _resolve_production_input(
    spark: SparkSession,
    spec: TableSource | MongoSource | ContractSource | PostgresTableSource,
    mongo_uri: str | None,
    cache: dict[str, DataFrame] | None = None,
) -> DataFrame:
    cache = cache or {}
    if isinstance(spec, TableSource):
        df = spark.read.table(spec.table_name)
        try:
            model_field_names = {f.name for f in spec.model.to_struct().fields}
        except Exception:
            model_field_names = set()
        for col in df.columns:
            if (
                col.startswith("_")
                and not col.startswith("__")
                and col[1:] in model_field_names
            ):
                df = df.withColumnRenamed(col, col[1:])
        return df
    if isinstance(spec, MongoSource):
        if mongo_uri is None:
            raise ValueError(
                f"MongoSource for collection {spec.db}.{spec.collection} requires "
                f"MONGO_URI; not set in environment / .env."
            )
        from utils.mongo import get_all

        return get_all(mongo_uri, spec.db, spec.collection, schema=spec.schema)
    if isinstance(spec, ContractSource):
        if spec.table_name not in cache:
            cache[spec.table_name] = _read_contract_source(spark, spec.table_name)
        return cache[spec.table_name]
    raise ValueError(
        f"PostgresTableSource {spec.table} is not supported in production mode; "
        f"it is only for legacy gold passthroughs."
    )


def _find_pipeline_by_table(table_name: str) -> PipelineMeta:
    """Find a pipeline by its table_name (e.g. 'smith.users')."""
    for meta in all_pipelines().values():
        if meta.table_name == table_name:
            return meta
    raise ValueError(f"No pipeline found for table {table_name!r}.")


def _read_contract_source(spark: SparkSession, table_name: str) -> DataFrame:
    """Resolve a ``ContractSource`` by reading the published contract + data
    rather than re-running the upstream pipeline.

    This is the primitive that makes cross-repo workflows possible: the
    consumer only needs the contract (schema + storage pointer) in MongoDB
    and access to the upstream's storage backend. The producer's source
    code does not need to be present.
    """
    from utils.contracts import fetch_contract

    from .settings import settings

    contract = fetch_contract(table_name)
    if contract is None:
        raise ValueError(
            f"ContractSource {table_name!r}: no contract published in "
            f"{settings.contracts_db}.{settings.contracts_collection}. "
            f"Run the upstream pipeline first."
        )
    storage = contract.get("storage")
    level = contract.get("level")
    if storage != "postgres":
        raise NotImplementedError(
            f"ContractSource {table_name!r} has storage={storage!r}; only "
            f"'postgres' is supported as a cross-repo read source."
        )
    pg_table = table_name.replace(".", "_")
    jdbc_url = (
        f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.postgres_db}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f'"{level}"."{pg_table}"')
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def _build_production_inputs(
    spark: SparkSession,
    inputs_cls: type[Inputs],
    mongo_uri: str | None = None,
    cache: dict[str, DataFrame] | None = None,
) -> Inputs:
    """Resolve every declared input from its real source."""
    cache = cache or {}
    dataframes: dict[str, DataFrame] = {}
    for name, spec in inputs_cls.sources().items():
        dataframes[name] = _resolve_production_input(spark, spec, mongo_uri, cache)
    return inputs_cls.from_dataframes(dataframes)


def _ensure_local_spark() -> SparkSession:
    """Build a local PySpark session."""
    from utils.spark_local import build_local_spark

    return build_local_spark()


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv as _load
    except ImportError:
        return
    _load()


# ---------------------------------------------------------------------------
# Architecture and contract validation
# ---------------------------------------------------------------------------


def _pipeline_dir_from_meta(meta: PipelineMeta) -> Path:
    """Safely derive pipeline directory from module path using importlib."""
    spec = importlib.util.find_spec(meta.module)
    if spec is None or spec.origin is None:
        raise RuntimeError(f"Cannot locate module {meta.module!r} on disk")
    return Path(spec.origin).parent


def _check_arch(meta: PipelineMeta) -> list[str]:
    """Run architecture checks on a pipeline's directory."""
    from .arch import check_pipeline_dir

    errors = check_pipeline_dir(_pipeline_dir_from_meta(meta))
    return [e.format() for e in errors]


def _validate_contract_sources(inputs_cls: type[Inputs]) -> list[str]:
    """Verify all ContractSource upstreams exist in MongoDB."""
    from utils.contracts import fetch_contract

    errors: list[str] = []
    for field_name, spec in inputs_cls.sources().items():
        if isinstance(spec, ContractSource):
            if fetch_contract(spec.table_name) is None:
                errors.append(
                    f"ContractSource '{spec.table_name}' (field '{field_name}') "
                    f"not found in MongoDB — run upstream pipeline first"
                )
    return errors


def _execute_pipeline(meta: PipelineMeta, inputs: Inputs) -> RunResult:
    """Compute and verify a pipeline's output against its schema."""
    df = cast("DataFrame", meta.original_fn(inputs))
    errors: list[str] = []
    try:
        meta.model.verify(df, strict=True)  # type: ignore[attr-defined]
    except Exception as exc:
        errors.append(str(exc))
    return RunResult(df=df, rows=df.count(), errors=errors)


# ---------------------------------------------------------------------------
# Raw pipeline execution (internal)
# ---------------------------------------------------------------------------


def _run_raw(
    meta: PipelineMeta,
    mode: str,
    spark: SparkSession,
    mongo_uri: str | None,
    cache: dict[str, DataFrame],
) -> RunResult:
    """Internal: execute a pipeline given PipelineMeta + SparkSession."""
    inputs_cls = meta.inputs_cls

    if mode == "fixtures":
        inputs = _build_fixtures_inputs(meta.table_name, inputs_cls, scenario_name=None)
    elif mode == "production":
        inputs = _build_production_inputs(
            spark, inputs_cls, mongo_uri=mongo_uri, cache=cache
        )
    else:
        raise ValueError(f"Internal _run_raw does not support mode {mode!r}.")

    return _execute_pipeline(meta, inputs)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    pipeline_key: str,
    mode: str = "fixtures",
    scenario_name: str | None = None,
    fault_name: str | None = None,
    skip_checks: bool = False,
) -> RunResult:
    """Run a pipeline locally and return ``RunResult``.

    Args:
        pipeline_key: pipeline table name or dotted path (e.g. ``"smith.users"`` or
            ``"silver.dim_patient"``). Can also be a registry key like
            ``"delta:smith_users"``.
        mode: one of ``"fixtures"``, ``"scenario"``, ``"production"``, ``"fault"``,
            ``"validate"``. ``"validate"`` runs checks without computing.
        scenario_name: required when ``mode="scenario"``.
        fault_name: required when ``mode="fault"``.
        skip_checks: if True, skip architecture and contract-source validation.

    Returns:
        ``RunResult(df, rows, errors)``.
    """
    valid_modes = {"fixtures", "scenario", "production", "fault", "validate"}
    if mode not in valid_modes:
        raise ValueError(f"Unknown mode {mode!r}. Valid: {sorted(valid_modes)}.")

    _load_dotenv()
    _import_pipeline_module(pipeline_key)
    meta = _resolve_meta(pipeline_key)
    inputs_cls = meta.inputs_cls

    # Run architecture and contract-source checks unless skipped
    if not skip_checks:
        arch_errors = _check_arch(meta)
        contract_errors = _validate_contract_sources(inputs_cls)
        all_errors = arch_errors + contract_errors

        # If validate-only mode, return errors without computing
        if mode == "validate":
            return RunResult(df=None, rows=None, errors=all_errors)

        # If any errors found, raise before touching Spark
        if all_errors:
            error_msg = "Pipeline checks failed:\n" + "\n".join(
                f"  {e}" for e in all_errors
            )
            raise RuntimeError(error_msg)
    elif mode == "validate":
        # skip_checks=True with mode=validate is a no-op
        return RunResult(df=None, rows=None, errors=[])

    # Build inputs based on mode
    mongo_uri = os.getenv("MONGO_URI")
    spark = _ensure_local_spark()
    cache: dict[str, DataFrame] = {}

    if mode == "fixtures":
        inputs = _build_fixtures_inputs(pipeline_key, inputs_cls, scenario_name=None)
    elif mode == "scenario":
        if scenario_name is None:
            raise ValueError("mode=scenario requires scenario_name.")
        inputs = _build_fixtures_inputs(pipeline_key, inputs_cls, scenario_name)
    elif mode == "production":
        inputs = _build_production_inputs(
            spark, inputs_cls, mongo_uri=mongo_uri, cache=cache
        )
    elif mode == "fault":
        if not fault_name:
            raise ValueError("mode=fault requires fault_name.")
        inputs = _build_fixtures_inputs(pipeline_key, inputs_cls, scenario_name=None)
        inputs = apply_fault(fault_name, inputs)
    else:
        raise AssertionError(f"unhandled mode {mode!r}")

    return _execute_pipeline(meta, inputs)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: ``poorbricks run --pipeline <key> --mode <mode>``.

    Used inside Airflow worker pods so each task is a single subprocess
    invocation. Returns the exit code; non-zero on validation errors.
    """
    import argparse

    parser = argparse.ArgumentParser(prog="poorbricks run")
    parser.add_argument(
        "--pipeline", required=True, help="pipeline key, e.g. 'silver.dim_patient'"
    )
    parser.add_argument(
        "--mode",
        default="production",
        choices=sorted({"fixtures", "scenario", "production", "fault", "validate"}),
    )
    parser.add_argument(
        "--scenario", default=None, help="required when --mode=scenario"
    )
    parser.add_argument("--fault", default=None, help="required when --mode=fault")
    args = parser.parse_args(argv)

    if args.mode == "production":
        from .persist import run_and_persist

        result = run_and_persist(
            pipeline_key=args.pipeline,
            mode=args.mode,
            scenario_name=args.scenario,
        )
    else:
        result = run(
            pipeline_key=args.pipeline,
            mode=args.mode,
            scenario_name=args.scenario,
            fault_name=args.fault,
        )
    if result.errors:
        for err in result.errors:
            print(f"✗ {err}")
        return 1
    if result.rows is not None:
        print(f"✓ {args.pipeline}: {result.rows} rows")
    return 0


__all__ = ["RunResult", "main", "run"]
