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
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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
    # Per-phase wall-clock seconds (spark_init_s, discovery_s, inputs_s,
    # compute_s, ...) — populated by run() / run_and_persist() for measurement.
    timings: dict[str, float] = field(default_factory=dict)
    # Column-level lineage captured from the Spark analyzed plan in
    # _execute_pipeline (best-effort; None when capture is unavailable).
    lineage: dict[str, Any] | None = None


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
    """Find the registered pipeline for the given key.

    Accepts either:
    - Registry key form: ``"<storage>:<table_name>"`` (e.g. ``"postgres:dim_patient"``)
    - Dotted-path form: matches against ``meta.module`` (e.g. ``"silver.dim_patient"``)
    - Bare table name: ``"dim_patient"`` (falls back to ``get_pipeline``)
    """
    pipelines = all_pipelines()
    if ":" in pipeline_key:
        if pipeline_key in pipelines:
            return pipelines[pipeline_key]
        storage, table_name = pipeline_key.split(":", 1)
        return get_pipeline(table_name, target_storage=storage)
    target_module = f"tables.{pipeline_key}.pipeline"
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
        # A TableSource names an upstream produced by another pipeline in the
        # same repo/bundle. In production it resolves exactly like a
        # ContractSource — the materialized rows are read from the Postgres
        # warehouse — except the schema comes from the locally declared model
        # and the warehouse schema ("level") from the registered producer, so
        # a not-yet-published contract never blocks the read.
        if spec.table_name not in cache:
            producer = _find_local_producer(spec.table_name)
            if producer is not None:
                if producer.target_storage != "postgres":
                    raise ValueError(
                        f"TableSource {spec.table_name!r} resolves to a local "
                        f"producer with storage={producer.target_storage!r}; "
                        f"only 'postgres' upstreams can be read in production."
                    )
                cache[spec.table_name] = _read_postgres_table(
                    spark,
                    producer.level,
                    spec.table_name,
                    spec.model.to_struct().jsonValue(),
                )
            else:
                # Not produced in this bundle — resolve via the published
                # contract, exactly as a ContractSource would.
                cache[spec.table_name] = _read_contract_source(spark, spec.table_name)
        return cache[spec.table_name]
    if isinstance(spec, MongoSource):
        if mongo_uri is None:
            raise ValueError(
                f"MongoSource for collection {spec.db}.{spec.collection} requires "
                f"MONGO_URI; not set in environment / .env."
            )
        from utils.mongo import get_all

        # read_schema relaxes spec.nullable_columns so a null/missing source
        # value never aborts the read; the pipeline imputes them downstream.
        return get_all(mongo_uri, spec.db, spec.collection, schema=spec.read_schema)
    if isinstance(spec, ContractSource):
        if spec.table_name not in cache:
            cache[spec.table_name] = _read_contract_source(spark, spec.table_name)
        return cache[spec.table_name]
    raise ValueError(
        f"PostgresTableSource {spec.table} is not supported in production mode; "
        f"it is only for legacy gold passthroughs."
    )


def _find_local_producer(table_name: str) -> PipelineMeta | None:
    """Return the registered pipeline that produces ``table_name``, or None.

    Resolves a ``TableSource`` against a producer in the same upload bundle
    without consulting the published contracts store. Returns None when no
    in-bundle producer is registered, so the caller falls back to the contract.
    """
    for meta in all_pipelines().values():
        if meta.table_name == table_name:
            return meta
    return None


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
    if storage != "postgres":
        raise NotImplementedError(
            f"ContractSource {table_name!r} has storage={storage!r}; only "
            f"'postgres' is supported as a cross-repo read source."
        )
    return _read_postgres_table(
        spark,
        str(contract.get("level")),
        table_name,
        contract.get("schema_json"),
    )


def _read_postgres_table(
    spark: SparkSession,
    level: str,
    table_name: str,
    schema_json: dict[str, Any] | None,
) -> DataFrame:
    """Read a materialized warehouse table from Postgres over partitioned JDBC.

    Shared by ``ContractSource`` and in-bundle ``TableSource`` production
    resolution: ``level`` is the Postgres schema the producer wrote to and
    ``schema_json`` (when available) lets the read fan out across executors.
    """
    from .settings import settings

    pg_table = table_name.replace(".", "_")
    jdbc_url = (
        f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.postgres_db}"
    )
    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f'"{level}"."{pg_table}"')
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        # Server-side cursor: stream rows in batches rather than buffering the
        # entire result set in the executor JVM (the postgres JDBC default).
        .option("fetchsize", "5000")
    )
    # Partition the read across executors when the schema has a column we can
    # range-partition on — N parallel range scans instead of one serial read.
    for key, value in _jdbc_partition_options(
        schema_json, level, pg_table, settings.read_partitions
    ).items():
        reader = reader.option(key, value)
    return _parse_complex_columns(reader.load(), schema_json)


def _parse_complex_columns(
    df: DataFrame, schema_json: dict[str, Any] | None
) -> DataFrame:
    """Restore struct/array/map columns from their JSON-text Postgres storage.

    Postgres has no column type matching Spark's nested types, so the writer
    (``utils.postgres``) serialises struct/array/map columns to JSON text; a
    JDBC read returns them as plain strings. This parses them back to the
    complex types declared in ``schema_json`` so downstream transforms see
    real structs/arrays rather than strings.
    """
    if not schema_json:
        return df
    from pyspark.sql import functions as f
    from pyspark.sql.types import ArrayType, MapType, StructType

    try:
        declared = StructType.fromJson(schema_json)
    except Exception:
        return df
    for schema_field in declared.fields:
        if schema_field.name in df.columns and isinstance(
            schema_field.dataType, ArrayType | MapType | StructType
        ):
            df = df.withColumn(
                schema_field.name,
                # pyspark's from_json stub omits MapType, which it accepts.
                f.from_json(f.col(schema_field.name), schema_field.dataType),  # type: ignore[arg-type]
            )
    return df


def _jdbc_partition_options(
    schema_json: dict[str, Any] | None,
    level: str,
    pg_table: str,
    num_partitions: int,
) -> dict[str, str]:
    """Pick a partition column + bounds so a JDBC read fans out across executors.

    Prefers a timestamp/date column, then an integral one. Returns ``{}`` (a
    safe single-partition read) when no suitable column exists or the table is
    empty / single-valued.
    """
    from pyspark.sql.types import (
        DateType,
        IntegerType,
        LongType,
        StructType,
        TimestampType,
    )

    if not schema_json:
        return {}
    try:
        schema = StructType.fromJson(schema_json)
    except Exception:
        return {}

    # Match the field's actual top-level type with isinstance — never a
    # substring of str(dataType), which would also match a nested
    # struct/array column whose repr merely contains "TimestampType" and
    # make Spark's JDBC reader reject it ("partition column ... string found").
    column: str | None = None
    for schema_field in schema.fields:
        if isinstance(schema_field.dataType, TimestampType | DateType):
            column = schema_field.name
            break
    if column is None:
        for schema_field in schema.fields:
            if isinstance(schema_field.dataType, LongType | IntegerType):
                column = schema_field.name
                break
    if column is None:
        return {}

    from utils.postgres import PostgresLoader

    low, high = PostgresLoader().column_bounds(level, pg_table, column)
    if low is None or high is None or low == high:
        return {}
    return {
        "partitionColumn": column,
        "lowerBound": _format_jdbc_bound(low),
        "upperBound": _format_jdbc_bound(high),
        "numPartitions": str(num_partitions),
    }


def _format_jdbc_bound(value: Any) -> str:
    """Render a partition bound for Spark's JDBC reader (no timezone suffix)."""
    from datetime import date, datetime

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


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
            try:
                result = fetch_contract(spec.table_name)
                missing = result is None
            except KeyError:
                missing = True
            if missing:
                errors.append(
                    f"ContractSource '{spec.table_name}' (field '{field_name}') "
                    f"not found in MongoDB — run upstream pipeline first"
                )
    return errors


def _execute_pipeline(meta: PipelineMeta, inputs: Inputs) -> RunResult:
    """Compute and verify a pipeline's output against its schema.

    The output is persisted DISK_ONLY (never held in the heap or collected to
    the driver) so the upstream sources are read exactly once: schema
    validation runs several scans and the persist step writes the same
    DataFrame, and without this each of those re-reads MongoDB / JDBC.
    """
    from pyspark.storagelevel import StorageLevel

    df = cast("DataFrame", meta.original_fn(inputs)).persist(StorageLevel.DISK_ONLY)
    errors: list[str] = []
    try:
        meta.model.verify(df, strict=True)  # type: ignore[attr-defined]
    except Exception as exc:
        errors.append(str(exc))

    # Capture column-level lineage from the analyzed plan (metadata only — no
    # recompute). Best-effort: a capture failure must never fail the run.
    lineage: dict[str, Any] | None = None
    try:
        from .lineage_runtime import capture_lineage, record_capture

        lineage = capture_lineage(df, meta.inputs_cls)
        record_capture(meta.table_name, lineage)
    except Exception:  # noqa: BLE001 — lineage is advisory
        lineage = None

    return RunResult(df=df, rows=df.count(), errors=errors, lineage=lineage)


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

    # For registry-key form ("storage:table"), _import_pipeline_module returned early
    # and skipped the fixtures module. Import it now that we have the module path.
    if ":" in pipeline_key:
        _fixtures_mod = meta.module.removesuffix(".pipeline") + ".fixtures"
        try:
            importlib.import_module(_fixtures_mod)
        except ImportError:
            pass

    inputs_cls = meta.inputs_cls
    # Scenarios are keyed by dotted module path (e.g. "bronze.smith.navigators"),
    # not by registry key — derive it from meta.module.
    scenario_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")

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
    timings: dict[str, float] = {}
    mongo_uri = os.getenv("MONGO_URI")
    spark_t0 = time.monotonic()
    spark = _ensure_local_spark()
    timings["spark_init_s"] = round(time.monotonic() - spark_t0, 3)
    cache: dict[str, DataFrame] = {}

    inputs_t0 = time.monotonic()
    if mode == "fixtures":
        inputs = _build_fixtures_inputs(scenario_key, inputs_cls, scenario_name=None)
    elif mode == "scenario":
        if scenario_name is None:
            raise ValueError("mode=scenario requires scenario_name.")
        inputs = _build_fixtures_inputs(scenario_key, inputs_cls, scenario_name)
    elif mode == "production":
        # Register every pipeline in the bundle so an in-bundle TableSource
        # upstream resolves against its producer's level (idempotent).
        from .discovery import discover_all_pipelines

        disc_t0 = time.monotonic()
        discover_all_pipelines()
        timings["discovery_s"] = round(time.monotonic() - disc_t0, 3)
        inputs = _build_production_inputs(
            spark, inputs_cls, mongo_uri=mongo_uri, cache=cache
        )
    elif mode == "fault":
        if not fault_name:
            raise ValueError("mode=fault requires fault_name.")
        inputs = _build_fixtures_inputs(scenario_key, inputs_cls, scenario_name=None)
        inputs = apply_fault(fault_name, inputs)
    else:
        raise AssertionError(f"unhandled mode {mode!r}")
    # inputs_s is input building only — discovery (production) is reported apart.
    timings["inputs_s"] = round(
        time.monotonic() - inputs_t0 - timings.get("discovery_s", 0.0), 3
    )

    compute_t0 = time.monotonic()
    result = _execute_pipeline(meta, inputs)
    timings["compute_s"] = round(time.monotonic() - compute_t0, 3)
    result.timings = timings
    return result


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
