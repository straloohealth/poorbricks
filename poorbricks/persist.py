"""Persistence layer: compute + write to Postgres + push contracts."""

from __future__ import annotations

import hashlib
import json
import os
import time
from typing import TYPE_CHECKING, Any

from .inputs import Inputs
from .registry import PipelineMeta
from .runner import RunResult, run
from .settings import settings

if TYPE_CHECKING:
    from validation import ValidatedStruct


# Regression-vs-prior diff loads both tables into the driver via pandas; skip it
# above this row count unless a pipeline raises its own REGRESSION_MAX_ROWS.
_REGRESSION_MAX_ROWS = 1_000_000


def _pg_table_name(table_name: str) -> str:
    """Map logical table name to a PostgreSQL-safe table name (no dots)."""
    return table_name.replace(".", "_")


def _flatten_fields(
    schema_json: dict[str, Any], literal_columns: set[str] | None = None
) -> list[dict[str, Any]]:
    """Pull out `[{name, type, nullable, description?, is_literal?}]` from a schema.

    ``description`` comes from the ``StructField`` metadata (sourced from the
    pydantic ``Field(description=...)``); ``is_literal`` flags columns produced
    by a constant ``f.lit(...)`` so the management UI can show an info badge.
    """
    literals = literal_columns or set()
    fields: list[dict[str, Any]] = []
    for field in schema_json.get("fields", []):
        spark_type = field.get("type")
        if isinstance(spark_type, dict):
            type_label = spark_type.get("type", "struct")
        else:
            type_label = str(spark_type)
        entry: dict[str, Any] = {
            "name": field["name"],
            "type": type_label,
            "nullable": field.get("nullable", True),
        }
        description = (field.get("metadata") or {}).get("description")
        if description:
            entry["description"] = description
        if field["name"] in literals:
            entry["is_literal"] = True
        fields.append(entry)
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
        "row_count_anomaly_method": getattr(
            expectations_cls, "ROW_COUNT_ANOMALY_METHOD", None
        ),
        "regression_enabled": getattr(expectations_cls, "REGRESSION_ENABLED", True),
        "regression_join_keys": getattr(expectations_cls, "REGRESSION_JOIN_KEYS", None),
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


def _log_run_timings(
    pipeline_key: str, mode: str, timings: dict[str, float], environment: str
) -> None:
    """Emit one structured timing line per run so DAG slowness is measurable.

    Printed to stdout (not the logger) so it always lands in worker pod logs
    captured by the KubernetesPodOperator, alongside the CLI's other output.
    """
    fields = " ".join(f"{key}={value}" for key, value in timings.items())
    print(
        f"[timing] pipeline={pipeline_key} mode={mode} env={environment} {fields}",
        flush=True,
    )


def _schema_hash(schema_json: dict[str, Any]) -> str:
    """Stable hash of a schema, used to detect schema changes between runs."""
    return hashlib.sha256(
        json.dumps(schema_json, sort_keys=True, default=str).encode()
    ).hexdigest()


def _safe_record(rec: Any) -> None:
    """Record a run to the history store, swallowing any error.

    Instrumentation must never fail a production pipeline: a meta-store outage
    is logged to stdout (so it lands in worker pod logs) and otherwise ignored.
    """
    try:
        from .run_history import RunHistoryStore

        RunHistoryStore().record(rec)
    except Exception as exc:  # noqa: BLE001 — best-effort instrumentation
        print(f"[run-history] record failed: {exc}", flush=True)


def _literal_columns_for_meta(meta: PipelineMeta) -> set[str]:
    """Schema columns this pipeline projects as a literal constant (best-effort).

    Used to flag ``is_literal`` on the contract fields so the management UI can
    show an informational badge. Never raises.
    """
    try:
        import importlib
        from pathlib import Path

        from .verification.no_stubs import literal_columns_for

        module = importlib.import_module(meta.module)
        if not module.__file__:
            return set()
        return literal_columns_for(Path(module.__file__).parent / "transform.py")
    except Exception:  # noqa: BLE001 — informational only
        return set()


def _analyze_data_health(
    registry_key: str,
    row_count: int,
    loader: Any,
    level: str,
    pg_table: str,
    exp_cls: type | None,
    environment: str,
    sha: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Row-count anomaly + regression-vs-prior; emit alerts. Best-effort.

    Returns ``(anomaly_summary, regression_summary)`` for the run record. Each
    sub-check is isolated so a meta-store/regression failure never breaks the
    pipeline run.
    """
    from .alerting import Alert, emit

    anomaly_dict: dict[str, Any] | None = None
    regression_dict: dict[str, Any] | None = None
    alerts: list[Alert] = []

    try:
        from .anomaly import check_row_count
        from .run_history import RunHistoryStore

        history = [
            rec.row_count
            for rec in RunHistoryStore().recent_successful(
                registry_key, 20, environment=environment
            )
            if rec.row_count is not None
        ]
        verdict = check_row_count(
            registry_key,
            row_count,
            history,
            method=getattr(exp_cls, "ROW_COUNT_ANOMALY_METHOD", None),
            z=getattr(exp_cls, "ROW_COUNT_ANOMALY_Z", None),
            pct=getattr(exp_cls, "ROW_COUNT_ANOMALY_PCT", None),
            min_samples=getattr(exp_cls, "ROW_COUNT_ANOMALY_MIN_SAMPLES", None),
        )
        anomaly_dict = verdict.to_dict()
        if verdict.is_anomaly:
            alerts.append(
                Alert(
                    kind="row_count_anomaly",
                    pipeline_key=registry_key,
                    severity="warn",
                    summary=verdict.reason,
                    context=anomaly_dict,
                    environment=environment,
                    sha=sha,
                )
            )
    except Exception as exc:  # noqa: BLE001 — instrumentation must not fail a run
        print(f"[anomaly] check failed: {exc}", flush=True)

    try:
        from .regression.prior import (
            diff_against_prior,
            drop_prior,
            regression_summary,
            snapshot_prior,
        )

        enabled = (
            True if exp_cls is None else getattr(exp_cls, "REGRESSION_ENABLED", True)
        )
        join_keys = getattr(exp_cls, "REGRESSION_JOIN_KEYS", None)
        unique_keys = getattr(exp_cls, "UNIQUE_KEYS", None)
        if not join_keys and unique_keys:
            join_keys = unique_keys[0]
        # The diff loads both the current and prior table into the driver via
        # pandas, so cap it by row count to avoid OOM on large tables (the
        # snapshot also doubles on-disk storage). Above the cap we skip the diff
        # AND drop any stale snapshot, so a later under-cap run does not compare
        # against an ancient frozen snapshot and raise a false regression.
        max_rows = getattr(exp_cls, "REGRESSION_MAX_ROWS", None) or _REGRESSION_MAX_ROWS
        if enabled and join_keys and row_count > max_rows:
            print(
                f"[regression] skipped for {registry_key}: {row_count} rows "
                f"> REGRESSION_MAX_ROWS={max_rows}",
                flush=True,
            )
            drop_prior(loader, level, pg_table)
        elif enabled and join_keys:
            report = diff_against_prior(
                loader,
                level,
                pg_table,
                list(join_keys),
                tolerance_pct=getattr(exp_cls, "REGRESSION_TOLERANCE_PCT", None)
                or 10.0,
                ignore_columns=getattr(exp_cls, "REGRESSION_IGNORE_COLUMNS", []) or [],
            )
            if report is not None:
                regression_dict = regression_summary(report)
                # Alert only on VALUE drift ("fail"). A column appearing/
                # disappearing between runs (missing/extra) is legitimate schema
                # evolution, not a data regression, and would otherwise alert on
                # every schema change. It is still recorded in regression_dict.
                value_regressions = [
                    c for c in report.regressions() if c.status == "fail"
                ]
                if value_regressions:
                    alerts.append(
                        Alert(
                            kind="regression",
                            pipeline_key=registry_key,
                            severity="warn",
                            summary=(
                                f"{len(value_regressions)} column(s) drifted in value "
                                "vs the previous run"
                            ),
                            context=regression_dict,
                            environment=environment,
                            sha=sha,
                        )
                    )
            # Refresh the prior snapshot so the next run compares against this one.
            snapshot_prior(loader, level, pg_table)
    except Exception as exc:  # noqa: BLE001 — instrumentation must not fail a run
        print(f"[regression] check failed: {exc}", flush=True)

    # Only page on alerts for production runs — a dev preview's tiny fixture
    # counts must not trip prod on-call. The verdicts are still recorded in the
    # run record (and shown in the dashboard) regardless of environment.
    if environment == "prod":
        emit(alerts)
    return anomaly_dict, regression_dict


def run_and_persist(
    pipeline_key: str,
    mode: str = "fixtures",
    scenario_name: str | None = None,
) -> RunResult:
    """Compute pipeline, write to Postgres if storage='postgres', push contract to MongoDB.

    Wraps the run so every execution is recorded in ``poorbricks_meta.run_history``
    (success or failure) and a denormalized ``last_run`` summary is written into
    the contract. Arch and contract-source checks are enforced by run() — not
    repeated here.
    """
    from datetime import UTC, datetime

    from .run_history import RunRecord, run_context

    # Single source of truth for the environment: ``settings`` (which also
    # drives the schema suffix + contract-publish decision), so the run record,
    # the dev schema, and alert gating never disagree. Only the SHA comes from
    # the live env (it is not a settings field).
    _, sha = run_context()
    environment = settings.environment
    prefix = os.getenv("POORBRICKS_PREFIX", "")
    started_at = datetime.now(UTC)
    total_t0 = time.monotonic()

    meta: PipelineMeta | None = None
    table_name = pipeline_key
    schema_hash: str | None = None
    row_count: int | None = None
    status = "ok"
    error_msg: str | None = None
    result: RunResult | None = None
    anomaly_payload: dict[str, Any] | None = None
    drift_payload: dict[str, Any] | None = None

    try:
        result = run(pipeline_key, mode, scenario_name)
        if result.df is None:
            status = "failed" if result.errors else "ok"
            error_msg = "; ".join(result.errors) or None
            return result

        from .runner import _resolve_meta

        meta = _resolve_meta(pipeline_key)
        table_name = meta.table_name
        registry_key = f"{meta.target_storage}:{meta.table_name}"

        from utils.contracts import profile_dataframe, push_contract
        from validation.expectations import find_expectations_class

        exp_cls = find_expectations_class(pipeline_key)
        schema = meta.model.to_struct()  # type: ignore[attr-defined]
        schema_json = schema.jsonValue()
        schema_hash = _schema_hash(schema_json)

        # Write to Postgres and profile the result, all without collecting the
        # DataFrame to the driver: the writer streams partition by partition, and
        # the profile is a single SQL pass over the freshly written table.
        # Dev/test runs write to a suffixed schema (e.g. ``silver__dev``) so they
        # never touch prod tables, and they do NOT publish a contract (which is
        # keyed by bare table name and would clobber the prod contract).
        target_schema = f"{meta.level}{settings.schema_suffix}"
        publish_contract = settings.environment == "prod"

        if meta.target_storage == "postgres":
            from utils.postgres import PostgresLoader

            loader = PostgresLoader()
            pg_table = _pg_table_name(meta.table_name)
            write_t0 = time.monotonic()
            result.rows = loader.write(result.df, target_schema, pg_table)
            result.timings["write_s"] = round(time.monotonic() - write_t0, 3)
            profile = loader.profile_table(target_schema, pg_table, schema)
            row_count = result.rows
            # Row-count anomaly + regression-vs-prior, with alerts. The history
            # query reflects only prior runs (this run is recorded in finally).
            anomaly_payload, drift_payload = _analyze_data_health(
                registry_key,
                row_count,
                loader,
                target_schema,
                pg_table,
                exp_cls,
                environment,
                sha,
            )
        else:
            profile = profile_dataframe(result.df)
            row_count = (
                result.rows if result.rows is not None else profile.get("row_count")
            )

        if not publish_contract:
            # Dev run: skip the contract push entirely. The data is in the dev
            # schema for preview; prod contracts stay pristine.
            result.timings["total_s"] = round(time.monotonic() - total_t0, 3)
            _log_run_timings(pipeline_key, mode, result.timings, environment)
            return result

        # Get example rows — fixture failures are non-fatal (contract still publishes)
        if mode == "fixtures":
            example_rows = [
                r.asDict(recursive=True) for r in result.df.limit(5).collect()
            ]
        else:
            try:
                fixtures_result = run(pipeline_key, mode="fixtures", scenario_name=None)
                if fixtures_result.df is not None:
                    example_rows = [
                        r.asDict(recursive=True)
                        for r in fixtures_result.df.limit(5).collect()
                    ]
                else:
                    example_rows = []
            except Exception:
                example_rows = []

        last_run = {
            "environment": environment,
            "sha": sha,
            "status": "ok",
            "finished_at": datetime.now(UTC).isoformat(),
            "row_count": row_count,
            "duration_s": round(time.monotonic() - total_t0, 3),
            "schema_hash": schema_hash,
        }

        contract_t0 = time.monotonic()
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
            fields=_flatten_fields(schema_json, _literal_columns_for_meta(meta)),
            validation_rules=_serialize_rules(meta.model),
            expectations=_serialize_expectations(exp_cls),
            inputs=_serialize_inputs(meta.inputs_cls),
            fixtures=_serialize_fixtures(meta),
            prefix=prefix,
            lineage=result.lineage,
            last_run=last_run,
        )
        result.timings["contract_s"] = round(time.monotonic() - contract_t0, 3)

        result.timings["total_s"] = round(time.monotonic() - total_t0, 3)
        _log_run_timings(pipeline_key, mode, result.timings, environment)
        return result
    except Exception as exc:
        status = "failed"
        error_msg = str(exc)
        _safe_alert_failure(
            f"{meta.target_storage}:{meta.table_name}" if meta else pipeline_key,
            str(exc),
            environment,
            sha,
        )
        raise
    finally:
        finished_at = datetime.now(UTC)
        rec = RunRecord(
            pipeline_key=(
                f"{meta.target_storage}:{meta.table_name}" if meta else pipeline_key
            ),
            table_name=table_name,
            environment=environment,
            mode=mode,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_s=round(time.monotonic() - total_t0, 3),
            sha=sha,
            row_count=row_count,
            schema_hash=schema_hash,
            error=error_msg,
            anomaly=anomaly_payload,
            drift_summary=drift_payload,
            timings=dict(result.timings) if result is not None else {},
        )
        _safe_record(rec)


def _safe_alert_failure(
    registry_key: str, message: str, environment: str, sha: str | None
) -> None:
    """Emit a failure alert without ever raising (instrumentation is best-effort).

    Gated to production so a dev run's failure does not page prod on-call.
    """
    if environment != "prod":
        return
    try:
        from .alerting import Alert, emit

        emit(
            [
                Alert(
                    kind="failure",
                    pipeline_key=registry_key,
                    severity="error",
                    summary=message[:500],
                    environment=environment,
                    sha=sha,
                )
            ]
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[alert] failure alert error: {exc}", flush=True)


__all__ = ["run_and_persist"]
