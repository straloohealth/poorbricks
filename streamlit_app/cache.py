"""Cached MongoDB + PostgreSQL fetchers shared by the Streamlit pages."""

from __future__ import annotations

import dataclasses
import os
from datetime import datetime
from typing import Any

import streamlit as st

from utils.contracts import fetch_contract, list_contract_details, list_contracts
from utils.postgres import PostgresInspector, TableSnapshot

try:  # Python 3.11+
    from datetime import UTC
except ImportError:  # pragma: no cover - older interpreters
    UTC = UTC


@st.cache_data(ttl=60)
def cached_summaries() -> list[dict[str, Any]]:
    return list_contracts()


@st.cache_data(ttl=60)
def cached_contract(table_name: str) -> dict[str, Any]:
    return fetch_contract(table_name)


@st.cache_data(ttl=60)
def cached_contract_details() -> list[dict[str, Any]]:
    return list_contract_details()


@st.cache_data(ttl=60)
def cached_server_info() -> dict[str, str]:
    return PostgresInspector().server_info()


@st.cache_data(ttl=60, show_spinner="Inspecting warehouse…")
def cached_warehouse_snapshots() -> list[TableSnapshot]:
    return PostgresInspector().inspect(sample_size=10)


def _record_to_dict(rec: Any) -> dict[str, Any]:
    """Convert a RunRecord dataclass to a JSON-friendly plain dict."""
    d = dataclasses.asdict(rec)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


@st.cache_data(ttl=15)
def cached_runs(
    limit: int = 100, environment: str | None = None
) -> list[dict[str, Any]]:
    """Recent run history as plain dicts, optionally filtered by environment."""
    try:
        from poorbricks.run_history import RunHistoryStore

        rows = [_record_to_dict(r) for r in RunHistoryStore().recent(limit)]
        if environment is not None:
            rows = [r for r in rows if r.get("environment") == environment]
        return rows
    except Exception:
        return []


@st.cache_data(ttl=15)
def cached_last_runs(environment: str | None = None) -> dict[str, dict[str, Any]]:
    """Latest run per pipeline_key as plain dicts."""
    try:
        from poorbricks.run_history import RunHistoryStore

        latest = RunHistoryStore().last_run_per_pipeline(environment)
        return {key: _record_to_dict(rec) for key, rec in latest.items()}
    except Exception:
        return {}


@st.cache_data(ttl=15)
def cached_staleness(environment: str = "prod") -> list[dict[str, Any]]:
    """Staleness verdicts from DAG cadences vs. last-run finish times."""
    try:
        from pathlib import Path

        from poorbricks.airflow.dag_store import LocalDagStore
        from poorbricks.run_history import RunHistoryStore
        from poorbricks.staleness import cadences_from_dags, evaluate

        now = datetime.now(UTC)
        store = LocalDagStore(
            Path(os.getenv("POORBRICKS_API_DAGS_DIR", "/opt/airflow/dags"))
        )
        cadences = cadences_from_dags(store, now=now)
        last = {
            key: rec.finished_at
            for key, rec in RunHistoryStore().last_run_per_pipeline(environment).items()
            if rec.finished_at
        }
        return [v.to_dict() for v in evaluate(cadences, last, now)]
    except Exception:
        return []


def _run_sort_key(run: dict[str, Any]) -> str:
    return run.get("finished_at") or ""


@st.cache_data(ttl=15)
def cached_alerts(environment: str = "prod") -> dict[str, list[dict[str, Any]]]:
    """Alerts grouped by severity, derived from runs + staleness verdicts."""
    try:
        alerts: list[dict[str, Any]] = []

        # Keep the newest run per pipeline_key (by finished_at).
        latest: dict[str, dict[str, Any]] = {}
        for run in cached_runs(limit=200, environment=environment):
            key = run.get("pipeline_key")
            if key is None:
                continue
            prev = latest.get(key)
            if prev is None or _run_sort_key(run) > _run_sort_key(prev):
                latest[key] = run

        for key, run in latest.items():
            finished_at = run.get("finished_at")
            if run.get("status") == "failed":
                alerts.append(
                    {
                        "severity": "error",
                        "kind": "failure",
                        "pipeline_key": key,
                        "summary": (run.get("error") or "")[:160],
                        "finished_at": finished_at,
                    }
                )
            anomaly = run.get("anomaly")
            if anomaly and anomaly.get("is_anomaly"):
                alerts.append(
                    {
                        "severity": "warn",
                        "kind": "row_count_anomaly",
                        "pipeline_key": key,
                        "summary": anomaly.get("reason") or "row-count anomaly",
                        "finished_at": finished_at,
                    }
                )
            drift = run.get("drift_summary")
            if drift and (
                drift.get("regression_count") or drift.get("regressed_columns")
            ):
                alerts.append(
                    {
                        "severity": "warn",
                        "kind": "regression",
                        "pipeline_key": key,
                        "summary": "regression vs. prior run",
                        "finished_at": finished_at,
                    }
                )

        for verdict in cached_staleness(environment):
            state = verdict.get("state")
            key = verdict.get("pipeline_key")
            if state == "missing":
                alerts.append(
                    {
                        "severity": "error",
                        "kind": "staleness",
                        "pipeline_key": key,
                        "summary": "no run on record",
                        "finished_at": verdict.get("last_run"),
                    }
                )
            elif state == "overdue":
                age_s = verdict.get("age_s") or 0
                interval_s = verdict.get("interval_s") or 0
                alerts.append(
                    {
                        "severity": "warn",
                        "kind": "staleness",
                        "pipeline_key": key,
                        "summary": (
                            f"overdue: last run {round(age_s / 3600, 1)}h ago "
                            f"(expected every {round(interval_s / 3600, 1)}h)"
                        ),
                        "finished_at": verdict.get("last_run"),
                    }
                )

        grouped: dict[str, list[dict[str, Any]]] = {
            "error": [],
            "warn": [],
            "info": [],
        }
        for alert in alerts:
            grouped.setdefault(alert["severity"], []).append(alert)
        for bucket in grouped.values():
            bucket.sort(key=lambda a: a.get("finished_at") or "", reverse=True)
        return grouped
    except Exception:
        return {"error": [], "warn": [], "info": []}


def _trunc(cols: list[str], n: int = 8) -> str:
    head = ", ".join(cols[:n])
    return head + ("…" if len(cols) > n else "")


@st.cache_data(ttl=30)
def cached_verification_findings() -> dict[str, list[dict[str, Any]]]:
    """Static verification findings derived from the published CONTRACTS.

    Distinct from runtime alerts: these surface architecture/contract issues
    that the framework's checks find (and that ``verify --mode arch`` /
    ``verify --mode contract`` gate on), made visible without re-scanning source:

    * **stub** (warn) — a schema column with no upstream lineage source (or, when
      lineage is absent, a column that is 100% null) — i.e. ``f.lit(None)``
      placeholders. These are what "stubs should never be used" forbids.
    * **literal** (info) — a column flagged ``is_literal`` (constant ``f.lit``).
    * **contract_break** (error) — a column this table consumes that no longer
      exists in the upstream's published contract (only checked between two
      published contracts, so a not-yet-seeded upstream is never false-flagged).
    """
    grouped: dict[str, list[dict[str, Any]]] = {"error": [], "warn": [], "info": []}
    try:
        names = [str(s["table_name"]) for s in list_contracts() if s.get("table_name")]
        contracts: dict[str, dict[str, Any]] = {}
        for name in names:
            try:
                contracts[name] = cached_contract(name)
            except Exception:
                continue

        for name, c in contracts.items():
            cols = (c.get("lineage") or {}).get("columns") or {}
            null_rates = (c.get("profile") or {}).get("null_rates") or {}
            fields = c.get("fields") or []
            if cols:
                stub = sorted(k for k, v in cols.items() if not v.get("sources"))
            else:
                stub = sorted(k for k, r in null_rates.items() if r == 1.0)
            literals = sorted(f["name"] for f in fields if f.get("is_literal"))
            if stub:
                grouped["warn"].append(
                    {
                        "severity": "warn",
                        "kind": "stub",
                        "pipeline_key": name,
                        "summary": f"{len(stub)} column(s) with no upstream source "
                        f"(stub/placeholder): {_trunc(stub)}",
                    }
                )
            if literals:
                grouped["info"].append(
                    {
                        "severity": "info",
                        "kind": "literal",
                        "pipeline_key": name,
                        "summary": f"{len(literals)} literal column(s): {_trunc(literals)}",
                    }
                )

        # Column-level contract breaks, only between two PUBLISHED contracts.
        from pyspark.sql.types import StructType

        for name, c in contracts.items():
            consumed = (c.get("lineage") or {}).get("consumed") or {}
            for upstream, used_cols in consumed.items():
                up = contracts.get(upstream)
                if up is None:
                    continue  # not seeded locally → don't false-flag
                have = {f.name for f in StructType.fromJson(up["schema_json"]).fields}
                missing = sorted(set(used_cols) - have)
                if missing:
                    grouped["error"].append(
                        {
                            "severity": "error",
                            "kind": "contract_break",
                            "pipeline_key": name,
                            "summary": f"consumes {upstream}.{{{', '.join(missing)}}} "
                            f"which no longer exists upstream",
                        }
                    )
    except Exception:
        return {"error": [], "warn": [], "info": []}
    return grouped


def clear() -> None:
    """Clear every cached fetcher so the next render refetches."""
    cached_summaries.clear()
    cached_contract.clear()
    cached_contract_details.clear()
    cached_server_info.clear()
    cached_warehouse_snapshots.clear()
    cached_runs.clear()
    cached_last_runs.clear()
    cached_staleness.clear()
    cached_alerts.clear()
    cached_verification_findings.clear()
