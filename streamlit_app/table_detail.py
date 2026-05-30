"""Shared, condensed single-table detail component.

Reused by both top-level pages (the "Main" explorer and the "Live Now"
dashboard) so a table is rendered the same way everywhere. Composes the
existing reusable modules — the contract renderer, the Postgres sample
renderer, the run-history accessors, and (best-effort) the Airflow run
listing — into one screen built from ``st.subheader`` + ``st.expander``
sections instead of the old eight-tab layout.

Every external read degrades gracefully: a missing contract, an empty
run-history store, an unmaterialised table, or an absent Airflow all show
a caption/info message rather than raising.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app import cache, postgres_status
from streamlit_app import contract as contract_renderer


def render(table_name: str, environment: str = "prod") -> None:
    """Render a condensed, single-screen detail view for one table.

    Sections: header + last-run badge, published contract, field lineage,
    previous runs, Postgres status, and best-effort Airflow history.
    """
    _render_header(table_name, environment)
    _render_contract(table_name)
    _render_field_lineage(table_name)
    _render_previous_runs(table_name, environment)
    _render_postgres_status(table_name, environment)
    _render_airflow_history(table_name)


# --------------------------------------------------------------------------- #
# 1. Header + last-run status badge
# --------------------------------------------------------------------------- #
def _short_sha(sha: Any) -> str:
    return str(sha)[:7] if sha else "—"


def _render_header(table_name: str, environment: str) -> None:
    st.subheader(f"◆ {table_name}")

    last_runs = cache.cached_last_runs(environment)
    run = (
        last_runs.get(f"{environment}:{table_name}")
        if isinstance(last_runs, dict)
        else None
    )
    if run is None and isinstance(last_runs, dict):
        # Fall back to matching by table_name across any storage/environment.
        run = next(
            (r for key, r in last_runs.items() if key.split(":", 1)[-1] == table_name),
            None,
        )

    if not run:
        st.caption("no recorded runs")
        return

    icon = "✅" if run.get("status") == "ok" else "❌"
    rows = run.get("row_count")
    rows_txt = f"{rows:,}" if isinstance(rows, int) else "—"
    finished = (run.get("finished_at") or "—").replace("T", " ").split(".")[0]
    st.markdown(
        f"{icon} **{run.get('status', '?')}** · `{run.get('environment', '?')}` · "
        f"{rows_txt} rows · {finished} · `{_short_sha(run.get('sha'))}`"
    )


# --------------------------------------------------------------------------- #
# 2. Contract (schema / fields / expectations / inputs / profile / examples)
# --------------------------------------------------------------------------- #
def _load_contract(table_name: str) -> dict[str, Any] | None:
    try:
        return cache.cached_contract(table_name)
    except KeyError:
        return None
    except Exception:
        return None


def _render_contract(table_name: str) -> None:
    st.subheader("Contract")
    doc = _load_contract(table_name)
    if doc is None:
        st.info("No published contract for this table.")
        return
    with st.expander("Contract details", expanded=True):
        try:
            contract_renderer.render(doc)
        except Exception as exc:  # noqa: BLE001 - never let one section break the page
            st.warning(f"Could not render contract: `{type(exc).__name__}`: {exc}")


# --------------------------------------------------------------------------- #
# 3. Field lineage — how each output column is generated, and what it consumes
# --------------------------------------------------------------------------- #
def _render_field_lineage(table_name: str) -> None:
    st.subheader("Field lineage")
    doc = _load_contract(table_name)
    lineage = (doc or {}).get("lineage") or {}
    columns = lineage.get("columns") or {}
    consumed = lineage.get("consumed") or {}

    if not columns and not consumed:
        st.caption("No lineage recorded for this table.")
        return

    with st.expander("How fields are generated", expanded=False):
        if columns:
            rows: list[dict[str, Any]] = []
            for col, info in columns.items():
                sources = (info or {}).get("sources") or []
                if sources:
                    src_txt = ", ".join(
                        f"{s.get('table', s.get('input', '?'))}.{s.get('column', '?')}"
                        for s in sources
                    )
                else:
                    src_txt = "(literal/none)"
                rows.append(
                    {
                        "column": col,
                        "source(s)": src_txt,
                        "exact": (info or {}).get("exact"),
                    }
                )
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
        else:
            st.caption("No per-column lineage captured.")

        if consumed:
            st.markdown("**Upstream columns consumed**")
            consumed_rows = [
                {"upstream table": table, "columns": ", ".join(cols or [])}
                for table, cols in consumed.items()
            ]
            st.dataframe(
                pd.DataFrame(consumed_rows), hide_index=True, use_container_width=True
            )


# --------------------------------------------------------------------------- #
# 4. Previous runs — last ~10 from run history, scoped to this table
# --------------------------------------------------------------------------- #
def _render_previous_runs(table_name: str, environment: str) -> None:
    st.subheader("Previous runs")
    suffix = ":" + table_name
    runs = [
        r
        for r in cache.cached_runs(limit=200, environment=environment)
        if str(r.get("pipeline_key", "")).endswith(suffix)
    ]
    if not runs:
        st.caption("No recorded runs for this table.")
        return

    rows: list[dict[str, Any]] = []
    for r in runs[:10]:
        anomaly = r.get("anomaly") or {}
        anomaly_txt = anomaly.get("reason", "") if anomaly.get("is_anomaly") else ""
        finished = (r.get("finished_at") or "—").replace("T", " ").split(".")[0]
        duration = r.get("duration_s")
        rows.append(
            {
                "status": "✅" if r.get("status") == "ok" else "❌",
                "rows": r.get("row_count"),
                "duration_s": round(duration, 1)
                if isinstance(duration, int | float)
                else None,
                "anomaly": anomaly_txt,
                "finished_at": finished,
                "sha": _short_sha(r.get("sha")),
                "error": (r.get("error") or "")[:80],
            }
        )
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- #
# 5. Postgres status — materialised table snapshot + sample rows
# --------------------------------------------------------------------------- #
def _find_snapshot(
    snapshots: list[Any], table_name: str, environment: str
) -> Any | None:
    matches = [s for s in snapshots if s.name == table_name]
    if not matches:
        return None
    if environment == "dev":
        dev_match = next((s for s in matches if s.schema.endswith("__dev")), None)
        if dev_match is not None:
            return dev_match
    # Prefer a non-dev schema for prod; otherwise just take the first match.
    prod_match = next((s for s in matches if not s.schema.endswith("__dev")), None)
    return prod_match or matches[0]


def _render_postgres_status(table_name: str, environment: str) -> None:
    st.subheader("Postgres status")
    try:
        snapshots = cache.cached_warehouse_snapshots()
    except Exception:
        st.caption("Postgres warehouse unavailable.")
        return

    snap = _find_snapshot(snapshots, table_name, environment)
    if snap is None:
        st.caption("not materialised")
        return

    cols = st.columns(2)
    cols[0].metric("Rows", f"{snap.row_count:,}")
    cols[1].metric("Size", postgres_status._format_bytes(snap.size_bytes))
    st.caption(f"`{snap.schema}.{snap.name}`")
    with st.expander("Sample rows", expanded=False):
        try:
            postgres_status._render_sample(snap)
        except Exception as exc:  # noqa: BLE001
            st.caption(f"Could not render sample: `{type(exc).__name__}`: {exc}")


# --------------------------------------------------------------------------- #
# 6. Airflow history — best-effort; gracefully absent when no live Airflow
# --------------------------------------------------------------------------- #
def _matching_dags(airflow_url: str, table_name: str) -> list[str]:
    """DAG ids whose id contains the table, or whose tasks reference it."""
    from streamlit_app import airflow_runs

    dags = airflow_runs._list_dags(airflow_url)
    by_id = [d["dag_id"] for d in dags if table_name in d.get("dag_id", "")]
    if by_id:
        return by_id
    # Fall back to scanning task instances of the most recent run per DAG.
    matches: list[str] = []
    for d in dags:
        dag_id = d.get("dag_id")
        if not dag_id:
            continue
        try:
            runs = airflow_runs._list_runs(airflow_url, dag_id, limit=1)
            if not runs:
                continue
            tasks = airflow_runs._list_task_instances(
                airflow_url, dag_id, runs[0].get("dag_run_id", "")
            )
        except Exception:
            continue
        if any(table_name in (t.get("task_id") or "") for t in tasks):
            matches.append(dag_id)
    return matches


def _render_airflow_history(table_name: str) -> None:
    st.subheader("Airflow history")
    try:
        from poorbricks.airflow import watch
        from streamlit_app import airflow_runs

        airflow_url = st.session_state.get("airflow_url", watch.DEFAULT_AIRFLOW_URL)
        dag_ids = _matching_dags(airflow_url, table_name)
        if not dag_ids:
            st.caption("No matching Airflow DAG for this table.")
            return

        dag_id = dag_ids[0]
        runs = airflow_runs._list_runs(airflow_url, dag_id, limit=5)
        grid_url = f"{airflow_url.rstrip('/')}/dags/{dag_id}/grid"
        st.markdown(f"**{dag_id}** · [Open in Airflow]({grid_url})")
        if not runs:
            st.caption("No runs recorded for this DAG yet.")
            return

        rows = []
        for r in runs:
            start = (r.get("start_date") or "—").replace("T", " ").split(".")[0]
            end = (r.get("end_date") or "—").replace("T", " ").split(".")[0]
            dur = r.get("duration")
            rows.append(
                {
                    "state": airflow_runs._state_badge(r.get("state")),
                    "run_id": r.get("dag_run_id"),
                    "start": start,
                    "end": end,
                    "duration_s": round(dur, 1)
                    if isinstance(dur, int | float)
                    else None,
                }
            )
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    except Exception:
        st.caption("Airflow history unavailable (no live Airflow).")


__all__ = ["render"]
