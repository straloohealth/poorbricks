"""Live Now page — a real-time operational view scoped to one environment.

A ``prod``/``dev`` toggle at the top filters every section below. Composes
the existing reusable modules instead of reinventing: it pulls Airflow runs
through :mod:`streamlit_app.airflow_runs`, the run-history / staleness /
last-run accessors through :mod:`streamlit_app.cache`, and renders the same
per-table drill-down via :func:`streamlit_app.table_detail.render` that the
Main page uses.

Four sections:
  1. Airflow history — recent DAG runs (best-effort; degrades when no live
     Airflow is reachable). Selecting a table renders the shared detail view.
  2. Recent errors per DAG — the 5 most recent *distinct* failing pipelines.
  3. Stale datasets — overdue + missing pipelines from staleness verdicts.
  4. Freshness distribution — an Altair dot chart of per-table age, with click
     selection that lists every table in the clicked age bucket.

Every external read is wrapped so an absent Airflow or empty store shows a
caption/info message rather than raising.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import streamlit as st

from streamlit_app import cache, table_detail

# Age buckets used by the freshness distribution, ordered oldest-allowed first.
# (upper_bound_hours, label) — None upper bound is the catch-all ">7d" lane.
_BUCKETS: list[tuple[float | None, str]] = [
    (1.0, "<1h"),
    (6.0, "1-6h"),
    (24.0, "6-24h"),
    (24.0 * 7, "1-7d"),
    (None, ">7d"),
]


def render() -> None:
    """Render the Live Now operational dashboard for one environment."""
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Live Now</span>"
        "<span class='page-header-module'>operational view</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_env, col_refresh = st.columns([4, 1])
    with col_env:
        env = st.radio("Environment", ["prod", "dev"], horizontal=True)
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True):
            cache.clear()
            st.rerun()

    _render_airflow_history(env)
    _render_recent_errors(env)
    _render_stale_datasets(env)
    _render_freshness_distribution(env)


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _table_name(pipeline_key: str | None) -> str:
    """Extract the table name from a ``<storage>:<table_name>`` pipeline key."""
    return str(pipeline_key or "").split(":", 1)[-1]


def _parse_ts(value: Any) -> datetime | None:
    """Parse an ISO timestamp string (as stored by the cache) to aware UTC."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _fmt_ts(value: Any) -> str:
    return str(value or "—").replace("T", " ").split(".")[0]


# --------------------------------------------------------------------------- #
# 1. Airflow history
# --------------------------------------------------------------------------- #
def _dag_in_env(dag_id: str, env: str) -> bool:
    """dev shows only ``dev-*`` DAGs; prod excludes them."""
    is_dev = dag_id.startswith("dev-")
    return is_dev if env == "dev" else not is_dev


@st.cache_data(ttl=15, show_spinner=False)
def _dag_tables(airflow_url: str, dag_id: str) -> list[str]:
    """Table/pipeline names a DAG produces, read from its latest run's tasks.

    Falls back to an empty list (caller offers the dag_id itself) when the
    rendered ``name`` field isn't available.
    """
    from streamlit_app import airflow_runs

    try:
        runs = airflow_runs._list_runs(airflow_url, dag_id, limit=1)
        if not runs:
            return []
        tasks = airflow_runs._list_task_instances(
            airflow_url, dag_id, runs[0].get("dag_run_id", "")
        )
    except Exception:
        return []
    names = []
    for ti in tasks:
        name = (ti.get("rendered_fields") or {}).get("name") or ti.get("task_id")
        if name and name not in names:
            names.append(name)
    return names


def _render_airflow_history(env: str) -> None:
    st.subheader("Airflow history")
    from poorbricks.airflow import watch

    airflow_url = st.session_state.setdefault("airflow_url", watch.DEFAULT_AIRFLOW_URL)
    airflow_url = st.text_input(
        "Airflow webserver URL",
        value=airflow_url,
        key="live_now_airflow_url",
        help="Defaults to the company tailnet ingress.",
    )
    st.session_state["airflow_url"] = airflow_url
    st.markdown(f"[Open Airflow]({airflow_url})")

    try:
        from streamlit_app import airflow_runs

        dags = [
            d
            for d in airflow_runs._list_dags(airflow_url)
            if _dag_in_env(d.get("dag_id", ""), env)
        ]
    except Exception:
        st.info("No live Airflow reachable.")
        return

    if not dags:
        st.caption(f"No `{env}` DAGs exposed by Airflow.")
        return

    # One combined table of the 5 most recent runs per DAG.
    rows: list[dict[str, Any]] = []
    try:
        for d in dags:
            dag_id = d.get("dag_id")
            if not dag_id:
                continue
            for r in airflow_runs._list_runs(airflow_url, dag_id, limit=5):
                rows.append(
                    {
                        "dag_id": dag_id,
                        "run_id": r.get("dag_run_id"),
                        "state": airflow_runs._state_badge(r.get("state")),
                        "start": _fmt_ts(r.get("start_date")),
                    }
                )
    except Exception:
        st.info("No live Airflow reachable.")
        return

    if not rows:
        st.caption("No recent DAG runs.")
    else:
        rows.sort(key=lambda x: x.get("start") or "", reverse=True)
        st.dataframe(rows, hide_index=True, use_container_width=True)

    # Drill-down: pick a DAG, then a table it produces, then reuse the shared
    # table_detail component (identical to the Main page).
    dag_ids = [d["dag_id"] for d in dags if d.get("dag_id")]
    if not dag_ids:
        return
    sel_cols = st.columns(2)
    with sel_cols[0]:
        sel_dag = st.selectbox("Inspect DAG", dag_ids, key=f"live_now_dag_{env}")
    tables = _dag_tables(airflow_url, sel_dag) or [_table_name(sel_dag)]
    with sel_cols[1]:
        sel_table = st.selectbox("Table", tables, key=f"live_now_table_{env}")
    if sel_table:
        table_detail.render(sel_table, environment=env)


# --------------------------------------------------------------------------- #
# 2. Recent 5 errors per DAG (deduped — one row per pipeline)
# --------------------------------------------------------------------------- #
def _render_recent_errors(env: str) -> None:
    st.subheader("Recent errors")
    st.caption("Most recent failure per pipeline (deduped), newest first.")

    runs = [
        r
        for r in cache.cached_runs(limit=300, environment=env)
        if r.get("status") == "failed"
    ]
    # Dedup by pipeline_key, keeping the most recent failure.
    latest: dict[str, dict[str, Any]] = {}
    for r in runs:
        key = r.get("pipeline_key")
        if key is None:
            continue
        prev = latest.get(key)
        if prev is None or (r.get("finished_at") or "") > (
            prev.get("finished_at") or ""
        ):
            latest[key] = r

    ordered = sorted(
        latest.values(), key=lambda r: r.get("finished_at") or "", reverse=True
    )[:5]
    if not ordered:
        st.caption("No recorded failures.")
        return

    st.dataframe(
        [
            {
                "pipeline_key": r.get("pipeline_key"),
                "finished_at": _fmt_ts(r.get("finished_at")),
                "error": (r.get("error") or "")[:200],
            }
            for r in ordered
        ],
        hide_index=True,
        use_container_width=True,
    )


# --------------------------------------------------------------------------- #
# 3. Stale datasets
# --------------------------------------------------------------------------- #
def _render_stale_datasets(env: str) -> None:
    st.subheader("Stale datasets")
    verdicts = [
        v
        for v in cache.cached_staleness(environment=env)
        if v.get("state") in {"overdue", "missing"}
    ]
    if not verdicts:
        st.success("All datasets are fresh — nothing overdue or missing.")
        return

    # Sort missing first, then most-overdue first; keep age as a typed float
    # for the sort key, separate from the display column.
    def _age_hours(v: dict[str, Any]) -> float:
        age_s = v.get("age_s")
        return age_s / 3600.0 if isinstance(age_s, int | float) else 0.0

    verdicts.sort(key=lambda v: (v.get("state") != "missing", -_age_hours(v)))

    rows: list[dict[str, Any]] = []
    for v in verdicts:
        age_s = v.get("age_s")
        rows.append(
            {
                "pipeline_key": v.get("pipeline_key"),
                "state": v.get("state"),
                "last_run": _fmt_ts(v.get("last_run")),
                "age (h)": round(age_s / 3600, 1)
                if isinstance(age_s, int | float)
                else None,
            }
        )
    st.dataframe(rows, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- #
# 4. Freshness distribution (dot graph + bucket drill-down)
# --------------------------------------------------------------------------- #
def _bucket_label(age_hours: float) -> str:
    for upper, label in _BUCKETS:
        if upper is None or age_hours < upper:
            return label
    return _BUCKETS[-1][1]


def _freshness_rows(env: str) -> list[dict[str, Any]]:
    """Per-table age in hours from the last-run-per-pipeline map."""
    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for key, run in cache.cached_last_runs(environment=env).items():
        finished = _parse_ts(run.get("finished_at"))
        if finished is None:
            continue
        age_hours = max((now - finished).total_seconds() / 3600.0, 0.0)
        rows.append(
            {
                "table": _table_name(key),
                "age_hours": round(age_hours, 2),
                "bucket": _bucket_label(age_hours),
                "last_run": _fmt_ts(run.get("finished_at")),
            }
        )
    return rows


def _render_freshness_distribution(env: str) -> None:
    st.subheader("Freshness distribution")
    rows = _freshness_rows(env)
    if not rows:
        st.caption("No last-run data to chart.")
        return

    bucket_order = [label for _, label in _BUCKETS]
    grouped: dict[str, list[dict[str, Any]]] = {label: [] for label in bucket_order}
    for r in rows:
        grouped[r["bucket"]].append(r)

    selected_bucket = _render_dot_chart(rows, bucket_order)
    if selected_bucket == "__fallback__":
        # The fallback path already rendered per-bucket expanders with the
        # full table lists, so there's nothing more to show here.
        return

    st.markdown("**Tables in bucket**")
    if selected_bucket:
        targets = (
            [selected_bucket] if isinstance(selected_bucket, str) else selected_bucket
        )
        any_shown = False
        for label in bucket_order:
            if label not in targets:
                continue
            tables = sorted(grouped[label], key=lambda x: x["age_hours"], reverse=True)
            if not tables:
                continue
            any_shown = True
            st.caption(f"{label} · {len(tables)} table(s)")
            st.dataframe(
                [
                    {
                        "table": t["table"],
                        "age (h)": t["age_hours"],
                        "last_run": t["last_run"],
                    }
                    for t in tables
                ],
                hide_index=True,
                use_container_width=True,
            )
        if not any_shown:
            st.caption("No tables in the selected bucket.")
    else:
        st.caption("Click a point (or bucket lane) above to list its tables.")


def _render_dot_chart(
    rows: list[dict[str, Any]], bucket_order: list[str]
) -> list[str] | str | None:
    """Altair strip/dot chart with click selection.

    Returns the clicked bucket label(s), or ``None`` if nothing is selected.
    Falls back to per-bucket expanders (returning their labels) when Altair
    selection is unavailable.
    """
    try:
        import altair as alt
        import pandas as pd
    except Exception:
        return _bucket_fallback(rows, bucket_order)

    df = pd.DataFrame(rows)
    present_buckets = [b for b in bucket_order if b in set(df["bucket"])]

    try:
        select = alt.selection_point(fields=["bucket"], on="click")
        chart = (
            alt.Chart(df)
            .mark_circle(size=140, opacity=0.75)
            .encode(
                x=alt.X("age_hours:Q", title="Age (hours)"),
                y=alt.Y(
                    "bucket:N",
                    title="Freshness bucket",
                    sort=present_buckets,
                ),
                color=alt.Color("bucket:N", sort=present_buckets, legend=None),
                tooltip=[
                    alt.Tooltip("table:N", title="Table"),
                    alt.Tooltip("age_hours:Q", title="Age (h)", format=".2f"),
                    alt.Tooltip("last_run:N", title="Last run"),
                ],
                opacity=alt.condition(select, alt.value(0.95), alt.value(0.25)),
            )
            .add_params(select)
            .properties(height=240)
        )
    except Exception:
        return _bucket_fallback(rows, bucket_order)

    try:
        event = st.altair_chart(chart, use_container_width=True, on_select="rerun")
    except Exception:
        # Older Streamlit without selection support — render static + fallback.
        try:
            st.altair_chart(chart, use_container_width=True)
        except Exception:
            pass
        return _bucket_fallback(rows, bucket_order)

    # Pull the clicked bucket(s) out of the selection payload. The event is a
    # mapping-like object whose exact type varies by Streamlit version, so we
    # treat it loosely and guard every access.
    buckets: list[str] = []
    try:
        payload: Any = event
        selection: Any = payload.get("selection") if payload else None
        if isinstance(selection, dict):
            for points in selection.values():
                if not isinstance(points, list):
                    continue
                for p in points:
                    b = p.get("bucket") if isinstance(p, dict) else None
                    if b and b not in buckets:
                        buckets.append(b)
    except Exception:
        buckets = []
    return buckets or None


def _bucket_fallback(rows: list[dict[str, Any]], bucket_order: list[str]) -> str:
    """No Altair selection available — render expanders, one per bucket.

    Returns the ``"__fallback__"`` sentinel so the caller knows the bucket
    lists were already rendered and skips the duplicate "Tables in bucket"
    panel.
    """
    grouped: dict[str, list[dict[str, Any]]] = {label: [] for label in bucket_order}
    for r in rows:
        grouped[r["bucket"]].append(r)
    st.caption("Altair selection unavailable — expand a bucket to list its tables.")
    for label in bucket_order:
        tables = sorted(grouped[label], key=lambda x: x["age_hours"], reverse=True)
        if not tables:
            continue
        with st.expander(f"{label} · {len(tables)} table(s)"):
            st.dataframe(
                [
                    {
                        "table": t["table"],
                        "age (h)": t["age_hours"],
                        "last_run": t["last_run"],
                    }
                    for t in tables
                ],
                hide_index=True,
                use_container_width=True,
            )
    return "__fallback__"


__all__ = ["render"]
