"""Airflow runs page: see DAG state, task outcomes, logs, and profiles.

Talks to the Airflow webserver via the same v2 REST API that
``poorbricks upload --watch`` uses, so a run started from the CLI shows
up here with the same task list and log excerpts. For successful tasks
that materialise a pipeline output, this page also surfaces the
per-pipeline profile written by ``run_and_persist`` (row count + field
profile) by pivoting through the contracts store.
"""

from __future__ import annotations

import urllib.parse
from typing import Any

import streamlit as st

from poorbricks.airflow import watch as _watch
from utils.contracts import fetch_contract

DEFAULT_AIRFLOW_URL = _watch.DEFAULT_AIRFLOW_URL
DEFAULT_LOKI_URL = _watch.DEFAULT_LOKI_URL


@st.cache_data(ttl=30, show_spinner=False)
def _list_dags(airflow_url: str) -> list[dict[str, Any]]:
    """Return every DAG the Airflow API exposes (page through if needed)."""
    out: list[dict[str, Any]] = []
    limit = 100
    offset = 0
    while True:
        url = (
            f"{airflow_url.rstrip('/')}/api/v2/dags"
            f"?limit={limit}&offset={offset}&order_by=dag_id"
        )
        status, text = _watch._request("GET", url)
        body = _watch._json_or_raise(status, text, "list-dags")
        dags = body.get("dags", []) or []
        out.extend(dags)
        if len(dags) < limit:
            break
        offset += limit
    return out


@st.cache_data(ttl=30, show_spinner=False)
def _list_runs(airflow_url: str, dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/"
        f"{urllib.parse.quote(dag_id, safe='')}/dagRuns"
        f"?order_by=-start_date&limit={limit}"
    )
    status, text = _watch._request("GET", url)
    body = _watch._json_or_raise(status, text, f"list-runs {dag_id}")
    return body.get("dag_runs", []) or []


@st.cache_data(ttl=15, show_spinner=False)
def _list_task_instances(
    airflow_url: str, dag_id: str, run_id: str
) -> list[dict[str, Any]]:
    return _watch._list_task_instances(airflow_url, dag_id, run_id)


@st.cache_data(ttl=15, show_spinner=False)
def _logs(
    airflow_url: str,
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    *,
    loki_url: str | None,
    start_ts: str | None,
    end_ts: str | None,
) -> tuple[list[str], str | None]:
    """Fetch logs from Airflow first; fall back to Loki when the API is
    blocked by the secret_key mismatch (or otherwise returns no content)."""
    lines, reason = _watch.fetch_task_logs(
        airflow_url,
        dag_id,
        run_id,
        task_id,
        try_number=try_number,
        max_lines=400,
    )
    if lines or not loki_url:
        return lines, reason
    loki_lines, loki_reason = _watch.fetch_logs_from_loki(
        loki_url,
        dag_id=dag_id,
        task_id=task_id,
        start_ts=start_ts,
        end_ts=end_ts,
        max_lines=400,
    )
    if loki_lines:
        return loki_lines, f"airflow_api:{reason}; loki_fallback_ok" if reason else None
    return [], f"{reason or 'empty'} (loki: {loki_reason or 'empty'})"


@st.cache_data(ttl=60, show_spinner=False)
def _try_contract(table_name: str) -> dict[str, Any] | None:
    """Look up a published contract for a task's pipeline name.

    Returns ``None`` when the table isn't in the contracts store (the task
    hasn't run successfully yet, or the pipeline writes nothing the framework
    profiles)."""
    try:
        return fetch_contract(table_name)
    except KeyError:
        return None


_STATE_BADGE = {
    "success": ":green-background[OK]",
    "failed": ":red-background[FAIL]",
    "upstream_failed": ":red-background[UPSTREAM_FAIL]",
    "running": ":blue-background[RUN]",
    "queued": ":blue-background[QUEUE]",
    "skipped": ":gray-background[SKIP]",
    "scheduled": ":gray-background[SCHED]",
    "no_status": ":gray-background[--]",
}


def _state_badge(state: str | None) -> str:
    if not state:
        return _STATE_BADGE["no_status"]
    return _STATE_BADGE.get(state, f":gray-background[{state}]")


def _render_dag_picker(dags: list[dict[str, Any]]) -> str | None:
    poorbricks_dags = [
        d for d in dags if any(t.get("name") == "poorbricks" for t in d.get("tags", []))
    ]
    pool = poorbricks_dags or dags
    if not pool:
        st.info("No DAGs available from the Airflow API.")
        return None
    prefixes = sorted({d["dag_id"].split("__", 1)[0] for d in pool})
    cols = st.columns([1, 3])
    with cols[0]:
        prefix = st.selectbox(
            "Prefix",
            ["(all)"] + prefixes,
            key="airflow_runs_prefix",
            help="Filter by the prefix every upload writes under (e.g. `gold-biz`).",
        )
    filtered = [
        d for d in pool if prefix == "(all)" or d["dag_id"].startswith(prefix + "__")
    ]
    with cols[1]:
        dag_id = st.selectbox(
            "DAG",
            [d["dag_id"] for d in filtered],
            key="airflow_runs_dag",
            help="The DAG name follows `{prefix}__{workflow}`; the workflow comes from `workflows/*.yaml`.",
        )
    return dag_id


def _render_run_table(
    airflow_url: str, dag_id: str, runs: list[dict[str, Any]]
) -> str | None:
    if not runs:
        st.info(
            f"No runs yet for `{dag_id}`. Trigger one with `poorbricks upload --watch`."
        )
        return None
    options = []
    for r in runs:
        state = r.get("state") or "no_status"
        run_id = r.get("dag_run_id", "?")
        start = (r.get("start_date") or "—").replace("T", " ").split(".")[0]
        end = (r.get("end_date") or "—").replace("T", " ").split(".")[0]
        dur = r.get("duration")
        dur_s = f"{dur:.0f}s" if isinstance(dur, int | float) else "—"
        run_type = r.get("run_type", "?")
        options.append(
            (
                run_id,
                f"{_state_badge(state)}  {run_type:7s}  {start} → {end}  ({dur_s})  {run_id}",
            )
        )
    labels = [text for _, text in options]
    idx = st.radio(
        "Recent runs",
        list(range(len(labels))),
        format_func=lambda i: labels[i],
        key=f"airflow_runs_pick_{dag_id}",
    )
    return options[idx][0]


def _render_task_table(
    airflow_url: str, dag_id: str, run_id: str, tasks: list[dict[str, Any]]
) -> None:
    st.write(f"### Tasks in {dag_id} / `{run_id}`")
    rows = []
    for ti in tasks:
        state = ti.get("state") or "no_status"
        rows.append(
            {
                "task_id": ti.get("task_id"),
                "state": state,
                "try": ti.get("try_number"),
                "duration_s": (
                    f"{ti['duration']:.1f}"
                    if isinstance(ti.get("duration"), int | float)
                    else "—"
                ),
                "operator": ti.get("operator_name") or ti.get("operator"),
                "pipeline": ti.get("rendered_fields", {}).get("name") or "",
            }
        )
    st.dataframe(rows, use_container_width=True, hide_index=True)

    failed = [ti for ti in tasks if ti.get("state") in {"failed", "upstream_failed"}]
    if failed:
        st.write("### Failed tasks — logs")
        for ti in failed:
            with st.expander(
                f"{ti.get('task_id')}  ({ti.get('state')}, try={ti.get('try_number')})"
            ):
                try_number = ti.get("try_number") or 1
                lines, reason = _logs(
                    airflow_url,
                    dag_id,
                    run_id,
                    ti.get("task_id"),
                    int(try_number),
                    loki_url=st.session_state.get("loki_url") or None,
                    start_ts=ti.get("start_date"),
                    end_ts=ti.get("end_date"),
                )
                if reason == "secret_key_mismatch":
                    st.error(
                        "Airflow components have different `[api] secret_key` values, "
                        "so the API can't decode the path to the worker's log file. "
                        "Fix in `deploy/k8s/airflow-custom/01-secrets.yaml` + the four "
                        "component deployments — sync the same `secret-key` across "
                        "api-server, scheduler, triggerer, and dag-processor."
                    )
                elif reason:
                    st.warning(f"Logs blocked: {reason}")
                elif not lines:
                    st.warning("Empty log payload returned.")
                else:
                    excerpts = _watch.extract_log_errors(lines, max_errors=40)
                    if excerpts:
                        st.caption("Error excerpts")
                        st.code("\n".join(excerpts), language="text")
                    st.caption(f"Full log (last {len(lines)} lines)")
                    st.code("\n".join(lines), language="text")

    success = [ti for ti in tasks if ti.get("state") == "success"]
    if success:
        st.write("### Successful tasks — pipeline profile")
        st.caption(
            "Row counts + per-field profile are pushed to the contracts store by "
            "`run_and_persist` when a pipeline writes. The table below pivots on "
            "the contract entry for each task's pipeline name."
        )
        for ti in success:
            task_id = ti.get("task_id")
            contract = _try_contract(task_id)
            if not contract:
                st.write(f"- `{task_id}` — no contract published (no profile to show).")
                continue
            with st.expander(f"{task_id}  —  profile + contract metadata"):
                profile = contract.get("profile") or contract.get("field_profile") or {}
                row_count = (
                    profile.get("row_count") if isinstance(profile, dict) else None
                )
                if row_count is not None:
                    st.metric("Rows", row_count)
                columns = profile.get("columns") if isinstance(profile, dict) else None
                if isinstance(columns, list) and columns:
                    st.caption("Per-column profile")
                    st.dataframe(columns, use_container_width=True, hide_index=True)
                elif isinstance(columns, dict) and columns:
                    st.caption("Per-column profile")
                    st.dataframe(
                        [{"column": k, **v} for k, v in columns.items()],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.write(
                        "_No column profile in contract. Profile shape: "
                        f"`{type(profile).__name__}`._"
                    )
                example_rows = contract.get("example_rows") or []
                if example_rows:
                    st.caption(f"Example rows ({len(example_rows)})")
                    st.dataframe(
                        example_rows, use_container_width=True, hide_index=True
                    )


def render() -> None:
    st.title("Airflow runs")
    st.caption(
        "Live view of DAG runs across every `poorbricks upload` prefix. "
        "Pulls from the Airflow v2 API; cached for 30s to stay responsive."
    )

    cols = st.columns(2)
    with cols[0]:
        airflow_url = st.text_input(
            "Airflow webserver URL",
            value=st.session_state.get("airflow_url", DEFAULT_AIRFLOW_URL),
            help="Defaults to the company tailnet ingress.",
        )
        st.session_state["airflow_url"] = airflow_url
    with cols[1]:
        loki_url = st.text_input(
            "Loki URL (log fallback)",
            value=st.session_state.get("loki_url", DEFAULT_LOKI_URL),
            help=(
                "Used as a fallback when the Airflow v2 logs API returns the "
                "`secret_key` mismatch warning. Leave empty to disable."
            ),
        )
        st.session_state["loki_url"] = loki_url

    if st.button("Refresh"):
        _list_dags.clear()
        _list_runs.clear()
        _list_task_instances.clear()
        _logs.clear()
        _try_contract.clear()
        st.rerun()

    try:
        dags = _list_dags(airflow_url)
    except RuntimeError as exc:
        st.error(f"Could not reach Airflow at {airflow_url}: {exc}")
        return

    dag_id = _render_dag_picker(dags)
    if not dag_id:
        return

    try:
        runs = _list_runs(airflow_url, dag_id)
    except RuntimeError as exc:
        st.error(f"Could not list runs for {dag_id}: {exc}")
        return

    run_id = _render_run_table(airflow_url, dag_id, runs)
    if not run_id:
        return

    try:
        tasks = _list_task_instances(airflow_url, dag_id, run_id)
    except RuntimeError as exc:
        st.error(f"Could not list tasks for {run_id}: {exc}")
        return

    _render_task_table(airflow_url, dag_id, run_id, tasks)
