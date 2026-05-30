"""Dev debug page: a live, input-free view of recent pipeline runs + results.

No manual entry — this auto-discovers activity from the poorbricks server:

* **Recent runs** from the run-history store (the same record every Airflow
  worker writes via ``run_and_persist``), so it reflects pipelines as they run —
  locally or on the cluster — with status, duration, row count, the relative
  row-count anomaly verdict, and the commit SHA.
* **Freshness** from the staleness monitor (overdue / missing pipelines).
* **Results** from the warehouse: every materialised table with its row count,
  size, and an auto-loaded sample of rows.

It refreshes on a short cache TTL (and a manual refresh button); there is
nothing to type.
"""

from __future__ import annotations

from typing import Any

import requests
import streamlit as st

from poorbricks.settings import settings

_STATUS_ICON = {"ok": "✅", "failed": "❌"}
_STATE_ICON = {"ok": "🟢", "overdue": "🟠", "missing": "🔴"}


def _api_url() -> str:
    return st.session_state.get("poorbricks_api_url", settings.contracts_api_url)


@st.cache_data(ttl=10, show_spinner=False)
def _get(path: str) -> Any:
    resp = requests.get(f"{_api_url().rstrip('/')}{path}", timeout=20)
    resp.raise_for_status()
    return resp.json()


def render() -> None:
    st.markdown("## Dev debug — live pipeline runs")
    st.caption(
        "Auto-discovered from the run-history store and the warehouse — no input "
        "needed. Trigger runs with `poorbricks upload --env dev … --watch` (or any "
        "`poorbricks run`); they appear here as they complete."
    )
    if st.button("↻ Refresh"):
        _get.clear()

    # --- Freshness (staleness monitor) -------------------------------------
    try:
        staleness = _get("/v1/staleness")
    except Exception:
        staleness = []
    overdue = [v for v in staleness if v.get("state") != "ok"]
    if overdue:
        st.warning(
            "Stale pipelines: "
            + ", ".join(
                f"{_STATE_ICON.get(v['state'], '')} `{v['pipeline_key']}` "
                f"({v['state']})"
                for v in overdue
            )
        )

    # --- Recent runs (run history) -----------------------------------------
    st.markdown("### Recent runs")
    try:
        runs = _get("/v1/runs?limit=50")
    except Exception as exc:  # noqa: BLE001 — surface a server/connectivity issue
        st.error(f"Could not load runs from {_api_url()}: {exc}")
        return
    if not runs:
        st.info("No runs recorded yet. Run a pipeline (e.g. `poorbricks run …`).")
    else:
        rows = []
        for r in runs:
            anom = r.get("anomaly") or {}
            rows.append(
                {
                    "": _STATUS_ICON.get(r.get("status"), "•"),
                    "pipeline": r.get("pipeline_key"),
                    "env": r.get("environment"),
                    "rows": r.get("row_count"),
                    "anomaly": "⚠️ " + anom.get("reason", "")
                    if anom.get("is_anomaly")
                    else "",
                    "dur (s)": r.get("duration_s"),
                    "sha": (r.get("sha") or "")[:7],
                    "finished": r.get("finished_at"),
                    "error": (r.get("error") or "")[:80],
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

    # --- Results: every warehouse table, with an auto-loaded sample --------
    st.markdown("### Results (warehouse tables)")
    try:
        stats = _get("/v1/stats")
    except Exception as exc:  # noqa: BLE001
        st.info(f"Warehouse stats unavailable: {exc}")
        return
    # Hide the framework's internal bookkeeping schema from the results view.
    tables = [t for t in stats.get("tables", []) if t["schema"] != "poorbricks_meta"]
    if not tables:
        st.info("No tables materialised yet.")
        return
    st.caption(
        f"{stats.get('table_count', 0)} tables · "
        f"{stats.get('total_rows', 0):,} total rows"
    )
    for t in sorted(tables, key=lambda x: (x["schema"], x["name"])):
        schema, name = t["schema"], t["name"]
        label = f"{schema}.{name} — {t['row_count']:,} rows"
        with st.expander(label):
            try:
                preview = _get(f"/v1/table/{schema}/{name}?limit=20")
                sample = preview.get("sample_rows") or []
                if sample:
                    st.dataframe(sample, use_container_width=True, hide_index=True)
                else:
                    st.caption("(empty)")
            except Exception as exc:  # noqa: BLE001
                st.caption(f"preview unavailable: {exc}")


__all__ = ["render"]
