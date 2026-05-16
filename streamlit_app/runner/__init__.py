"""Editable-fixtures test runner tab."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app.runner.execution import run_pipeline


def render(contract: dict[str, Any]) -> None:
    """Render scenario picker, editable tables per source, and run button."""
    fixtures = contract.get("fixtures") or []
    if not fixtures:
        st.info(
            "No fixtures were captured for this pipeline. Add a `@scenario` "
            "to `fixtures.py` and re-run `push_contract.py`."
        )
        return

    selected, fixture = _pick_scenario(fixtures)
    rows_by_source = fixture.get("rows_by_source") or {}
    edited_by_source = _render_editors(contract, selected, rows_by_source)

    st.divider()
    if _run_button():
        _execute(contract, edited_by_source)


def _pick_scenario(
    fixtures: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    scenario_names = [f["scenario"] for f in fixtures]
    col_picker, col_meta = st.columns([2, 3])
    with col_picker:
        selected = st.selectbox("Scenario", scenario_names, key="scenario_picker")
    fixture = next(f for f in fixtures if f["scenario"] == selected)
    rows_by_source = fixture.get("rows_by_source") or {}
    total_rows = sum(len(rows) for rows in rows_by_source.values())
    with col_meta:
        st.markdown(
            f"<div style='padding-top:1.85rem;color:#9ca3af;font-size:0.85rem;'>"
            f"<b style='color:#e5e7eb;'>{len(rows_by_source)}</b> source(s) · "
            f"<b style='color:#e5e7eb;'>{total_rows}</b> row(s)</div>",
            unsafe_allow_html=True,
        )
    return selected, fixture


def _render_editors(
    contract: dict[str, Any],
    selected: str,
    rows_by_source: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    st.caption(
        "Edit any value, or add/remove rows. The transform runs on the "
        "values shown below."
    )
    edited_by_source: dict[str, list[dict[str, Any]]] = {}
    for src, rows in rows_by_source.items():
        with st.expander(f"Input  ·  {src}  ({len(rows)} row(s))", expanded=True):
            df = pd.DataFrame(rows)
            edited = st.data_editor(
                df,
                num_rows="dynamic",
                use_container_width=True,
                key=f"editor_{contract['table_name']}_{selected}_{src}",
            )
            edited_by_source[src] = edited.to_dict(orient="records")
    return edited_by_source


def _run_button() -> bool:
    run_col, _ = st.columns([1, 5])
    with run_col:
        return st.button(
            "Run transform",
            type="primary",
            use_container_width=True,
            icon=":material/play_arrow:",
        )


def _execute(
    contract: dict[str, Any],
    edited_by_source: dict[str, list[dict[str, Any]]],
) -> None:
    with st.status("Running transform…", expanded=True) as status:
        ok = run_pipeline(contract, edited_by_source)
        if ok:
            status.update(label="Run complete", state="complete", expanded=True)
        else:
            status.update(label="Run failed", state="error", expanded=True)
