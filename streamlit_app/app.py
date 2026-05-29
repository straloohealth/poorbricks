"""Poorbricks observability UI — two condensed pages.

Run with:

    poetry run streamlit run streamlit_app/app.py

Pages:
  - Main: alerts grouped by severity, an interactive lineage navigator
    (click a table to highlight its sources + destinations), and a unified
    table-detail view (contract, field lineage, profiling, previous runs,
    Postgres status, last run, Airflow history).
  - Live Now: dev/prod toggle over Airflow run history, recent deduped errors
    per DAG, stale datasets, and a freshness distribution dot graph. Selecting a
    table renders the same table-detail component used on the Main page.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from streamlit_app import live_now, main_page, theme  # noqa: E402

st.set_page_config(
    page_title="Poorbricks",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)
theme.inject()


def main_page_render() -> None:
    main_page.render()


def live_now_render() -> None:
    live_now.render()


nav = st.navigation(
    [
        st.Page(
            main_page_render, title="Main", icon=":material/dashboard:", default=True
        ),
        st.Page(live_now_render, title="Live Now", icon=":material/bolt:"),
    ]
)
nav.run()
