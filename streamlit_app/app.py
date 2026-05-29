"""Streamlit Contracts Explorer + Test Runner.

Run with:

    poetry run streamlit run streamlit_app/app.py

Pages:
  - Contracts: browse every pipeline's persisted contract and run their
    transforms against editable fixture rows. Contracts are produced by the
    distributed pipeline test; refresh them with
    `poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v`.
  - Status: warehouse health — tables missing a contract, empty tables, and
    the last-sync distribution with a 48h staleness alert.
  - Lineage: an interactive DAG of table-to-table dependencies.
  - Postgres status: row counts and sizes per table in the configured
    PostgreSQL warehouse.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from streamlit_app import (  # noqa: E402
    airflow_runs,
    cache,
    contract,
    dev_debug,
    header,
    health,
    lineage,
    postgres_status,
    runner,
    sidebar,
    status_dashboard,
    theme,
)

st.set_page_config(
    page_title="Poorbricks",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)
theme.inject()


def contracts_page() -> None:
    selected = sidebar.render()
    if selected is None:
        header.render_empty_state()
        return

    try:
        contract_doc = cache.cached_contract(selected)
    except KeyError as exc:
        st.error(str(exc))
        return

    header.render_header(contract_doc)

    contract_tab, runner_tab = st.tabs(["Contract", "Test runner"])
    with contract_tab:
        contract.render(contract_doc)
    with runner_tab:
        runner.render(contract_doc)

    header.render_footer(contract_doc)


def status_page() -> None:
    status_dashboard.render()


def lineage_page() -> None:
    lineage.render()


def health_page() -> None:
    health.render()


def postgres_status_page() -> None:
    postgres_status.render()


def airflow_runs_page() -> None:
    airflow_runs.render()


def dev_debug_page() -> None:
    dev_debug.render()


nav = st.navigation(
    [
        st.Page(
            contracts_page,
            title="Contracts",
            icon=":material/contract:",
            default=True,
        ),
        st.Page(
            status_page,
            title="Status",
            icon=":material/monitoring:",
        ),
        st.Page(
            lineage_page,
            title="Lineage",
            icon=":material/account_tree:",
        ),
        st.Page(
            health_page,
            title="Health",
            icon=":material/health_and_safety:",
        ),
        st.Page(
            postgres_status_page,
            title="Postgres status",
            icon=":material/database:",
        ),
        st.Page(
            airflow_runs_page,
            title="Airflow runs",
            icon=":material/event_repeat:",
        ),
        st.Page(
            dev_debug_page,
            title="Dev debug",
            icon=":material/bug_report:",
        ),
    ]
)
nav.run()
