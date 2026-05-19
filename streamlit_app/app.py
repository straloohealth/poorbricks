"""Streamlit Contracts Explorer + Test Runner.

Run with:

    poetry run streamlit run streamlit_app/app.py

Pages:
  - Contracts: browse every pipeline's persisted contract and run their
    transforms against editable fixture rows. Contracts are produced by the
    distributed pipeline test; refresh them with
    `poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v`.
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
    cache,
    contract,
    header,
    postgres_status,
    runner,
    sidebar,
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


def postgres_status_page() -> None:
    postgres_status.render()


nav = st.navigation(
    [
        st.Page(
            contracts_page,
            title="Contracts",
            icon=":material/contract:",
            default=True,
        ),
        st.Page(
            postgres_status_page,
            title="Postgres status",
            icon=":material/database:",
        ),
    ]
)
nav.run()
