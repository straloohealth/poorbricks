"""Streamlit Contracts Explorer + Test Runner.

Run with:

    poetry run streamlit run streamlit_app/app.py

Browse every pipeline's persisted contract (fields, expectations, inputs,
fixtures, sample data, profile) and run their transforms against editable
fixture rows.

The contract documents are produced by `scripts/push_contract.py`; refresh
them with `poetry run python scripts/push_contract.py --all` whenever a
pipeline's shape or fixtures change.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from streamlit_app import cache, contract, header, runner, sidebar, theme  # noqa: E402

st.set_page_config(
    page_title="Poorbricks Contracts",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)
theme.inject()


def main() -> None:
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


main()
