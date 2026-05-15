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
from typing import Any

import streamlit as st

# Ensure the project root is in sys.path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from streamlit_app import contract_view, runner_view
from utils.contracts import fetch_contract, list_contracts

st.set_page_config(
    page_title="Poorbricks Contracts",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=60)
def _cached_summaries() -> list[dict[str, Any]]:
    return list_contracts()


@st.cache_data(ttl=60)
def _cached_contract(table_name: str) -> dict[str, Any]:
    return fetch_contract(table_name)


def _sidebar() -> str | None:
    st.sidebar.title("📜 Contracts")
    if st.sidebar.button("🔄 Refresh contracts"):
        _cached_summaries.clear()
        _cached_contract.clear()

    summaries = _cached_summaries()
    if not summaries:
        st.sidebar.warning(
            "No contracts found. Run "
            "`poetry run python scripts/push_contract.py --all`."
        )
        return None

    by_level: dict[str, list[dict[str, Any]]] = {}
    for summary in summaries:
        by_level.setdefault(summary.get("level", "?"), []).append(summary)

    level_order = ["bronze", "silver", "gold"]
    options: list[str] = []
    captions: dict[str, str] = {}
    for level in level_order + sorted(set(by_level) - set(level_order)):
        for summary in sorted(
            by_level.get(level, []), key=lambda s: s["table_name"]
        ):
            options.append(summary["table_name"])
            captions[summary["table_name"]] = (
                f"{level} / {summary.get('storage', '?')}"
            )

    if not options:
        return None

    selected = st.sidebar.radio(
        "Pipeline",
        options=options,
        format_func=lambda t: f"{t}  ({captions[t]})",
        index=0,
    )
    return selected


def main() -> None:
    selected = _sidebar()
    if selected is None:
        st.title("Poorbricks Contracts Explorer")
        st.info(
            "No contract is selected. Push a contract first:\n\n"
            "```\npoetry run python scripts/push_contract.py --all\n```"
        )
        return

    try:
        contract = _cached_contract(selected)
    except KeyError as exc:
        st.error(str(exc))
        return

    contract_tab, runner_tab = st.tabs(["📋 Contract", "🧪 Run tests"])
    with contract_tab:
        contract_view.render(contract)
    with runner_tab:
        runner_view.render(contract)


main()
