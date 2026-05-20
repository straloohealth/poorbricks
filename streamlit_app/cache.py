"""Cached MongoDB + PostgreSQL fetchers shared by the Streamlit pages."""

from __future__ import annotations

from typing import Any

import streamlit as st

from utils.contracts import fetch_contract, list_contract_details, list_contracts
from utils.postgres import PostgresInspector, TableSnapshot


@st.cache_data(ttl=60)
def cached_summaries() -> list[dict[str, Any]]:
    return list_contracts()


@st.cache_data(ttl=60)
def cached_contract(table_name: str) -> dict[str, Any]:
    return fetch_contract(table_name)


@st.cache_data(ttl=60)
def cached_contract_details() -> list[dict[str, Any]]:
    return list_contract_details()


@st.cache_data(ttl=60)
def cached_server_info() -> dict[str, str]:
    return PostgresInspector().server_info()


@st.cache_data(ttl=60, show_spinner="Inspecting warehouse…")
def cached_warehouse_snapshots() -> list[TableSnapshot]:
    return PostgresInspector().inspect(sample_size=10)


def clear() -> None:
    """Clear every cached fetcher so the next render refetches."""
    cached_summaries.clear()
    cached_contract.clear()
    cached_contract_details.clear()
    cached_server_info.clear()
    cached_warehouse_snapshots.clear()
