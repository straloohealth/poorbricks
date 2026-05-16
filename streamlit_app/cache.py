"""Cached MongoDB contract fetchers for the Streamlit UI."""

from __future__ import annotations

from typing import Any

import streamlit as st

from utils.contracts import fetch_contract, list_contracts


@st.cache_data(ttl=60)
def cached_summaries() -> list[dict[str, Any]]:
    return list_contracts()


@st.cache_data(ttl=60)
def cached_contract(table_name: str) -> dict[str, Any]:
    return fetch_contract(table_name)


def clear() -> None:
    cached_summaries.clear()
    cached_contract.clear()
