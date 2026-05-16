"""Top-of-page metrics and the storage destination summary."""

from __future__ import annotations

from typing import Any

import streamlit as st


def metrics(contract: dict[str, Any]) -> None:
    fields = contract.get("fields") or []
    rules = contract.get("validation_rules") or []
    inputs = contract.get("inputs") or []
    fixtures = contract.get("fixtures") or []
    profile = contract.get("profile") or {}
    row_count = profile.get("row_count") if profile else None

    cols = st.columns(5)
    cols[0].metric("Fields", len(fields))
    cols[1].metric("Inputs", len(inputs))
    cols[2].metric("Validation rules", len(rules))
    cols[3].metric("Fixtures", len(fixtures))
    cols[4].metric("Sampled rows", row_count if row_count is not None else "—")


def storage(contract: dict[str, Any]) -> None:
    storage_kind = contract.get("storage", "delta")
    level = contract.get("level", "?")
    table_name = contract["table_name"]
    st.markdown("### Storage")
    if storage_kind == "postgres":
        st.markdown(
            f"Materialized into PostgreSQL as `analytics.{level}.{table_name}`."
        )
    else:
        st.markdown("`delta` — Spark memory only (test / fixture mode).")
