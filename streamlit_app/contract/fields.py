"""Output schema (fields) and per-row validation rules."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render(contract: dict[str, Any]) -> None:
    st.markdown("### Fields")
    fields = contract.get("fields") or []
    if not fields:
        st.caption("No field metadata stored.")
        return

    df = pd.DataFrame(fields)
    if "nullable" in df.columns:
        df = df.assign(nullable=df["nullable"].map(_nullable_label))

    st.dataframe(df, use_container_width=True, hide_index=True)


def validation_rules(contract: dict[str, Any]) -> None:
    rules = contract.get("validation_rules") or []
    if not rules:
        return
    st.markdown("### Validation rules")
    st.caption("Per-row checks executed by `model.verify(df)`.")
    st.dataframe(pd.DataFrame(rules), use_container_width=True, hide_index=True)


def _nullable_label(val: Any) -> str:
    if isinstance(val, bool):
        return "nullable" if val else "required"
    return str(val)
