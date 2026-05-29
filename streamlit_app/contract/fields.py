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
    if "is_literal" in df.columns:
        df = df.assign(is_literal=df["is_literal"].fillna(False).map(bool))

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Literal columns carry a constant value with no upstream source —
    # informational, not an error (e.g. a constant flag or source tag).
    literal_cols = [f["name"] for f in fields if f.get("is_literal")]
    if literal_cols:
        st.info(
            "ℹ️ Literal columns (constant value, no upstream source): "
            + ", ".join(f"`{c}`" for c in literal_cols)
        )


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
