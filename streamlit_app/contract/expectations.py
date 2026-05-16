"""Production-health expectations (min rows, unique keys, freshness, …)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render(contract: dict[str, Any]) -> None:
    expectations = contract.get("expectations") or {}
    has_content = expectations and any(
        v not in (None, [], {}, "")
        for k, v in expectations.items()
        if k != "class_name"
    )
    if not has_content:
        return

    st.markdown("### Expectations")
    st.caption("Production health thresholds checked at runtime.")

    _summary_metrics(expectations)
    _detail_table(expectations)


def _summary_metrics(expectations: dict[str, Any]) -> None:
    unique_keys = expectations.get("unique_keys") or []
    non_null = expectations.get("non_null_columns") or []
    cols = st.columns(3)
    cols[0].metric("Min rows", expectations.get("min_rows") or "—")
    cols[1].metric("Unique keys", len(unique_keys))
    cols[2].metric("Non-null columns", len(non_null))


def _detail_table(expectations: dict[str, Any]) -> None:
    rows: list[dict[str, Any]] = []
    unique_keys = expectations.get("unique_keys") or []
    if unique_keys:
        rows.append(
            {
                "check": "Unique keys",
                "value": ", ".join("(" + ", ".join(k) + ")" for k in unique_keys),
            }
        )
    non_null = expectations.get("non_null_columns") or []
    if non_null:
        rows.append({"check": "Non-null columns", "value": ", ".join(non_null)})
    if expectations.get("null_rate_max"):
        rows.append(
            {
                "check": "Max null rate",
                "value": str(expectations["null_rate_max"]),
            }
        )
    if expectations.get("enum_values"):
        rows.append(
            {
                "check": "Allowed enum values",
                "value": str(expectations["enum_values"]),
            }
        )
    if expectations.get("fresh_column"):
        rows.append(
            {
                "check": "Freshness",
                "value": (
                    f"max(`{expectations['fresh_column']}`) within "
                    f"{expectations.get('fresh_max_age_days')} days"
                ),
            }
        )
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
