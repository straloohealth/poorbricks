"""Sampled profile, example rows, and available fixture scenarios."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render(contract: dict[str, Any]) -> None:
    profile = contract.get("profile") or {}
    if not profile:
        return

    st.markdown("### Profile")
    st.caption("Snapshot taken at last contract push.")

    null_rates = profile.get("null_rates") or {}
    enum_samples = profile.get("enum_samples") or {}

    col_chart, col_enum = st.columns([3, 2]) if enum_samples else (st.container(), None)

    with col_chart:
        _null_rates_chart(null_rates)

    if enum_samples and col_enum is not None:
        with col_enum:
            st.markdown("**Enum samples**")
            st.caption("Low-cardinality fields and observed values.")
            st.json(enum_samples, expanded=False)


def examples(contract: dict[str, Any]) -> None:
    example_rows = contract.get("example_rows") or []
    if not example_rows:
        return
    st.markdown("### Example rows")
    st.caption(f"{len(example_rows)} sample row(s) captured at last contract push.")
    st.dataframe(pd.DataFrame(example_rows), use_container_width=True, hide_index=True)


def fixtures(contract: dict[str, Any]) -> None:
    fixture_list = contract.get("fixtures") or []
    if not fixture_list:
        return
    st.markdown("### Fixtures")
    st.caption(
        "Open the **Test runner** tab to edit fixtures and execute the pipeline."
    )
    summary = [_fixture_summary(fixture) for fixture in fixture_list]
    st.dataframe(pd.DataFrame(summary), use_container_width=True, hide_index=True)


def _null_rates_chart(null_rates: dict[str, float]) -> None:
    if not null_rates:
        st.caption("No null-rate data.")
        return
    st.markdown("**Null rates by column**")
    ranked = sorted(null_rates.items(), key=lambda kv: kv[1], reverse=True)
    df = pd.DataFrame(ranked, columns=["column", "null_rate"]).set_index("column")
    st.bar_chart(df, height=240)


def _fixture_summary(fixture: dict[str, Any]) -> dict[str, Any]:
    rows_by_source = fixture.get("rows_by_source") or {}
    total_rows = sum(len(rows) for rows in rows_by_source.values())
    return {
        "scenario": fixture["scenario"],
        "sources": len(rows_by_source),
        "total rows": total_rows,
        "rows by source": {src: len(rows) for src, rows in rows_by_source.items()},
    }
