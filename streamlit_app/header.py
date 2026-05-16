"""Page header and empty-state rendering."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.theme import LEVEL_COLORS


def render_header(contract: dict[str, Any]) -> None:
    table_name = contract["table_name"]
    level = contract.get("level", "?")
    storage = contract.get("storage", "?")
    module = contract.get("module", "")
    comment = contract.get("comment") or ""
    color = LEVEL_COLORS.get(level, "#6b7280")

    st.markdown(
        f"""
        <div class="page-header">
            <div class="page-header-title">{table_name}</div>
            <div class="page-header-module">{module}</div>
        </div>
        <div style="display:flex;gap:0.4rem;align-items:center;margin-bottom:0.85rem;">
            <span style="background:{color};color:#0e1117;padding:0.15rem 0.55rem;
                         border-radius:4px;font-size:0.75rem;font-weight:600;
                         text-transform:uppercase;letter-spacing:0.05em;">{level}</span>
            <span style="background:rgba(96,165,250,0.15);color:#60a5fa;
                         padding:0.15rem 0.55rem;border-radius:4px;font-size:0.75rem;
                         font-weight:600;">{storage}</span>
        </div>
        {f'<div class="page-header-comment">{comment}</div>' if comment else ""}
        """,
        unsafe_allow_html=True,
    )


def render_empty_state() -> None:
    st.markdown(
        "<div style='text-align:center;padding:5rem 1rem;color:#6b7280;'>"
        "<div style='font-size:3rem;color:#374151;margin-bottom:0.5rem;'>◆</div>"
        "<div style='font-size:1.2rem;color:#9ca3af;font-weight:500;'>"
        "Poorbricks Contracts Explorer</div>"
        "<div style='margin-top:0.5rem;'>No contracts found.</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.code(
        "poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v",
        language="bash",
    )


def render_footer(contract: dict[str, Any]) -> None:
    pushed_at = contract.get("pushed_at")
    if not pushed_at:
        return
    st.markdown(
        f"<div style='text-align:right;color:#4b5563;font-size:0.75rem;"
        f"margin-top:2rem;'>Last pushed · {pushed_at}</div>",
        unsafe_allow_html=True,
    )
