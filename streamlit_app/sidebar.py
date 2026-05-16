"""Sidebar with pipeline search, level groups, and selection buttons."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app import cache
from streamlit_app.theme import LEVEL_COLORS, LEVEL_ORDER


def render() -> str | None:
    """Render the sidebar and return the currently selected pipeline name."""
    with st.sidebar:
        _brand()

        query = _search_bar()

        summaries = cache.cached_summaries()
        if not summaries:
            st.warning(
                "No contracts found.\n\nRun "
                "`poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v`."
            )
            return None

        if query:
            q = query.lower()
            summaries = [s for s in summaries if q in s["table_name"].lower()]

        by_level = _group_by_level(summaries)
        options = [
            s["table_name"]
            for level in _ordered_levels(by_level)
            for s in sorted(by_level[level], key=lambda s: s["table_name"])
        ]

        if not options:
            st.caption("No matches.")
            return None

        _summary_counts(by_level)
        _ensure_selection(options)
        _render_pipeline_groups(by_level)

        return st.session_state.get("selected_pipeline")


def _brand() -> None:
    st.markdown(
        "<div style='display:flex;align-items:center;gap:0.5rem;"
        "padding:0.25rem 0 1rem 0;'>"
        "<span style='font-size:1.5rem;color:#60a5fa;'>◆</span>"
        "<span style='font-size:1.15rem;font-weight:600;'>Poorbricks</span>"
        "<span style='color:#6b7280;font-size:0.85rem;'>Contracts</span>"
        "</div>",
        unsafe_allow_html=True,
    )


def _search_bar() -> str:
    col_search, col_refresh = st.columns([4, 1])
    with col_search:
        query = st.text_input(
            "Search",
            placeholder="Filter pipelines…",
            label_visibility="collapsed",
        )
    with col_refresh:
        if st.button("↻", help="Refresh contracts", use_container_width=True):
            cache.clear()
            st.rerun()
    return query


def _group_by_level(
    summaries: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_level: dict[str, list[dict[str, Any]]] = {}
    for summary in summaries:
        by_level.setdefault(summary.get("level", "?"), []).append(summary)
    return by_level


def _ordered_levels(by_level: dict[str, list[dict[str, Any]]]) -> list[str]:
    return [lvl for lvl in LEVEL_ORDER if lvl in by_level] + sorted(
        set(by_level) - set(LEVEL_ORDER)
    )


def _summary_counts(by_level: dict[str, list[dict[str, Any]]]) -> None:
    total = sum(len(v) for v in by_level.values())
    st.caption(
        f"**{total}** pipeline{'s' if total != 1 else ''} "
        f"· {len(by_level.get('bronze', []))} bronze "
        f"· {len(by_level.get('silver', []))} silver "
        f"· {len(by_level.get('gold', []))} gold"
    )


def _ensure_selection(options: list[str]) -> None:
    current = st.session_state.get("selected_pipeline")
    if current not in options:
        st.session_state["selected_pipeline"] = options[0]


def _render_pipeline_groups(by_level: dict[str, list[dict[str, Any]]]) -> None:
    for level in _ordered_levels(by_level):
        level_summaries = sorted(by_level.get(level, []), key=lambda s: s["table_name"])
        if not level_summaries:
            continue
        color = LEVEL_COLORS.get(level, "#6b7280")
        st.markdown(
            f"<div class='pl-section-label' style='color:{color};'>"
            f"● {level}  <span style='color:#4b5563;font-weight:500;'>"
            f"({len(level_summaries)})</span></div>",
            unsafe_allow_html=True,
        )
        for summary in level_summaries:
            _render_pipeline_button(summary["table_name"])


def _render_pipeline_button(name: str) -> None:
    short = name.split(".", 1)[-1] if "." in name else name
    is_active = st.session_state.get("selected_pipeline") == name
    if st.button(
        short,
        key=f"pick_{name}",
        use_container_width=True,
        type="primary" if is_active else "tertiary",
    ):
        st.session_state["selected_pipeline"] = name
        st.rerun()
