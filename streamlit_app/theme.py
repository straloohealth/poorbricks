"""Custom CSS and shared visual constants for the contracts UI."""

from __future__ import annotations

import streamlit as st

LEVEL_COLORS: dict[str, str] = {
    "bronze": "#b45309",
    "silver": "#94a3b8",
    "gold": "#eab308",
}

STORAGE_GLYPH: dict[str, str] = {
    "delta": "△",
    "postgres": "◇",
    "mongo": "○",
}

LEVEL_ORDER: list[str] = ["bronze", "silver", "gold"]


_CUSTOM_CSS = """
<style>
    .block-container { padding-top: 2rem; padding-bottom: 4rem; max-width: 1400px; }

    section[data-testid="stSidebar"] {
        background-color: #0e1117;
        border-right: 1px solid #1f2937;
    }
    section[data-testid="stSidebar"] .stRadio > label { display: none; }
    section[data-testid="stSidebar"] [role="radiogroup"] > label {
        padding: 0.4rem 0.6rem;
        border-radius: 6px;
        margin-bottom: 2px;
        transition: background-color 0.15s ease;
    }
    section[data-testid="stSidebar"] [role="radiogroup"] > label:hover {
        background-color: rgba(255,255,255,0.04);
    }

    div[data-testid="stTabs"] button[role="tab"] {
        font-size: 0.95rem;
        font-weight: 500;
        padding: 0.6rem 1.2rem;
    }
    div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        color: #60a5fa;
        border-bottom-color: #60a5fa;
    }

    div[data-testid="stMetric"] {
        background-color: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
        padding: 0.85rem 1rem;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: #94a3b8;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.55rem;
        font-weight: 600;
    }

    h1 { font-weight: 600; letter-spacing: -0.02em; }
    h2 { font-weight: 600; letter-spacing: -0.01em; margin-top: 0.5rem; }
    h3 {
        font-weight: 600;
        font-size: 1.1rem;
        color: #e5e7eb;
        text-transform: none;
        margin-top: 1.5rem;
        padding-bottom: 0.35rem;
        border-bottom: 1px solid rgba(255,255,255,0.08);
    }

    .pl-section-label {
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #6b7280;
        margin: 1.1rem 0 0.4rem 0.25rem;
    }

    div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
    }

    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
    }

    section[data-testid="stSidebar"] .stButton > button {
        text-align: left;
        justify-content: flex-start;
        font-size: 0.85rem;
        padding: 0.35rem 0.65rem;
        margin-bottom: 1px;
        min-height: 0;
        border: 1px solid transparent;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="tertiary"] {
        background-color: transparent;
        color: #cbd5e1;
    }
    section[data-testid="stSidebar"] .stButton > button[kind="tertiary"]:hover {
        background-color: rgba(255,255,255,0.05);
        color: #fff;
        border-color: rgba(255,255,255,0.08);
    }
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
        background-color: rgba(96,165,250,0.15);
        color: #93c5fd;
        border-color: rgba(96,165,250,0.35);
    }

    .page-header {
        display: flex;
        align-items: baseline;
        gap: 0.75rem;
        margin-bottom: 0.25rem;
    }
    .page-header-title {
        font-size: 1.9rem;
        font-weight: 600;
        letter-spacing: -0.02em;
    }
    .page-header-module {
        color: #6b7280;
        font-family: ui-monospace, SFMono-Regular, monospace;
        font-size: 0.85rem;
    }
    .page-header-comment {
        color: #9ca3af;
        margin: 0.25rem 0 1rem 0;
        max-width: 70ch;
        line-height: 1.5;
    }
</style>
"""


def inject() -> None:
    """Inject the custom stylesheet into the Streamlit page."""
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)
