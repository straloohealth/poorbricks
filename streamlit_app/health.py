"""Health panel — ranked findings from :mod:`utils.diagnostics`.

Surfaces ghost contracts / orphan silvers / literal-NULL columns / soft
PKs / weak contracts / missing freshness / empty row counts as a ranked
human-readable list. Click a finding to jump to the affected node on
the Lineage page (state is carried through ``st.session_state``).

A self-contained page registered alongside Contracts / Lineage /
Postgres status in :mod:`streamlit_app.app`.
"""

from __future__ import annotations

from collections import Counter, defaultdict

import streamlit as st

from utils.diagnostics import Severity, collect_findings

_SEVERITY_BADGE = {
    Severity.ERROR: ("🔴", "Erro"),
    Severity.WARNING: ("🟡", "Aviso"),
    Severity.INFO: ("⚪", "Info"),
}


def _severity_color(sev: Severity) -> str:
    return {
        Severity.ERROR: "#dc2626",
        Severity.WARNING: "#f59e0b",
        Severity.INFO: "#6b7280",
    }[sev]


def render() -> None:
    """Render the Health page."""
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Health</span>"
        "<span class='page-header-module'>contract & lineage findings</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True, key="health_refresh"):
            st.cache_data.clear()
            st.rerun()

    findings = _cached_findings()

    if not findings:
        st.success("✓ Sem achados. Todas as verificações passaram.")
        return

    # Top-line counters per severity.
    counts: Counter[Severity] = Counter(f.severity for f in findings)
    col_e, col_w, col_i = st.columns(3)
    col_e.metric("Erros", counts.get(Severity.ERROR, 0))
    col_w.metric("Avisos", counts.get(Severity.WARNING, 0))
    col_i.metric("Info", counts.get(Severity.INFO, 0))

    # Per-check counters (compact horizontal chip strip).
    by_check: Counter[str] = Counter(f.check for f in findings)
    if by_check:
        chips = "  ·  ".join(f"`{c}` ({n})" for c, n in by_check.most_common())
        st.caption(chips)

    # Severity filter.
    sev_filter = st.multiselect(
        "Filtrar por severidade",
        options=[s.value for s in Severity],
        default=[Severity.ERROR.value, Severity.WARNING.value],
        key="health_sev_filter",
    )

    by_severity: dict[Severity, list] = defaultdict(list)
    for f in findings:
        if f.severity.value in sev_filter:
            by_severity[f.severity].append(f)

    for sev in (Severity.ERROR, Severity.WARNING, Severity.INFO):
        bucket = by_severity.get(sev, [])
        if not bucket:
            continue
        emoji, label = _SEVERITY_BADGE[sev]
        st.markdown(
            f"### {emoji} {label} ({len(bucket)})",
            unsafe_allow_html=True,
        )
        for finding in bucket:
            with st.container(border=True):
                col_table, col_check = st.columns([2, 1])
                col_table.markdown(f"**`{finding.table}`**")
                col_check.markdown(
                    f"<span style='color:{_severity_color(sev)}'>"
                    f"`{finding.check}`</span>",
                    unsafe_allow_html=True,
                )
                st.write(finding.message)
                if finding.details:
                    with st.expander("Detalhes"):
                        st.json(finding.details)
                if st.button(
                    "Ver na linhagem →",
                    key=f"jump_{finding.check}_{finding.table}",
                    use_container_width=False,
                ):
                    st.session_state["lineage_selected_node"] = finding.table
                    st.switch_page("Lineage")


@st.cache_data(ttl=60)
def _cached_findings() -> list:
    """Cache findings for 60s so navigating between filters is snappy."""
    return collect_findings()
