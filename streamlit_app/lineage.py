"""Lineage page: an interactive DAG of table-to-table dependencies."""

from __future__ import annotations

import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from streamlit_app import cache, contract, header
from streamlit_app.theme import LEVEL_COLORS
from utils.lineage import LineageNode, build_lineage_graph

# Colours for non-pipeline nodes (pipeline levels reuse theme.LEVEL_COLORS).
_EXTERNAL_COLORS: dict[str, str] = {
    "mongo": "#10b981",
    "postgres": "#60a5fa",
    "unknown": "#6b7280",
}
_PIPELINE_KINDS: frozenset[str] = frozenset({"bronze", "silver", "gold"})
_SELECTED_KEY: str = "lineage_selected_node"


def _node_color(kind: str) -> str:
    """Resolve a node colour from its kind."""
    return LEVEL_COLORS.get(kind) or _EXTERNAL_COLORS.get(kind, "#6b7280")


def render() -> None:
    """Render the lineage DAG page."""
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Lineage</span>"
        "<span class='page-header-module'>table dependency graph</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True):
            cache.clear()
            st.session_state.pop(_SELECTED_KEY, None)
            st.rerun()

    try:
        contracts = cache.cached_contract_details()
    except Exception as exc:
        st.error(
            f"Could not load contracts from MongoDB: `{type(exc).__name__}`: {exc}"
        )
        return

    lineage_nodes, lineage_edges = build_lineage_graph(contracts)
    if not lineage_nodes:
        st.info("No contracts found — nothing to graph.")
        return

    _render_legend()

    nodes = [
        Node(
            id=node.id,
            label=node.label,
            color=_node_color(node.kind),
            size=18,
            shape="dot",
        )
        for node in lineage_nodes
    ]
    edges = [Edge(source=edge.source, target=edge.target) for edge in lineage_edges]
    config = Config(
        width=1000,
        height=560,
        directed=True,
        hierarchical=True,
        physics=False,
        collapsible=False,
        nodeHighlightBehavior=True,
        highlightColor="#f8fafc",
    )

    clicked = agraph(nodes=nodes, edges=edges, config=config)
    if clicked:
        st.session_state[_SELECTED_KEY] = clicked

    st.caption(
        f"{len(lineage_nodes)} nodes · {len(lineage_edges)} edges · "
        "click a node to inspect it"
    )

    node_index = {node.id: node for node in lineage_nodes}
    _render_details(st.session_state.get(_SELECTED_KEY), node_index)


def _render_legend() -> None:
    """Render a colour key for the graph node kinds."""
    swatches = [
        ("bronze", _node_color("bronze")),
        ("silver", _node_color("silver")),
        ("gold", _node_color("gold")),
        ("mongo source", _node_color("mongo")),
        ("postgres source", _node_color("postgres")),
        ("no contract", _node_color("unknown")),
    ]
    chips = "".join(
        f"<span style='display:inline-flex;align-items:center;gap:0.35rem;"
        f"margin-right:1rem;font-size:0.8rem;color:#9ca3af;'>"
        f"<span style='width:0.7rem;height:0.7rem;border-radius:50%;"
        f"background:{color};display:inline-block;'></span>{label}</span>"
        for label, color in swatches
    )
    st.markdown(
        f"<div style='margin:0.25rem 0 0.75rem 0;'>{chips}</div>",
        unsafe_allow_html=True,
    )


def _render_details(selected: str | None, node_index: dict[str, LineageNode]) -> None:
    """Render the details panel for the currently selected node."""
    st.markdown("### Details")
    if not selected:
        st.info("Click a node in the graph to see its contract details.")
        return

    node = node_index.get(selected)
    if node is None:
        st.info("That node is no longer in the graph — refresh to reload.")
        return

    if node.kind == "mongo":
        st.info(
            f"**{node.label}** — external MongoDB collection. It is a raw source, "
            "so it has no published contract of its own."
        )
        return
    if node.kind == "postgres":
        st.info(
            f"**{node.label}** — external Postgres table referenced as an upstream."
        )
        return
    if node.kind not in _PIPELINE_KINDS:
        st.warning(
            f"**{node.label}** is referenced as an upstream but has no published "
            "contract. Publish it with the distributed pipeline test."
        )
        return

    try:
        contract_doc = cache.cached_contract(node.id)
    except Exception as exc:
        st.warning(
            f"Could not load the contract for `{node.id}`: "
            f"`{type(exc).__name__}`: {exc}"
        )
        return

    header.render_header(contract_doc)
    contract.render(contract_doc)
