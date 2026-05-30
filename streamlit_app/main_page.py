"""The condensed "Main" page: alerts, a lineage navigator, and table detail.

Three stacked sections, top to bottom:

1. **Alerts grouped by severity** — errors then warnings (one line each),
   with a small metric row of counts and an all-clear note when empty.
2. **Lineage navigator** — an interactive table-to-table DAG built from the
   ``inputs`` of every published contract. Clicking a node highlights the
   selection (red), all of its upstream sources/ancestors (blue), all of its
   downstream destinations/descendants (green), and de-emphasises everything
   else; only edges lying on a highlighted path are emphasised.
3. **Table detail** — the shared :mod:`streamlit_app.table_detail` component
   rendered for whichever table is currently selected.

Every external read degrades gracefully; the page never raises when MongoDB,
Postgres, the run-history store, or Airflow are unavailable.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app import cache, table_detail

# Session-state key holding the table the user last clicked / selected.
_SELECTED_KEY = "lineage_selected"

# Highlight palette for the lineage navigator.
_COLOR_SELECTED = "#ef4444"  # red — the clicked node
_COLOR_SOURCE = "#3b82f6"  # blue — upstream ancestors
_COLOR_DEST = "#22c55e"  # green — downstream descendants
_COLOR_IDLE = "#374151"  # grey — everything else / no selection
_COLOR_EDGE_IDLE = "#374151"
_COLOR_EDGE_HOT = "#f8fafc"


def render() -> None:
    """Render the Main page (theme injection is handled by ``app.py``)."""
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Main</span>"
        "<span class='page-header-module'>alerts · lineage · table detail</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True):
            cache.clear()
            st.rerun()

    _render_alerts(environment="prod")
    st.divider()
    selected = _render_lineage_navigator()
    st.divider()
    _render_table_detail(selected, environment="prod")


# --------------------------------------------------------------------------- #
# 1. Alerts grouped by severity
# --------------------------------------------------------------------------- #
def _alert_line(alert: dict[str, Any]) -> str:
    """One-line ``kind — pipeline_key: summary`` rendering for an alert."""
    kind = alert.get("kind", "alert")
    key = alert.get("pipeline_key", "?")
    summary = alert.get("summary") or ""
    line = f"{kind} — {key}"
    if summary:
        line += f": {summary}"
    return line


def _render_alerts(environment: str) -> None:
    st.subheader("Alerts")

    try:
        runtime = cache.cached_alerts(environment=environment)
    except Exception:
        runtime = {"error": [], "warn": [], "info": []}
    try:
        verify = cache.cached_verification_findings()
    except Exception:
        verify = {"error": [], "warn": [], "info": []}

    # Combined severity counts across runtime alerts + contract verifications.
    n_err = len(runtime.get("error") or []) + len(verify.get("error") or [])
    n_warn = len(runtime.get("warn") or []) + len(verify.get("warn") or [])
    n_info = len(runtime.get("info") or []) + len(verify.get("info") or [])
    cols = st.columns(3)
    cols[0].metric("Errors", n_err)
    cols[1].metric("Warnings", n_warn)
    cols[2].metric("Info", n_info)

    # 1. Runtime alerts (from run history + staleness).
    st.markdown(
        "**Runtime** — failures · row-count anomalies · regressions · staleness"
    )
    r_err, r_warn, r_info = (
        runtime.get("error") or [],
        runtime.get("warn") or [],
        runtime.get("info") or [],
    )
    if not (r_err or r_warn or r_info):
        st.success(f"No runtime alerts in `{environment}`.")
    for alert in r_err:
        st.error(_alert_line(alert))
    for alert in r_warn:
        st.warning(_alert_line(alert))
    for alert in r_info:
        st.info(_alert_line(alert))

    # 2. Contract verification findings (stubs / literals / contract breaks).
    st.markdown("**Verification** — stub columns · literals · contract breaks")
    v_err, v_warn, v_info = (
        verify.get("error") or [],
        verify.get("warn") or [],
        verify.get("info") or [],
    )
    if not (v_err or v_warn or v_info):
        st.success("No contract verification findings.")
    for alert in v_err:
        st.error(_alert_line(alert))
    for alert in v_warn:
        st.warning(_alert_line(alert))
    with st.expander(f"Literal columns ({len(v_info)})", expanded=False):
        if v_info:
            for alert in v_info:
                st.info(_alert_line(alert))
        else:
            st.caption("none")


# --------------------------------------------------------------------------- #
# 2. Lineage navigator
# --------------------------------------------------------------------------- #
def _build_edges(
    contracts: list[dict[str, Any]],
) -> tuple[set[str], dict[str, str], list[tuple[str, str]]]:
    """Build the upstream->table graph from contract ``inputs``.

    Delegates to the canonical :func:`utils.lineage.build_lineage_graph`, which
    resolves *every* upstream kind — ``ContractSource``/``TableSource`` (other
    poorbricks tables) as well as ``MongoSource``/``PostgresTableSource`` (raw
    external source leaves) — so a table's full source chain shows, not only the
    contract-backed parents. Returns ``(node_ids, labels, edges)``.
    """
    from utils.lineage import build_lineage_graph

    lin_nodes, lin_edges = build_lineage_graph(contracts)
    labels = {n.id: n.label for n in lin_nodes}
    node_ids = set(labels)
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for e in lin_edges:
        edge = (e.source, e.target)
        if edge not in seen:
            seen.add(edge)
            edges.append(edge)
    return node_ids, labels, edges


def _ancestors(node: str, edges: list[tuple[str, str]]) -> set[str]:
    """All nodes that (transitively) flow *into* ``node`` (its upstreams)."""
    parents: dict[str, list[str]] = {}
    for src, dst in edges:
        parents.setdefault(dst, []).append(src)
    seen: set[str] = set()
    stack = list(parents.get(node, []))
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(parents.get(cur, []))
    return seen


def _descendants(node: str, edges: list[tuple[str, str]]) -> set[str]:
    """All nodes that ``node`` (transitively) flows *into* (its downstreams)."""
    children: dict[str, list[str]] = {}
    for src, dst in edges:
        children.setdefault(src, []).append(dst)
    seen: set[str] = set()
    stack = list(children.get(node, []))
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(children.get(cur, []))
    return seen


def _render_lineage_navigator() -> str | None:
    """Render the interactive DAG and return the currently selected table."""
    from streamlit_agraph import Config, Edge, Node, agraph

    st.subheader("Lineage navigator")

    try:
        contracts = cache.cached_contract_details()
    except Exception as exc:  # noqa: BLE001
        st.warning(
            f"Lineage unavailable — could not load contracts: "
            f"`{type(exc).__name__}`: {exc}"
        )
        return st.session_state.get(_SELECTED_KEY)

    node_ids, labels, edges = _build_edges(contracts)
    if not node_ids:
        st.info("No contracts found — nothing to graph yet.")
        return st.session_state.get(_SELECTED_KEY)

    selected = st.session_state.get(_SELECTED_KEY)
    if selected not in node_ids:
        selected = None

    # Compute highlight sets from the current selection (recomputed every run).
    sources: set[str] = set()
    dests: set[str] = set()
    if selected is not None:
        sources = _ancestors(selected, edges)
        dests = _descendants(selected, edges)

    def _color(node_id: str) -> str:
        if selected is None:
            return _COLOR_IDLE
        if node_id == selected:
            return _COLOR_SELECTED
        if node_id in sources:
            return _COLOR_SOURCE
        if node_id in dests:
            return _COLOR_DEST
        return _COLOR_IDLE

    # An edge is "hot" if both endpoints lie on a highlighted path through the
    # selected node (selection<->ancestors, or selection<->descendants).
    hot_up = sources | ({selected} if selected else set())
    hot_down = dests | ({selected} if selected else set())

    def _edge_hot(src: str, dst: str) -> bool:
        if selected is None:
            return False
        return (src in hot_up and dst in hot_up) or (
            src in hot_down and dst in hot_down
        )

    nodes = [
        Node(
            id=nid, label=labels.get(nid, nid), color=_color(nid), size=18, shape="dot"
        )
        for nid in sorted(node_ids)
    ]
    agraph_edges = [
        Edge(
            source=src,
            target=dst,
            color=_COLOR_EDGE_HOT if _edge_hot(src, dst) else _COLOR_EDGE_IDLE,
        )
        for src, dst in edges
    ]
    config = Config(
        width=1000,
        height=560,
        directed=True,
        hierarchical=True,
        physics=False,
        collapsible=False,
        nodeHighlightBehavior=False,
        highlightColor=_COLOR_EDGE_HOT,
    )

    clicked = agraph(nodes=nodes, edges=agraph_edges, config=config)
    if clicked and clicked in node_ids:
        st.session_state[_SELECTED_KEY] = clicked
        if clicked != selected:
            # Re-render with the new selection's highlight colours applied.
            st.rerun()
        selected = clicked

    _render_legend()
    st.caption(
        f"{len(node_ids)} tables · {len(edges)} dependencies · "
        "click a node to highlight its sources (blue) and destinations (green)"
    )

    # Selectbox fallback (agraph clicks can be finicky).
    options = ["—"] + sorted(node_ids)
    default_index = options.index(selected) if selected in options else 0
    picked = st.selectbox(
        "Inspect table",
        options,
        index=default_index,
        help="Pick a table to highlight in the graph and show its detail below.",
    )
    if picked != "—" and picked != selected:
        st.session_state[_SELECTED_KEY] = picked
        st.rerun()

    return st.session_state.get(_SELECTED_KEY) if selected != "—" else None


def _render_legend() -> None:
    swatches = [
        ("selected", _COLOR_SELECTED),
        ("source (upstream)", _COLOR_SOURCE),
        ("destination (downstream)", _COLOR_DEST),
        ("other", _COLOR_IDLE),
    ]
    chips = "".join(
        f"<span style='display:inline-flex;align-items:center;gap:0.35rem;"
        f"margin-right:1rem;font-size:0.8rem;color:#9ca3af;'>"
        f"<span style='width:0.7rem;height:0.7rem;border-radius:50%;"
        f"background:{color};display:inline-block;'></span>{label}</span>"
        for label, color in swatches
    )
    st.markdown(
        f"<div style='margin:0.25rem 0 0.5rem 0;'>{chips}</div>",
        unsafe_allow_html=True,
    )


# --------------------------------------------------------------------------- #
# 3. Table detail
# --------------------------------------------------------------------------- #
def _render_table_detail(selected: str | None, environment: str) -> None:
    if not selected:
        st.info("Select a table in the lineage navigator to see its details.")
        return
    try:
        table_detail.render(selected, environment=environment)
    except Exception as exc:  # noqa: BLE001 - never let detail break the page
        st.error(
            f"Could not render detail for `{selected}`: `{type(exc).__name__}`: {exc}"
        )


__all__ = ["render"]
