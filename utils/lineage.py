"""Pure builder for the table-to-table lineage DAG.

Turns the ``inputs`` declarations stored on each MongoDB contract into a
directed graph of nodes and edges. Contains no I/O so it is unit-testable
without a database; the Streamlit lineage page feeds it the output of
:func:`utils.contracts.list_contract_details`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_PIPELINE_LEVELS: frozenset[str] = frozenset({"bronze", "silver", "gold"})


@dataclass(frozen=True)
class LineageNode:
    """A single node in the lineage DAG.

    ``kind`` is one of ``bronze``/``silver``/``gold`` (a pipeline table),
    ``mongo``/``postgres`` (an external upstream), or ``unknown``.
    """

    id: str
    label: str
    kind: str


@dataclass(frozen=True)
class LineageEdge:
    """A directed dependency edge: ``source`` is read by ``target``."""

    source: str
    target: str


def _table_kind(level: Any) -> str:
    """Normalise a contract ``level`` into a node kind."""
    if isinstance(level, str) and level.lower() in _PIPELINE_LEVELS:
        return level.lower()
    return "unknown"


def _upstream_node(entry: dict[str, Any]) -> LineageNode | None:
    """Resolve one ``inputs`` entry into its upstream node.

    Returns ``None`` for entries that name no resolvable upstream.
    """
    kind = entry.get("kind")
    if kind in ("ContractSource", "TableSource"):
        table_name = entry.get("table_name")
        if not table_name:
            return None
        return LineageNode(id=table_name, label=table_name, kind="unknown")
    if kind == "MongoSource":
        db = entry.get("db", "?")
        collection = entry.get("collection", "?")
        return LineageNode(
            id=f"mongo:{db}.{collection}",
            label=f"{db}.{collection}",
            kind="mongo",
        )
    if kind == "PostgresTableSource":
        schema = entry.get("schema_name", "?")
        table = entry.get("table", "?")
        return LineageNode(
            id=f"pg:{schema}.{table}",
            label=f"{schema}.{table}",
            kind="postgres",
        )
    return None


def build_lineage_graph(
    contracts: list[dict[str, Any]],
) -> tuple[list[LineageNode], list[LineageEdge]]:
    """Build the lineage DAG from a list of contract documents.

    Each contract becomes a node keyed by its ``table_name``; every entry in
    its ``inputs`` list becomes an upstream node plus a directed edge into the
    contract. ContractSource/TableSource upstreams that have no contract of
    their own are kept as ``unknown`` nodes. Nodes and edges are de-duplicated;
    a contract node always wins over an inferred ``unknown`` upstream node.

    Args:
        contracts: contract documents, e.g. from
            :func:`utils.contracts.list_contract_details`.

    Returns:
        ``(nodes, edges)`` with stable insertion order — contract nodes first,
        then discovered upstream nodes.
    """
    nodes: dict[str, LineageNode] = {}
    edges: list[LineageEdge] = []
    seen_edges: set[LineageEdge] = set()

    # Pass 1: every contract is a first-class node (kind from its level).
    for contract in contracts:
        table_name = contract.get("table_name")
        if not table_name:
            continue
        nodes[table_name] = LineageNode(
            id=table_name,
            label=table_name,
            kind=_table_kind(contract.get("level")),
        )

    # Pass 2: wire upstream nodes and edges.
    for contract in contracts:
        table_name = contract.get("table_name")
        if not table_name:
            continue
        for entry in contract.get("inputs") or []:
            upstream = _upstream_node(entry)
            if upstream is None:
                continue
            # A real contract node always wins over an inferred upstream.
            nodes.setdefault(upstream.id, upstream)
            edge = LineageEdge(source=upstream.id, target=table_name)
            if edge not in seen_edges:
                seen_edges.add(edge)
                edges.append(edge)

    return list(nodes.values()), edges


__all__ = ["LineageEdge", "LineageNode", "build_lineage_graph"]
