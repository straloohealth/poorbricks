"""Dependency-graph utilities — topological ordering and cycle detection.

Pure functions over a ``node -> predecessors`` mapping; no I/O, no framework
types, so they are trivially unit-testable and reusable by both the verifier
and the DAG generator.
"""

from __future__ import annotations


class CycleError(ValueError):
    """Raised when a dependency graph contains a cycle."""

    def __init__(self, nodes: list[str]) -> None:
        super().__init__(f"dependency cycle among: {nodes}")
        self.nodes = nodes


def topological_order(graph: dict[str, set[str]]) -> list[str]:
    """Return ``graph``'s nodes in dependency order — predecessors first.

    ``graph`` maps each node to the set of nodes that must precede it. Every
    node, including pure sources, must appear as a key. Ties are broken
    alphabetically so the order is deterministic. Raises :class:`CycleError`
    naming the unresolved nodes when a cycle exists.
    """
    remaining = {node: set(preds) for node, preds in graph.items()}
    order: list[str] = []
    while remaining:
        ready = sorted(n for n, preds in remaining.items() if not preds)
        if not ready:
            raise CycleError(sorted(remaining))
        for node in ready:
            order.append(node)
            del remaining[node]
        for preds in remaining.values():
            preds.difference_update(ready)
    return order


__all__ = ["CycleError", "topological_order"]
