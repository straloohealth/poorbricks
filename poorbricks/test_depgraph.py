"""Unit tests for poorbricks.depgraph."""

from __future__ import annotations

import pytest

from poorbricks.depgraph import CycleError, topological_order


def test_topological_order_orders_predecessors_first() -> None:
    graph = {
        "bronze": set(),
        "dim": {"bronze"},
        "fact": {"dim", "bronze"},
    }
    order = topological_order(graph)
    assert order.index("bronze") < order.index("dim") < order.index("fact")


def test_topological_order_is_deterministic() -> None:
    graph = {"a": set(), "b": set(), "c": {"a", "b"}}
    assert topological_order(graph) == ["a", "b", "c"]


def test_topological_order_raises_on_cycle() -> None:
    graph = {"a": {"b"}, "b": {"a"}}
    with pytest.raises(CycleError) as exc:
        topological_order(graph)
    assert sorted(exc.value.nodes) == ["a", "b"]


def test_topological_order_empty_graph() -> None:
    assert topological_order({}) == []
