"""Tests for the pure lineage DAG builder."""

from __future__ import annotations

from typing import Any

from utils.lineage import LineageEdge, LineageNode, build_lineage_graph


def _contract(
    table_name: str,
    level: str,
    inputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a minimal contract document for graph tests."""
    return {"table_name": table_name, "level": level, "inputs": inputs or []}


class TestBuildLineageGraph:
    """Test cases for build_lineage_graph."""

    def test_empty_contracts_yield_empty_graph(self) -> None:
        """No contracts produce no nodes and no edges."""
        nodes, edges = build_lineage_graph([])
        assert nodes == []
        assert edges == []

    def test_contract_without_inputs_is_a_lone_node(self) -> None:
        """A contract with empty inputs is a node with no edges."""
        nodes, edges = build_lineage_graph([_contract("smith_users", "bronze")])
        assert nodes == [
            LineageNode(id="smith_users", label="smith_users", kind="bronze")
        ]
        assert edges == []

    def test_contract_source_edge_between_two_contracts(self) -> None:
        """A ContractSource upstream links one contract node to another."""
        contracts = [
            _contract("smith_users", "bronze"),
            _contract(
                "dim_patient",
                "silver",
                [
                    {
                        "name": "smith_users",
                        "kind": "ContractSource",
                        "table_name": "smith_users",
                    }
                ],
            ),
        ]
        nodes, edges = build_lineage_graph(contracts)
        assert edges == [LineageEdge(source="smith_users", target="dim_patient")]
        # Both upstream and downstream keep their contract-derived kinds.
        kinds = {n.id: n.kind for n in nodes}
        assert kinds == {"smith_users": "bronze", "dim_patient": "silver"}

    def test_table_source_edge(self) -> None:
        """A TableSource upstream also produces a table-to-table edge."""
        contracts = [
            _contract("raw_events", "bronze"),
            _contract(
                "fct_events",
                "silver",
                [
                    {
                        "name": "raw_events",
                        "kind": "TableSource",
                        "table_name": "raw_events",
                        "model": "RawEvents",
                    }
                ],
            ),
        ]
        _, edges = build_lineage_graph(contracts)
        assert edges == [LineageEdge(source="raw_events", target="fct_events")]

    def test_mongo_source_becomes_external_node(self) -> None:
        """A MongoSource upstream creates a prefixed external mongo node."""
        contracts = [
            _contract(
                "smith_users",
                "bronze",
                [
                    {
                        "name": "upstream",
                        "kind": "MongoSource",
                        "db": "smith",
                        "collection": "users",
                    }
                ],
            ),
        ]
        nodes, edges = build_lineage_graph(contracts)
        assert (
            LineageNode(id="mongo:smith.users", label="smith.users", kind="mongo")
            in nodes
        )
        assert edges == [LineageEdge(source="mongo:smith.users", target="smith_users")]

    def test_postgres_source_becomes_external_node(self) -> None:
        """A PostgresTableSource upstream creates an external postgres node."""
        contracts = [
            _contract(
                "gold_metric",
                "gold",
                [
                    {
                        "name": "src",
                        "kind": "PostgresTableSource",
                        "schema_name": "silver",
                        "table": "dim_patient",
                    }
                ],
            ),
        ]
        nodes, edges = build_lineage_graph(contracts)
        assert (
            LineageNode(
                id="pg:silver.dim_patient", label="silver.dim_patient", kind="postgres"
            )
            in nodes
        )
        assert edges == [
            LineageEdge(source="pg:silver.dim_patient", target="gold_metric")
        ]

    def test_unknown_upstream_table_becomes_unknown_node(self) -> None:
        """A ContractSource pointing at a missing contract yields an unknown node."""
        contracts = [
            _contract(
                "dim_patient",
                "silver",
                [
                    {
                        "name": "ghost",
                        "kind": "ContractSource",
                        "table_name": "missing_table",
                    }
                ],
            ),
        ]
        nodes, edges = build_lineage_graph(contracts)
        assert (
            LineageNode(id="missing_table", label="missing_table", kind="unknown")
            in nodes
        )
        assert edges == [LineageEdge(source="missing_table", target="dim_patient")]

    def test_duplicate_edges_are_deduplicated(self) -> None:
        """Repeated input declarations collapse into a single edge."""
        contracts = [
            _contract("smith_users", "bronze"),
            _contract(
                "dim_patient",
                "silver",
                [
                    {
                        "name": "a",
                        "kind": "ContractSource",
                        "table_name": "smith_users",
                    },
                    {
                        "name": "b",
                        "kind": "ContractSource",
                        "table_name": "smith_users",
                    },
                ],
            ),
        ]
        _, edges = build_lineage_graph(contracts)
        assert edges == [LineageEdge(source="smith_users", target="dim_patient")]

    def test_shared_upstream_node_not_duplicated(self) -> None:
        """Two contracts reading the same source share one upstream node."""
        contracts = [
            _contract(
                "bronze_a",
                "bronze",
                [
                    {
                        "name": "u",
                        "kind": "MongoSource",
                        "db": "shared",
                        "collection": "events",
                    }
                ],
            ),
            _contract(
                "bronze_b",
                "bronze",
                [
                    {
                        "name": "u",
                        "kind": "MongoSource",
                        "db": "shared",
                        "collection": "events",
                    }
                ],
            ),
        ]
        nodes, edges = build_lineage_graph(contracts)
        mongo_nodes = [n for n in nodes if n.id == "mongo:shared.events"]
        assert len(mongo_nodes) == 1
        assert len(edges) == 2

    def test_missing_level_yields_unknown_kind(self) -> None:
        """A contract with an unrecognised level is kind 'unknown'."""
        nodes, _ = build_lineage_graph(
            [{"table_name": "weird", "level": "platinum", "inputs": []}]
        )
        assert nodes == [LineageNode(id="weird", label="weird", kind="unknown")]
