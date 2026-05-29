"""Tests for pipeline-removal contract reconciliation."""

from __future__ import annotations

from typing import Any

import pytest

from api.main import _external_consumers, _reconcile_contracts


def test_external_consumers_collects_other_prefix_deps() -> None:
    details = [
        {"table_name": "a1", "prefix": "repo-a"},
        {
            "table_name": "g1",
            "prefix": "repo-b",
            "inputs": [{"name": "x", "table_name": "a2"}],
            "lineage": {"consumed": {"a3": ["c"]}},
        },
        # Same-prefix consumer should be ignored.
        {
            "table_name": "a9",
            "prefix": "repo-a",
            "inputs": [{"table_name": "a1"}],
        },
    ]
    assert _external_consumers(details, "repo-a") == {"a2", "a3"}


def test_reconcile_deletes_orphans_but_keeps_consumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owned = {"a1", "a2", "a3"}
    details = [{"table_name": t, "prefix": "repo-a"} for t in owned] + [
        # repo-b still consumes a2.
        {
            "table_name": "g1",
            "prefix": "repo-b",
            "inputs": [{"table_name": "a2"}],
        }
    ]

    captured: dict[str, Any] = {}

    def _fake_prune(prefix: str, keep: set[str]) -> list[str]:
        captured["keep"] = keep
        return sorted(owned - keep)

    monkeypatch.setattr("utils.contracts.list_contract_details", lambda: details)
    monkeypatch.setattr("utils.contracts.prune_contracts", _fake_prune)

    # Upload now publishes only a1; a2 and a3 were removed from the repo.
    result = _reconcile_contracts("repo-a", keep_published={"a1"})

    # a3 is a true orphan → deleted; a2 is still consumed by repo-b → kept+warned.
    assert result["deleted"] == ["a3"]
    assert "a2" in captured["keep"]  # protected from prune
    assert any("a2" in w for w in result["warnings"])


def test_reconcile_skips_when_no_published_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("utils.contracts.list_contract_details", lambda: [])

    def _boom(*a: object, **k: object) -> list[str]:
        raise AssertionError("prune must not run with empty keep set")

    monkeypatch.setattr("utils.contracts.prune_contracts", _boom)
    result = _reconcile_contracts("repo-a", keep_published=set())
    assert result["deleted"] == []
    assert result["warnings"]
