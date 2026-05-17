"""Tests for poorbricks.airflow.dag_store.LocalDagStore (filesystem)."""

from __future__ import annotations

from pathlib import Path

from poorbricks.airflow.dag_store import LocalDagStore


def test_put_creates_files(tmp_path: Path) -> None:
    store = LocalDagStore(root=tmp_path)
    store.put("repo_a", "wf1", "print('a')")
    store.put("repo_a", "wf2", "print('b')")
    assert (tmp_path / "repo_a" / "wf1.py").read_text() == "print('a')"
    assert (tmp_path / "repo_a" / "wf2.py").read_text() == "print('b')"


def test_list_returns_sorted_stems(tmp_path: Path) -> None:
    store = LocalDagStore(root=tmp_path)
    store.put("r", "b", "x")
    store.put("r", "a", "x")
    assert store.list_dags("r") == ["a", "b"]


def test_list_empty_when_prefix_missing(tmp_path: Path) -> None:
    store = LocalDagStore(root=tmp_path)
    assert store.list_dags("never") == []


def test_prune_removes_unknown(tmp_path: Path) -> None:
    store = LocalDagStore(root=tmp_path)
    store.put("r", "keep", "x")
    store.put("r", "drop1", "x")
    store.put("r", "drop2", "x")
    deleted = store.prune("r", keep={"keep"})
    assert deleted == ["drop1", "drop2"]
    assert store.list_dags("r") == ["keep"]


def test_prune_isolated_per_prefix(tmp_path: Path) -> None:
    store = LocalDagStore(root=tmp_path)
    store.put("repo_a", "wfa", "x")
    store.put("repo_b", "wfb", "x")
    # Prune repo_a with empty keep set — must not touch repo_b.
    deleted = store.prune("repo_a", keep=set())
    assert deleted == ["wfa"]
    assert store.list_dags("repo_a") == []
    assert store.list_dags("repo_b") == ["wfb"]
