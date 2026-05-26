"""Tests for utils.mongo_sample — $sample random sampling with a fake client."""

from __future__ import annotations

from typing import Any

import pytest

from utils.mongo_sample import EmptyCollectionError, sample_random_docs


class _FakeCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def aggregate(
        self, pipeline: list[dict[str, Any]], allowDiskUse: bool = False
    ) -> list[dict[str, Any]]:
        size = pipeline[0]["$sample"]["size"]
        return list(self._docs[:size])


class _FakeDb:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def __getitem__(self, _name: str) -> _FakeCollection:
        return _FakeCollection(self._docs)


class _FakeClient:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs
        self.closed = False

    def __getitem__(self, _name: str) -> _FakeDb:
        return _FakeDb(self._docs)

    def close(self) -> None:
        self.closed = True


def _patch_client(monkeypatch: pytest.MonkeyPatch, docs: list[dict[str, Any]]) -> None:
    import pymongo

    monkeypatch.setattr(pymongo, "MongoClient", lambda uri: _FakeClient(docs))


def test_sample_random_docs_honors_size(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_client(monkeypatch, [{"_id": i, "x": i} for i in range(10)])
    out = sample_random_docs("mongodb://x", "db", "coll", sample_size=3)
    assert len(out) == 3


def test_sample_random_docs_dedupes_by_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_client(monkeypatch, [{"_id": 1}, {"_id": 1}, {"_id": 2}])
    out = sample_random_docs("mongodb://x", "db", "coll", sample_size=10)
    assert len(out) == 2


def test_empty_collection_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_client(monkeypatch, [])
    with pytest.raises(EmptyCollectionError):
        sample_random_docs("mongodb://x", "db", "coll", sample_size=10)
