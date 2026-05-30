"""Unit tests for run-history context + best-effort recording (no live DB)."""

from __future__ import annotations

import pytest

from poorbricks import persist
from poorbricks.run_history import run_context


def test_run_context_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POORBRICKS_ENV", raising=False)
    monkeypatch.delenv("POORBRICKS_SHA", raising=False)
    monkeypatch.delenv("GIT_SHA", raising=False)
    assert run_context() == ("unknown", None)


def test_run_context_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POORBRICKS_ENV", "dev")
    monkeypatch.setenv("POORBRICKS_SHA", "abc1234")
    assert run_context() == ("dev", "abc1234")


def test_run_context_falls_back_to_git_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POORBRICKS_SHA", raising=False)
    monkeypatch.setenv("GIT_SHA", "fromgit")
    monkeypatch.setenv("POORBRICKS_ENV", "ci")
    assert run_context() == ("ci", "fromgit")


def test_schema_hash_is_stable_and_sensitive() -> None:
    a = {"fields": [{"name": "x", "type": "string"}]}
    b = {"fields": [{"name": "x", "type": "long"}]}
    assert persist._schema_hash(a) == persist._schema_hash(a)
    assert persist._schema_hash(a) != persist._schema_hash(b)


def test_safe_record_swallows_store_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Instrumentation must never raise into a pipeline run."""

    class _BoomStore:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def record(self, rec: object) -> int:
            raise RuntimeError("meta-store down")

    monkeypatch.setattr("poorbricks.run_history.RunHistoryStore", _BoomStore)
    # Should not raise despite the store blowing up.
    persist._safe_record(object())
    assert "record failed" in capsys.readouterr().out
