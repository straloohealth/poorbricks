"""Intentionally malformed pipeline — missing fixtures.py, transform.py, test_pipeline.py.

Used by tests/test_multi_repo.py to verify that check_architecture() detects missing files.
"""

from __future__ import annotations

from pydantic import Field

from validation import Expectations, ValidatedStruct


class MalformedModel(ValidatedStruct):
    """Test model for the malformed pipeline fixture."""

    id: str = Field(description="Record identifier.")


class MalformedExpectations(Expectations):
    MIN_ROWS = 1
    UNIQUE_KEYS = [["id"]]
