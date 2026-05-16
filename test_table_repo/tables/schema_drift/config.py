"""Scenario: local TableSource model has fields that don't exist in the published contract."""

from __future__ import annotations

from pydantic import Field

from validation import Expectations, ValidatedStruct


class LocalSmithUsers(ValidatedStruct):
    """Locally-declared expectation of smith.users schema.

    Intentionally includes ``nonexistent_local_field`` that is NOT in the
    real bronze smith.users contract — verify --mode local must catch this.
    """

    mongo_id: str | None = Field(description="MongoDB ObjectId")
    nonexistent_local_field: str = Field(
        description="Field declared locally that does not exist in the published contract."
    )


class SchemaDrift(ValidatedStruct):
    out: str = Field(description="placeholder output field")


class SchemaDriftExpectations(Expectations):
    MIN_ROWS = 1
    UNIQUE_KEYS = [["out"]]
