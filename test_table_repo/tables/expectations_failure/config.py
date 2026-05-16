"""Scenario: pipeline output passes schema but violates Expectations (MIN_ROWS)."""

from __future__ import annotations

from pydantic import Field

from validation import Expectations, NotNullRule, ValidatedStruct, ValidationRule


class TinyUpstream(ValidatedStruct):
    """Tiny upstream model used only to anchor the TableSource for this scenario."""

    user_id: str = Field(description="User id")


class ExpectationsFailure(ValidatedStruct):
    user_id: str = Field(description="User id")

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [NotNullRule(column="user_id")]


class ExpectationsFailureExpectations(Expectations):
    MIN_ROWS = 999_999  # impossible to satisfy in fixtures mode
    UNIQUE_KEYS = [["user_id"]]
