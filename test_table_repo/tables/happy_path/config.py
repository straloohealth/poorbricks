"""Happy-path scenario: contract exists and schema matches."""

from __future__ import annotations

from pydantic import Field

from validation import Expectations, NotNullRule, ValidatedStruct, ValidationRule


class HappyPath(ValidatedStruct):
    user_id: str = Field(description="User id passed through from smith.users")
    is_active: bool = Field(description="Whether user is currently active")

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [NotNullRule(column="user_id")]


class HappyPathExpectations(Expectations):
    MIN_ROWS = 1
    UNIQUE_KEYS = [["user_id"]]
    NON_NULL_COLUMNS = ["user_id"]
