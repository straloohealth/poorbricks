"""Scenario: references a ContractSource whose contract is not published."""

from __future__ import annotations

from pydantic import Field

from validation import Expectations, ValidatedStruct


class MissingContract(ValidatedStruct):
    user_id: str = Field(description="User id")


class MissingContractExpectations(Expectations):
    MIN_ROWS = 1
