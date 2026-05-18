"""Fixtures producing a DataFrame that violates UNIQUE_KEYS."""

from __future__ import annotations

from poorbricks import scenario
from tables.expectations_failure.pipeline import ExpectationsFailureInputs


@scenario("nominal")
def nominal() -> ExpectationsFailureInputs:
    rows = [{"user_id": "u1"}, {"user_id": "u1"}]
    return ExpectationsFailureInputs.from_rows({"upstream": rows})
