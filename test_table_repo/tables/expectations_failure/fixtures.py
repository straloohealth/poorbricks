"""Fixtures producing a small DataFrame — guaranteed under MIN_ROWS."""

from __future__ import annotations

from poorbricks import scenario
from tables.expectations_failure.pipeline import ExpectationsFailureInputs


@scenario("nominal")
def nominal() -> ExpectationsFailureInputs:
    rows = [{"user_id": "u1"}, {"user_id": "u2"}]
    return ExpectationsFailureInputs.from_rows({"upstream": rows})
