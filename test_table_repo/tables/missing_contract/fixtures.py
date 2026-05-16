"""Fixtures for missing_contract — verifies local mode detects absent contracts."""

from __future__ import annotations

from poorbricks import scenario
from tables.missing_contract.pipeline import MissingContractInputs


@scenario("nominal")
def nominal() -> MissingContractInputs:
    rows = [{"user_id": "u1"}]
    return MissingContractInputs.from_rows({"ghost": rows})
