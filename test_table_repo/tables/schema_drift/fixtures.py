"""Fixtures for schema_drift — verifies local mode detects model/contract field mismatch."""

from __future__ import annotations

from poorbricks import scenario
from tables.schema_drift.pipeline import SchemaDriftInputs


@scenario("nominal")
def nominal() -> SchemaDriftInputs:
    rows = [{"mongo_id": "507f1f77bcf86cd799439011", "nonexistent_local_field": "val"}]
    return SchemaDriftInputs.from_rows({"smith_users": rows})
