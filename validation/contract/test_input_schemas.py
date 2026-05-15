"""Integration contract tests: verify declared ContractSource schemas exist in MongoDB.

Requires MONGO_URI in .env or environment variable.

Run:
    poetry run pytest validation/contract/ -m integration -v
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import StructType

from framework.discovery import discover_all_pipelines
from framework.inputs import ContractSource
from framework.registry import all_pipelines
from utils.contracts import fetch_contract


@pytest.mark.integration
def test_contracts_registered() -> None:
    """For every ContractSource(table_name) in every pipeline, verify the contract
    exists in MongoDB and its schema is non-empty."""
    discover_all_pipelines()

    errors: list[str] = []
    for registry_key, meta in all_pipelines().items():
        for attr_name, source in meta.inputs_cls.sources().items():
            if not isinstance(source, ContractSource):
                continue

            try:
                contract = fetch_contract(source.table_name)
                schema = StructType.fromJson(contract["schema_json"])
                if not schema.fields:
                    errors.append(
                        f"[{registry_key}] {attr_name}: schema is empty for "
                        f"{source.table_name!r}"
                    )
            except KeyError as exc:
                errors.append(f"[{registry_key}] {attr_name}: {exc}")

    assert not errors, "\n".join(errors)
