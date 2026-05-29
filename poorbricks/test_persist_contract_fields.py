"""Tests for contract field enrichment: descriptions (req 4) + is_literal (req 5)."""

from __future__ import annotations

from pydantic import Field

from poorbricks.persist import _flatten_fields
from validation import ValidatedStruct


def test_field_description_flows_into_contract_fields() -> None:
    class M(ValidatedStruct):
        patient_id: str = Field(description="Stable de-identified key")
        name: str | None

    by = {f["name"]: f for f in _flatten_fields(M.to_struct().jsonValue())}
    assert by["patient_id"]["description"] == "Stable de-identified key"
    # A field with no description carries no description key (not an empty one).
    assert "description" not in by["name"]


def test_is_literal_flag_marks_only_literal_columns() -> None:
    class M(ValidatedStruct):
        id: str
        source_system: str

    by = {
        f["name"]: f
        for f in _flatten_fields(
            M.to_struct().jsonValue(), literal_columns={"source_system"}
        )
    }
    assert by["source_system"]["is_literal"] is True
    assert "is_literal" not in by["id"]
