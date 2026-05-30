"""Tests for contract field enrichment: descriptions (req 4) + is_literal (req 5)."""

from __future__ import annotations

from pydantic import Field

from poorbricks.persist import _flatten_fields, _generate_description
from validation import ValidatedStruct


def test_authored_description_wins_and_is_not_flagged_generated() -> None:
    class M(ValidatedStruct):
        patient_id: str = Field(description="Stable de-identified key")
        name: str | None

    by = {f["name"]: f for f in _flatten_fields(M.to_struct().jsonValue())}
    # The authored Field(description=) flows through verbatim and is NOT flagged.
    assert by["patient_id"]["description"] == "Stable de-identified key"
    assert "description_generated" not in by["patient_id"]


def test_missing_description_is_auto_filled_and_flagged() -> None:
    class M(ValidatedStruct):
        patient_id: str = Field(description="Stable de-identified key")
        name: str | None

    by = {f["name"]: f for f in _flatten_fields(M.to_struct().jsonValue())}
    # A field with no authored description gets a generated one, flagged so
    # cosmo/LLMs know it is heuristic and a later authored description wins.
    assert by["name"]["description"]
    assert by["name"]["description_generated"] is True


def test_auto_description_uses_lineage_provenance() -> None:
    class M(ValidatedStruct):
        is_active: bool
        cost_usd: float

    lineage = {
        "columns": {
            "is_active": {
                "sources": [{"table": "smith.navigators", "column": "active"}]
            },
        }
    }
    by = {
        f["name"]: f
        for f in _flatten_fields(M.to_struct().jsonValue(), lineage=lineage)
    }
    # Boolean intent + lineage provenance.
    assert by["is_active"]["description"].startswith("Whether active.")
    assert "smith.navigators.active" in by["is_active"]["description"]
    # Monetary heuristic, no lineage → no provenance clause.
    assert "monetary amount" in by["cost_usd"]["description"]
    assert "Derived from" not in by["cost_usd"]["description"]


def test_generate_description_heuristics() -> None:
    assert _generate_description("id", "string", False, None) == (
        "Unique identifier for the row."
    )
    assert _generate_description("patient_id", "string", False, None).startswith(
        "Identifier of the patient"
    )
    assert _generate_description("created_at", "timestamp", False, None).startswith(
        "Created at (timestamp)"
    )
    assert _generate_description("source_system", "string", True, None) == (
        "Constant literal value (string) stamped on every row."
    )


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
