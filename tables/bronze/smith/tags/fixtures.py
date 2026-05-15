"""Named scenarios for verifying analytics.bronze.smith_tags."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.bronze.smith.tags.pipeline import SmithTagsInputs

_NOW = datetime(2026, 5, 7, 12, 0, 0)


@scenario("empty")
def empty() -> SmithTagsInputs:
    """Empty upstream — confirms the bronze writer tolerates no rows."""
    return SmithTagsInputs.from_rows({"upstream": []})


@scenario("smoke")
def smoke() -> SmithTagsInputs:
    """Two tag assignments for the same patient."""
    return SmithTagsInputs.from_rows(
        {
            "upstream": [
                {
                    "patient_id": "patient-001",
                    "tags_name": "high_risk",
                    "disabled": False,
                    "created_at": _NOW,
                },
                {
                    "patient_id": "patient-001",
                    "tags_name": "churned",
                    "disabled": True,
                    "created_at": _NOW,
                },
            ]
        }
    )
