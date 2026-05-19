"""Named scenarios for verifying analytics.bronze.smith_navigators."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.bronze.smith.navigators.pipeline import SmithNavigatorsInputs

_NOW = datetime(2026, 5, 7, 12, 0, 0)


@scenario("empty")
def empty() -> SmithNavigatorsInputs:
    """Empty upstream — confirms the bronze writer tolerates no rows."""
    return SmithNavigatorsInputs.from_rows({"upstream": []})


@scenario("smoke")
def smoke() -> SmithNavigatorsInputs:
    """Single representative active navigator."""
    return SmithNavigatorsInputs.from_rows(
        {
            "upstream": [
                {
                    "navigator_id": "507f1f77bcf86cd799439012",
                    "name": "Maria Silva",
                    "role": "navigator",
                    "is_active": True,
                    "started_at": _NOW,
                    "email": "maria.silva@straloo.com.br",
                    "phone": "+5585988887777",
                    "org": "straloo",
                    "groups": ["navigator"],
                }
            ]
        }
    )
