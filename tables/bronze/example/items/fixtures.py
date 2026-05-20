"""Named scenarios for verifying bronze.example_items."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.bronze.example.items.pipeline import ItemInputs

_NOW = datetime(2026, 1, 1, 12, 0, 0)


@scenario("empty")
def empty() -> ItemInputs:
    """Empty upstream — confirms the bronze writer tolerates no rows."""
    return ItemInputs.from_rows({"upstream": []})


@scenario("smoke")
def smoke() -> ItemInputs:
    """Two representative items covering active and inactive states."""
    return ItemInputs.from_rows(
        {
            "upstream": [
                {
                    "item_id": "item-001",
                    "name": "Widget Alpha",
                    "active": True,
                    "created_at": _NOW,
                },
                {
                    "item_id": "item-002",
                    "name": "Widget Beta",
                    "active": False,
                    "created_at": _NOW,
                },
            ]
        }
    )
