"""Schema (data contract) for bronze.example_items.

Minimal example table used to exercise the framework's pipeline
mechanics in integration tests. Not tied to any production data source.
"""

from datetime import datetime

from pydantic import Field

from validation import (
    Expectations,
    ValidatedStruct,
    ValidationRule,
)

EXAMPLE_ITEMS_TABLE_NAME = "example_items"


class Item(ValidatedStruct):
    """Example item record for framework integration testing."""

    item_id: str = Field(description="Unique identifier for the item.")
    name: str | None = Field(default=None, description="Display name of the item.")
    active: bool | None = Field(default=None, description="Whether the item is active.")
    created_at: datetime | None = Field(
        default=None, description="Timestamp when the item was created."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return []


class ItemExpectations(Expectations):
    MIN_ROWS = 1
    UNIQUE_KEYS = [["item_id"]]
    NON_NULL_COLUMNS = ["item_id"]
    NULL_RATE_MAX: dict[str, float] = {}
    ENUM_VALUES: dict[str, list[str]] = {}
