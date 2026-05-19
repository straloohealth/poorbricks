"""Schema (data contract) for analytics.bronze.smith_navigators.

Mirrors poorbricks_dev.master.navigators — the navigator-identity table
sourced from MongoDB mongo_smith.navigators — into the medallion-Postgres
bronze layer.
"""

from datetime import datetime

from pydantic import Field

from validation import (
    Expectations,
    StringLengthRule,
    ValidatedStruct,
    ValidationRule,
)

SMITH_NAVIGATORS_BRONZE_TABLE_NAME = "smith_navigators"


class SmithNavigatorBronze(ValidatedStruct):
    """Navigator identity master, sourced from the Smith navigator collection.

    One row per navigator (clinical / operational user). Mirror of
    poorbricks_dev.master.navigators into the Postgres bronze schema.
    """

    navigator_id: str | None = Field(
        default=None,
        description="Unique navigator identifier (Mongo ObjectId from mongo_smith.navigators).",
    )
    name: str | None = Field(description="Full name of the navigator.")
    role: str | None = Field(
        description="Navigator role/title (e.g. care manager, coordinator)."
    )
    is_active: bool | None = Field(
        description="Whether the navigator is currently active."
    )
    started_at: datetime | None = Field(
        description="When the navigator joined the program."
    )
    email: str | None = Field(description="Work email address of the navigator.")
    phone: str | None = Field(description="Phone number of the navigator.")
    org: str | None = Field(
        description="Organization the navigator belongs to (Straloo internal partition)."
    )
    groups: list[str] | None = Field(
        description="Permission groups the navigator belongs to (admin / navigator / dashviewer)."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            StringLengthRule(column="navigator_id", min_length=1, max_length=255),
        ]


class SmithNavigatorBronzeExpectations(Expectations):
    """Production-derived expectations for analytics.bronze.smith_navigators.

    Floor inherited from poorbricks_dev.master.navigators (≥300 rows on
    2026-05-07).
    """

    MIN_ROWS = 300
    UNIQUE_KEYS = [["navigator_id"]]
    NON_NULL_COLUMNS = []
