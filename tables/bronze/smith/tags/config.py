"""Schema (data contract) for analytics.bronze.smith_tags.

Mirrors mongo_smith.tags — patient-level tag rows as applied by the
Smith user-store. The bronze pipeline reads MongoDB directly.
"""

from datetime import datetime

from pydantic import Field

from validation import (
    Expectations,
    NotNullRule,
    StringLengthRule,
    ValidatedStruct,
    ValidationRule,
)

SMITH_TAGS_BRONZE_TABLE_NAME = "smith_tags"


class SmithTagBronze(ValidatedStruct):
    """Patient-level tag applied via the Smith user-store.

    One row per (patient_id, tag) assignment. Sourced from
    ``mongo_smith.tags``. Silver ``dim_tag`` derives the canonical
    tag lookup from this table.
    """

    patient_id: str = Field(description="Tagged patient identifier (natural key).")
    tags_name: str = Field(
        description="Tag label as captured in Smith (e.g. 'high_risk', 'churned')."
    )
    disabled: bool | None = Field(
        description="Whether the tag has been disabled/removed from the patient."
    )
    created_at: datetime | None = Field(
        description="UTC timestamp when the tag was applied."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="patient_id"),
            NotNullRule(column="tags_name"),
            StringLengthRule(column="patient_id", min_length=1, max_length=255),
            StringLengthRule(column="tags_name", min_length=1, max_length=255),
        ]


class SmithTagBronzeExpectations(Expectations):
    """Expectations for analytics.bronze.smith_tags.

    Loose floor — the collection may be small or empty early in rollout.
    """

    MIN_ROWS = 0
    UNIQUE_KEYS = [["patient_id", "tags_name"]]
    NON_NULL_COLUMNS = ["patient_id", "tags_name"]
