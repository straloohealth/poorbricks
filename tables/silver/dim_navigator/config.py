"""Schema for the silver dim_navigator table."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field

from validation import (
    Expectations,
    NotNullRule,
    StringLengthRule,
    ValidatedStruct,
    ValidationRule,
)

DIM_NAVIGATOR_TABLE_NAME = "dim_navigator"


class DimNavigator(ValidatedStruct):
    """Silver navigator dimension — one row per navigator (care-team member).

    Source: ``bronze.smith_navigators`` (the Smith user-store rows whose
    ``role`` marks them as care-team operators rather than patients). All
    fact_* tables that record a navigator action join here.
    """

    navigator_id: str = Field(description="Stable navigator identifier — natural key.")
    name: str | None = Field(description="Navigator full name.")
    role: str | None = Field(
        description="Navigator role/title (e.g. 'navigator', 'admin')."
    )
    is_active: bool = Field(
        description="Whether the navigator is currently active in Smith."
    )
    started_at: datetime | None = Field(
        description="When the navigator joined the program."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="navigator_id"),
            NotNullRule(column="is_active"),
            StringLengthRule(column="navigator_id", min_length=1, max_length=255),
        ]


class DimNavigatorExpectations(Expectations):
    """Production expectations for ``analytics.silver.dim_navigator``."""

    MIN_ROWS = 100
    UNIQUE_KEYS = [["navigator_id"]]
    NON_NULL_COLUMNS = ["navigator_id", "is_active"]
