"""Schema (data contract) for the analytics.bronze.smith_organizations table.

Mirrors the ``smith.organizations`` MongoDB collection — the canonical
client/account org master used by overseer dashboards and downstream
silver dimensions (e.g. ``silver.dim_organization``).

No Fivetran→Delta mirror exists in poorbricks_dev for this collection, so
the bronze pipeline reads MongoDB directly through ``MongoSource``.
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

SMITH_ORGANIZATIONS_BRONZE_TABLE_NAME = "smith.organizations"


class SmithOrganizationBronze(ValidatedStruct):
    """Smith organizations — one row per client / account org.

    Sourced from ``mongo_smith.organizations``. Used as the canonical
    org reference by silver dimensions (``dim_organization``) and by
    overseer per-account reporting.
    """

    org_id: str = Field(
        description="Smith organization identifier (Mongo _id, natural key)."
    )
    slug: str = Field(
        description="Machine-readable org slug (e.g. 'aon', 'ge', 'sepaco')."
    )
    display_name: str | None = Field(
        description="Human-readable org name as shown in dashboards."
    )
    account_type: str | None = Field(
        description=(
            "Account type / segmentation (e.g. 'enterprise', 'pilot', "
            "'self-serve'). Optional."
        )
    )
    contract_start_date: datetime | None = Field(
        description="UTC timestamp when the org's contract started."
    )
    created_at: datetime | None = Field(
        description="UTC timestamp when the org row was created in smith."
    )
    updated_at: datetime | None = Field(
        description="UTC timestamp of the last update to the org row."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="org_id"),
            NotNullRule(column="slug"),
            StringLengthRule(column="org_id", min_length=1, max_length=255),
            StringLengthRule(column="slug", min_length=1, max_length=255),
        ]


class SmithOrganizationBronzeExpectations(Expectations):
    """Expectations for analytics.bronze.smith_organizations.

    Loose floor — the org master is a small reference table (handful
    of rows). Tighten during review once production sample is available.
    """

    MIN_ROWS = 1
    UNIQUE_KEYS = [["org_id"], ["slug"]]
    NON_NULL_COLUMNS = ["org_id", "slug"]
