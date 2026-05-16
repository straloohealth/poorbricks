"""Wiring for analytics.bronze.smith_organizations (Postgres-target bronze).

Reads directly from MongoDB (``mongo_smith.organizations``).
"""

from __future__ import annotations

from typing import Annotated

from pyspark.sql import DataFrame

from poorbricks import Inputs, MongoSource, pipeline
from tables.bronze.smith.organizations.config import (
    SMITH_ORGANIZATIONS_BRONZE_TABLE_NAME,
    SmithOrganizationBronze,
)
from tables.bronze.smith.organizations.transform import compute

DB_NAME = "smith"
COLLECTION_NAME = "organizations"


class SmithOrganizationsInputs(Inputs):
    """Single Mongo upstream feeding analytics.bronze.smith_organizations."""

    upstream: Annotated[
        DataFrame,
        MongoSource(
            db=DB_NAME,
            collection=COLLECTION_NAME,
            schema=SmithOrganizationBronze.to_struct(),
        ),
    ]


@pipeline(
    name=SMITH_ORGANIZATIONS_BRONZE_TABLE_NAME,
    model=SmithOrganizationBronze,
    level="bronze",
    comment=(
        "Mirror of mongo_smith.organizations — the canonical client / "
        "account org master. Lands in analytics.bronze.smith_organizations "
        "so silver dimensions (dim_organization) and overseer per-account "
        "reports can read directly from Postgres."
    ),
)
def smith_organizations_bronze(inputs: SmithOrganizationsInputs) -> DataFrame:
    return compute(inputs)
