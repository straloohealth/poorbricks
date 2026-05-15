"""Transform: pass through mongo_smith.organizations into the
analytics.bronze.smith_organizations contract. Bronze is shape-only;
no business derivation lives here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from tables.bronze.smith.organizations.config import (
    SmithOrganizationBronze,
)
from utils.dataframes import create_dataframe

if TYPE_CHECKING:
    from tables.bronze.smith.organizations.pipeline import (
        SmithOrganizationsInputs,
    )


def compute(inputs: SmithOrganizationsInputs) -> DataFrame:
    return create_dataframe(inputs.upstream, SmithOrganizationBronze.to_struct())
