"""Named scenarios for verifying analytics.bronze.smith_organizations."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.bronze.smith.organizations.pipeline import (
    SmithOrganizationsInputs,
)

_NOW = datetime(2026, 5, 7, 12, 0, 0)
_CONTRACT_START = datetime(2024, 1, 1, 0, 0, 0)


@scenario("empty")
def empty() -> SmithOrganizationsInputs:
    """Empty upstream — confirms the bronze writer tolerates no rows."""
    return SmithOrganizationsInputs.from_rows({"upstream": []})


@scenario("smoke")
def smoke() -> SmithOrganizationsInputs:
    """Two representative orgs — one enterprise, one pilot."""
    return SmithOrganizationsInputs.from_rows(
        {
            "upstream": [
                {
                    "org_id": "org-aon",
                    "slug": "aon",
                    "display_name": "Aon Movimento 360",
                    "account_type": "enterprise",
                    "contract_start_date": _CONTRACT_START,
                    "created_at": _CONTRACT_START,
                    "updated_at": _NOW,
                },
                {
                    "org_id": "org-sepaco",
                    "slug": "sepaco",
                    "display_name": "Sepaco",
                    "account_type": "pilot",
                    "contract_start_date": _CONTRACT_START,
                    "created_at": _CONTRACT_START,
                    "updated_at": _NOW,
                },
            ]
        }
    )
