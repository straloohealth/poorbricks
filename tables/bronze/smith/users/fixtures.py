"""Named scenarios for verifying analytics.bronze.smith_users."""

from __future__ import annotations

from datetime import datetime

from framework import scenario
from tables.bronze.smith.users.pipeline import SmithUsersInputs

_NOW = datetime(2026, 5, 7, 12, 0, 0)
_BIRTH = datetime(1985, 6, 12, 0, 0, 0)


@scenario("empty")
def empty() -> SmithUsersInputs:
    """Empty upstream — confirms the bronze writer tolerates no rows."""
    return SmithUsersInputs.from_rows({"upstream": []})


@scenario("smoke")
def smoke() -> SmithUsersInputs:
    """Single representative AON-program patient."""
    return SmithUsersInputs.from_rows(
        {
            "upstream": [
                {
                    "mongo_id": "507f1f77bcf86cd799439011",
                    "externalId": "AON12345",
                    "name": "John Doe",
                    "email": "john@aon.com",
                    "phone": "+1-555-0100",
                    "origin": "aon",
                    "active": True,
                    "createdAt": _NOW,
                    "birth_date": _BIRTH,
                    "cpf": None,
                    "extraFields": [
                        {"fieldName": "company", "fieldValue": "AON Corp"}
                    ],
                    "fivetran_synced": None,
                    "fivetran_deleted": None,
                }
            ]
        }
    )
