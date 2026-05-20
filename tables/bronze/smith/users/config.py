"""Schema (data contract) for analytics.bronze.smith_users.

Mirrors mongo_smith.users — the patient-identity master built by Smith —
into the medallion-Postgres bronze layer.
"""

from datetime import datetime

from pydantic import Field

from validation import (
    Expectations,
    ValidatedStruct,
    ValidationRule,
)

SMITH_USERS_BRONZE_TABLE_NAME = "smith_users"


class SmithUserBronze(ValidatedStruct):
    mongo_id: str | None = Field(
        description="MongoDB ObjectId — the true unique identifier for the patient."
    )
    external_id: str | None = None
    name: str | None = None
    email: str | None = None
    phone: str | None = None
    origin: str | None = None
    active: bool | None = None
    created_at: datetime | None = None
    birth_date: datetime | None = None
    cpf: str | None = None

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return []


class SmithUserBronzeExpectations(Expectations):
    MIN_ROWS = 9_199
    UNIQUE_KEYS = [["mongo_id"]]
    NON_NULL_COLUMNS = []
    NULL_RATE_MAX = {
        "birth_date": 0.1,
        "external_id": 0.15,
        "origin": 0.01,
    }
    ENUM_VALUES: dict[str, list[str]] = {}
