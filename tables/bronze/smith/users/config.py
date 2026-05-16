"""Schema (data contract) for analytics.bronze.smith_users.

Mirrors poorbricks_dev.master.patients — the patient-identity master built
from MongoDB mongo_smith.users — into the medallion-Postgres bronze
layer. Named 'smith_users' here because Smith is the team's user-identity
service; downstream tables prefer the Smith-rooted name.
"""

from datetime import datetime

from pydantic import BaseModel, Field

from validation import (
    Expectations,
    ValidatedStruct,
    ValidationRule,
)

SMITH_USERS_BRONZE_TABLE_NAME = "smith.users"


class ExtraField(BaseModel):
    fieldName: str | None
    fieldValue: str | None


class SmithUserBronze(ValidatedStruct):
    """Patient identity master, mirrored from the Smith user collection.

    One row per patient. Direct mirror of mongo_smith.users into the
    Postgres bronze schema so silver / gold pipelines that need patient
    demographics can read directly from analytics.bronze.smith_users.

    Maps MongoDB camelCase fields to snake_case for consistency.
    """

    mongo_id: str | None = Field(
        description="MongoDB ObjectId — the true unique identifier for the patient."
    )
    externalId: str | None = Field(
        description="External identifier from the source system."
    )
    name: str | None = Field(description="Full name of the patient.")
    email: str | None = Field(description="Email address of the patient.")
    phone: str | None = Field(description="Phone number of the patient.")
    origin: str | None = Field(
        description="Program origin (e.g. 'aon', 'ge', 'camed')."
    )
    active: bool | None = Field(
        default=None, description="Whether the patient is currently active."
    )
    createdAt: datetime | None = Field(
        default=None,
        description="Timestamp when the patient record was created in Smith.",
    )
    birth_date: datetime | None = Field(description="Patient date of birth.")
    cpf: str | None = Field(description="CPF (Brazilian tax ID) if available.")
    extraFields: list[ExtraField] | None = Field(
        description="Additional fields from the patient profile."
    )
    fivetran_synced: datetime | None = Field(
        default=None, description="Timestamp of the last Fivetran sync for this row."
    )
    fivetran_deleted: bool | None = Field(
        default=None, description="True if the row was soft-deleted by Fivetran."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return []


class SmithUserBronzeExpectations(Expectations):
    """Production-derived expectations for analytics.bronze.smith_users.

    Floor inherited from poorbricks_dev.master.patients (≥9,199 rows on
    2026-05-07).
    """

    MIN_ROWS = 9_199
    UNIQUE_KEYS = [["patient_id"]]
    NON_NULL_COLUMNS = []
    NULL_RATE_MAX = {
        "birth_date": 0.1,
        "external_id": 0.15,
        "extra_fields": 0.05,
        "gender": 0.15,
        "origin": 0.01,
    }
    ENUM_VALUES = {"gender": ["FEMALE", "MALE"]}
