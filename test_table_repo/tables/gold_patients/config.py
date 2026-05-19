"""Schema for gold_patients — migrated from framework-repo/tables/gold/patients/.

One row per patient, sourced from silver.dim_patient via the contracts store.
"""

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


class PatientGold(ValidatedStruct):
    patient_id: str = Field(description="Stable Smith user id — natural key.")
    name: str | None = Field(description="Patient full name.")
    email: str | None = Field(description="Primary email address; may be missing.")
    phone: str | None = Field(description="Primary phone number.")
    birth_date: datetime | None = Field(description="Date of birth.")
    origin_slug: str | None = Field(description="Program/contract origin slug.")
    is_active: bool = Field(description="Whether the patient is currently active.")
    is_high_risk: bool = Field(description="Clinical triage high-risk flag.")
    is_surgical: bool = Field(description="Has at least one surgical recommendation.")
    created_at: datetime = Field(description="When the patient record was created.")

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="patient_id"),
            NotNullRule(column="created_at"),
            NotNullRule(column="is_active"),
            NotNullRule(column="is_high_risk"),
            NotNullRule(column="is_surgical"),
            StringLengthRule(column="patient_id", min_length=1, max_length=255),
        ]


class PatientGoldExpectations(Expectations):
    MIN_ROWS = 1
    UNIQUE_KEYS = [["patient_id"]]
    NON_NULL_COLUMNS = [
        "patient_id",
        "created_at",
        "is_active",
        "is_high_risk",
        "is_surgical",
    ]
