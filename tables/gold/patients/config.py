"""Schema for gold.patients — silver-fed patient dimension for overseer.

Exposes the silver ``DimPatient`` columns directly, so overseer's reads
trace end-to-end through the silver/gold medallion. Replaces the legacy
``poorbricks.patients`` Postgres mirror that previously passthrough'd
``master.patients``.
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

PATIENTS_TABLE_NAME = "patients"


class PatientGold(ValidatedStruct):
    """Gold patient dataset — one row per patient, sourced from silver.dim_patient.

    Surfaces the columns overseer's PostgresQuery classes consume:
    identity (id, name, email, phone), program origin (origin_slug),
    lifecycle flags (is_active), clinical flags (is_high_risk,
    is_surgical), and the canonical timestamps. ``patient_id`` is the
    natural key.
    """

    patient_id: str = Field(
        description="Stable Smith user id — natural key, FK target for fact_*.",
    )
    name: str | None = Field(description="Patient full name as stored in Smith.")
    email: str | None = Field(description="Primary email address; may be missing.")
    phone: str | None = Field(
        description="Primary phone number (E.164 if normalized upstream).",
    )
    birth_date: datetime | None = Field(
        description="Date of birth as a timestamp (00:00:00 UTC of the date).",
    )
    origin_slug: str | None = Field(
        description=(
            "Program/contract origin slug (e.g. 'aon', 'ge', 'camed'). Drives "
            "per-account dashboards downstream."
        ),
    )
    is_active: bool = Field(
        description="Whether the patient is currently active in the program.",
    )
    is_high_risk: bool = Field(
        description="Whether clinical triage flagged the patient as high-risk.",
    )
    is_surgical: bool = Field(
        description="Whether the patient has at least one surgical recommendation.",
    )
    created_at: datetime = Field(
        description="When the patient record was first created in Smith.",
    )

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


# Backwards-compatible aliases. The gold class name is ``PatientGold`` to
# avoid colliding with master ``Patients`` in the auto-generated
# ``tables/fields.py`` namespace.
Patients = PatientGold
PatientsGold = PatientGold


class PatientsGoldExpectations(Expectations):
    """Production expectations for ``analytics.gold.patients`` (silver-fed)."""

    MIN_ROWS = 5_000
    UNIQUE_KEYS = [["patient_id"]]
    NON_NULL_COLUMNS = [
        "patient_id",
        "created_at",
        "is_active",
        "is_high_risk",
        "is_surgical",
    ]


__all__ = [
    "PATIENTS_TABLE_NAME",
    "PatientGold",
    "Patients",
    "PatientsGold",
    "PatientsGoldExpectations",
]
