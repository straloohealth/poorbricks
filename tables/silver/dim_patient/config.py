"""Schema (data contract) for the silver dim_patient table.

One row per patient. Built by joining/cleaning ``bronze.smith_users`` so
downstream facts and reports can join against a stable, deduplicated
patient dimension.
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

DIM_PATIENT_TABLE_NAME = "dim_patient"


class DimPatient(ValidatedStruct):
    """Silver patient dimension — one row per patient.

    Source: ``bronze.smith_users`` (the Smith user-store ingestion).
    Cleansed (trimmed strings, normalized booleans) and deduplicated by
    ``patient_id``. Joined to by every fact_* table in the silver layer
    and surfaced in dashboards (Streamlit / Oak / Morpheus) as the canonical
    patient reference.
    """

    patient_id: str = Field(
        description="Stable Smith user identifier — natural key of the dimension."
    )
    mongo_id: str | None = Field(
        description=(
            "Original Mongo ObjectId (string) of the patient record in the source "
            "system, when known. Useful for tracing back to legacy ingest paths."
        )
    )
    name: str | None = Field(description="Patient full name as stored in Smith.")
    email: str | None = Field(description="Primary email address; may be missing.")
    phone: str | None = Field(
        description="Primary phone number (E.164 if normalized upstream)."
    )
    birth_date: datetime | None = Field(
        description="Date of birth as a timestamp (00:00:00 UTC of the date)."
    )
    created_at: datetime = Field(
        description="When the patient record was first created in Smith."
    )
    origin_slug: str | None = Field(
        description=(
            "Program/contract origin slug (e.g. 'aon', 'ge', 'camed'). Drives "
            "per-account dashboards in the gold layer."
        )
    )
    is_active: bool = Field(
        description="Whether the patient is currently active in the program."
    )
    is_deleted: bool = Field(
        description="Soft-delete flag carried from Smith (true means the patient was removed)."
    )
    is_high_risk: bool = Field(
        description="Whether clinical triage flagged the patient as high-risk."
    )
    is_surgical: bool = Field(
        description="Whether the patient has at least one surgical recommendation."
    )

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="patient_id"),
            NotNullRule(column="created_at"),
            NotNullRule(column="is_active"),
            NotNullRule(column="is_deleted"),
            NotNullRule(column="is_high_risk"),
            NotNullRule(column="is_surgical"),
            StringLengthRule(column="patient_id", min_length=1, max_length=255),
        ]


class DimPatientExpectations(Expectations):
    """Production expectations for ``analytics.silver.dim_patient``.

    Calibrated against the same source counts as ``patients`` (legacy
    Rocky bronze, ~11k rows). Tighten after the first prod run lands.
    """

    MIN_ROWS = 5_000
    UNIQUE_KEYS = [["patient_id"]]
    NON_NULL_COLUMNS = [
        "patient_id",
        "created_at",
        "is_active",
        "is_deleted",
        "is_high_risk",
        "is_surgical",
    ]
