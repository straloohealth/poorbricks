"""Scenarios for verifying silver.dim_patient locally.

Fixture rows mirror the canonical ``bronze.smith_users`` shape
(``patient_id, external_id, origin, active, created_at, gender,
birth_date, extra_fields, fivetran_synced, fivetran_deleted``). The
transform null-pads silver-only columns (name/email/phone/is_high_risk
/is_surgical) until bronze grows them, so this fixture file does not
attempt to set them — they don't exist on the input.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from poorbricks import scenario
from tables.bronze.smith.users.config import SmithUserBronze
from tables.silver.dim_patient.pipeline import DimPatientInputs
from utils.dataframes import create_dataframe

_NOW = datetime(2026, 1, 15, 12, 0, 0)
_EARLIER = datetime(2025, 6, 1, 9, 0, 0)


def _user(
    mongo_id: str = "507f1f77bcf86cd799439011",
    external_id: str | None = None,
    active: bool = True,
    fivetran_deleted: bool | None = None,
    created_at: datetime = _NOW,
    origin: str | None = "aon",
) -> dict:
    return {
        "mongo_id": mongo_id,
        "externalId": external_id
        if external_id is not None
        else f"mongo-{mongo_id[-4:]}",
        "name": "Test Patient",
        "email": None,
        "phone": None,
        "origin": origin,
        "active": active,
        "createdAt": created_at,
        "birth_date": datetime(1990, 5, 20, 0, 0, 0),
        "cpf": None,
        "extraFields": None,
        "fivetran_synced": None,
        "fivetran_deleted": fivetran_deleted,
    }


@scenario("empty")
def empty() -> DimPatientInputs:
    """No upstream rows — output should be empty."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found")
    df = create_dataframe([], SmithUserBronze.to_struct())
    return DimPatientInputs.from_dataframes({"smith_users": df})


@scenario("smoke")
def smoke() -> DimPatientInputs:
    """Three patients spanning active / inactive / different origins."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found")
    rows = [
        _user(mongo_id="507f1f77bcf86cd799439011"),
        _user(mongo_id="507f1f77bcf86cd799439012", active=False),
        _user(mongo_id="507f1f77bcf86cd799439013", origin="ge"),
    ]
    df = create_dataframe(rows, SmithUserBronze.to_struct())
    return DimPatientInputs.from_dataframes({"smith_users": df})


@scenario("duplicate_patient_id_keeps_latest")
def duplicate_patient_id() -> DimPatientInputs:
    """Two rows for the same mongo_id — silver should keep the latest one."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found")
    rows = [
        _user(
            mongo_id="507f1f77bcf86cd799439011",
            external_id="mongo-p1-old",
            created_at=_EARLIER,
        ),
        _user(
            mongo_id="507f1f77bcf86cd799439011",
            external_id="mongo-p1-new",
            created_at=_NOW,
        ),
    ]
    df = create_dataframe(rows, SmithUserBronze.to_struct())
    return DimPatientInputs.from_dataframes({"smith_users": df})
