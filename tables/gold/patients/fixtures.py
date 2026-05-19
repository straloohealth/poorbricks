"""Scenarios for gold.patients (silver.dim_patient → projection)."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.gold.patients.pipeline import PatientsGoldInputs

NOW = datetime(2026, 5, 8, 12, 0, 0)


@scenario("smoke")
def smoke() -> PatientsGoldInputs:
    """One row with the silver.dim_patient shape."""
    return PatientsGoldInputs.from_rows(
        {
            "dim_patient": [
                {
                    "patient_id": "p1",
                    "mongo_id": "m1",
                    "name": "Maria Silva",
                    "email": "maria@example.com",
                    "phone": "+5511999998888",
                    "birth_date": NOW,
                    "created_at": NOW,
                    "origin_slug": "rede_sc",
                    "is_active": True,
                    "is_high_risk": False,
                    "is_surgical": True,
                }
            ]
        }
    )


__all__ = ["smoke"]
