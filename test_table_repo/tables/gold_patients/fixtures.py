"""Fixtures for gold_patients."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.gold_patients.pipeline import GoldPatientsInputs

NOW = datetime(2026, 5, 8, 12, 0, 0)


@scenario("smoke")
def smoke() -> GoldPatientsInputs:
    return GoldPatientsInputs.from_rows(
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
