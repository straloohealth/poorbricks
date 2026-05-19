"""Fixtures for happy_path — relies on smith.users contract from MongoDB."""

from __future__ import annotations

from datetime import datetime

from poorbricks import scenario
from tables.happy_path.pipeline import HappyPathInputs


@scenario("nominal")
def nominal() -> HappyPathInputs:
    rows = [
        {
            "mongo_id": "507f1f77bcf86cd799439011",
            "externalId": "ext-1",
            "name": "Alice",
            "email": None,
            "phone": None,
            "origin": "aon",
            "active": True,
            "createdAt": datetime(2026, 1, 1),
            "birth_date": None,
            "cpf": None,
            "extraFields": None,
        }
    ]
    return HappyPathInputs.from_rows({"smith_users": rows})
