"""Scenarios for verifying silver.dim_navigator locally."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from poorbricks import scenario
from tables.bronze.smith.navigators.config import SmithNavigatorBronze
from tables.silver.dim_navigator.pipeline import DimNavigatorInputs
from utils.dataframes import create_dataframe


def _nav(
    navigator_id: str = "n1",
    name: str | None = "Carla Lima",
    role: str | None = "navigator",
    is_active: bool = True,
    started_at: datetime | None = datetime(2024, 1, 1, 9, 0, 0),
) -> dict:
    return {
        "navigator_id": navigator_id,
        "name": name,
        "role": role,
        "is_active": is_active,
        "started_at": started_at,
        "email": f"{navigator_id}@example.com",
        "phone": None,
        "org": None,
        "groups": None,
    }


@scenario("empty")
def empty() -> DimNavigatorInputs:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found")
    df = create_dataframe([], SmithNavigatorBronze.to_struct())
    return DimNavigatorInputs.from_dataframes({"smith_navigators": df})


@scenario("smoke")
def smoke() -> DimNavigatorInputs:
    """Three navigators with different roles and activity states."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found")
    rows = [
        _nav(navigator_id="n1", name=" Carla Lima ", role="navigator"),
        _nav(navigator_id="n2", name="Pedro Santos", role="admin"),
        _nav(
            navigator_id="n3",
            name="Beatriz Alves",
            role="navigator",
            is_active=False,
        ),
    ]
    df = create_dataframe(rows, SmithNavigatorBronze.to_struct())
    return DimNavigatorInputs.from_dataframes({"smith_navigators": df})
