"""Drift detection: compare pipeline output against stored contract baseline.

Fetches the stored profile (null rates, enum samples, row count) from the
MongoDB contract collection and compares against current execution stats.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class DriftReport:
    """Report of data drift detected in a pipeline output."""

    table_name: str
    row_count_baseline: int
    row_count_current: int
    new_null_columns: list[str]
    resolved_enum_changes: list[str]
    schema_additions: list[str]
    schema_removals: list[str]
    type_changes: list[str]

    @property
    def has_drift(self) -> bool:
        """True if any drift was detected."""
        return bool(
            self.new_null_columns
            or self.resolved_enum_changes
            or self.schema_additions
            or self.schema_removals
            or self.type_changes
        )


def check_drift(
    table_name: str,
    current_df: DataFrame,
    null_rate_threshold: float = 0.1,
    enum_threshold: int = 50,
) -> DriftReport:
    """Compare current DataFrame against stored contract profile.

    Args:
        table_name: Logical table name (e.g. "smith.users")
        current_df: Current pipeline output DataFrame
        null_rate_threshold: Alert if null rate increases beyond this % (0.0-1.0)
        enum_threshold: Treat columns with >N distinct non-null values as
            non-enum (don't track enum changes for high-cardinality columns)

    Returns:
        DriftReport with detected schema and data changes
    """
    from utils.contracts import fetch_contract, profile_dataframe

    try:
        contract = fetch_contract(table_name)
    except KeyError:
        raise ValueError(f"No contract for {table_name!r}.") from None

    baseline_profile = contract.get("profile", {})
    baseline_row_count = baseline_profile.get("row_count", 0)
    baseline_null_rates = baseline_profile.get("null_rates", {})
    baseline_enum_samples = baseline_profile.get("enum_samples", {})

    current_profile = profile_dataframe(current_df)
    current_row_count = current_profile["row_count"]
    current_null_rates = current_profile["null_rates"]
    current_enum_samples = current_profile["enum_samples"]

    new_null_columns: list[str] = []
    for col, rate in current_null_rates.items():
        baseline_rate = baseline_null_rates.get(col, 0.0)
        if rate > baseline_rate + null_rate_threshold:
            new_null_columns.append(f"{col}: {baseline_rate:.1%} → {rate:.1%}")

    resolved_enum_changes: list[str] = []
    for col, current_values in current_enum_samples.items():
        baseline_values = set(baseline_enum_samples.get(col, []))
        current_set = set(current_values)
        new_values = current_set - baseline_values
        if new_values and len(baseline_values) <= enum_threshold:
            resolved_enum_changes.append(f"{col}: new values {sorted(new_values)}")

    baseline_cols = set(baseline_null_rates.keys())
    current_cols = set(current_null_rates.keys())
    schema_additions = sorted(current_cols - baseline_cols)
    schema_removals = sorted(baseline_cols - current_cols)

    from pyspark.sql.types import StructType

    baseline_schema = StructType.fromJson(contract["schema_json"])
    baseline_types = {f.name: f.dataType.simpleString() for f in baseline_schema.fields}
    current_types = {
        f.name: f.dataType.simpleString() for f in current_df.schema.fields
    }

    type_changes = [
        f"{col}: {baseline_types[col]} → {current_types[col]}"
        for col in baseline_types.keys() & current_types.keys()
        if baseline_types[col] != current_types[col]
    ]

    return DriftReport(
        table_name=table_name,
        row_count_baseline=baseline_row_count,
        row_count_current=current_row_count,
        new_null_columns=new_null_columns,
        resolved_enum_changes=resolved_enum_changes,
        schema_additions=schema_additions,
        schema_removals=schema_removals,
        type_changes=type_changes,
    )


__all__ = ["DriftReport", "check_drift"]
