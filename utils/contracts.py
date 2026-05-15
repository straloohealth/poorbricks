"""MongoDB data_contracts collection operations: fetch, profile, and push table contracts."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import pymongo
from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


def _client() -> pymongo.MongoClient[Any]:
    """Get a MongoDB client connected to the configured URI."""
    from poorbricks.settings import settings

    return pymongo.MongoClient(settings.mongo_uri)


def fetch_contract(table_name: str) -> dict[str, Any]:
    """Look up a contract by table_name in the contracts store.

    Raises KeyError if not found.
    """
    from poorbricks.settings import settings

    doc = _client()[settings.contracts_db][settings.contracts_collection].find_one(
        {"_id": table_name}
    )
    if doc is None:
        raise KeyError(
            f"No contract found for table {table_name!r}. Run: "
            f"poetry run python scripts/push_contract.py --pipeline {table_name}"
        )
    return doc


def profile_dataframe(df: DataFrame) -> dict[str, Any]:
    """Compute row count, null rates per column, and enum samples for low-cardinality fields."""
    row_count = df.count()
    null_rates: dict[str, float] = {}
    enum_samples: dict[str, list[Any]] = {}

    for field in df.schema.fields:
        col = field.name
        null_count = df.filter(f.col(col).isNull()).count()
        null_rates[col] = round(null_count / row_count, 4) if row_count > 0 else 0.0

        type_str = str(field.dataType)
        if type_str in ("StringType()", "BooleanType()"):
            distinct_values = [
                r[col] for r in df.select(col).distinct().limit(51).collect()
            ]
            if len(distinct_values) <= 50:
                enum_samples[col] = sorted(v for v in distinct_values if v is not None)

    return {
        "row_count": row_count,
        "null_rates": null_rates,
        "enum_samples": enum_samples,
    }


def push_contract(
    table_name: str,
    schema: StructType,
    example_rows: list[dict[str, Any]],
    pipeline_key: str,
    level: str,
    profile: dict[str, Any],
) -> None:
    """Upsert a contract document into the contracts collection.

    Stores schema JSON, example rows, and profiling stats (row count, null rates,
    enum samples). The profile is used as a baseline for future drift detection.
    """
    from poorbricks.settings import settings

    _client()[settings.contracts_db][settings.contracts_collection].replace_one(
        {"_id": table_name},
        {
            "_id": table_name,
            "table_name": table_name,
            "schema_json": schema.jsonValue(),
            "example_rows": example_rows,
            "pipeline_key": pipeline_key,
            "level": level,
            "profile": profile,
            "pushed_at": datetime.utcnow().isoformat(),
        },
        upsert=True,
    )


__all__ = ["fetch_contract", "profile_dataframe", "push_contract"]
