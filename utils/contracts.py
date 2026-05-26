"""Table contract operations.

Contracts live in a MongoDB ``data_contracts`` collection that only the
poorbricks server can reach. Consumers (CI, fixtures, ``verify``) resolve
contracts over HTTP from the server via :func:`fetch_contract`; the Mongo
helpers below are server-side only.
"""

from __future__ import annotations

from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING, Any

import pymongo
from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

# Keep the contract HTTP fetch snappy: a missing/unreachable server should
# fail in seconds, not stall every test for pymongo's 30s default.
_CONTRACT_HTTP_TIMEOUT_SECONDS = 10


def _client() -> pymongo.MongoClient[Any]:
    """Get a MongoDB client connected to the contracts URI.

    Falls back to ``mongo_uri`` if ``contracts_mongo_uri`` is unset, so a
    single-Mongo deployment still works.
    """
    from poorbricks.settings import settings

    uri = settings.contracts_mongo_uri or settings.mongo_uri
    return pymongo.MongoClient(uri)


def fetch_contract_from_mongo(table_name: str) -> dict[str, Any]:
    """Read a contract straight from the MongoDB contracts store.

    Server-side only — the poorbricks server has Mongo access and is what
    exposes contracts over HTTP. Consumers must use :func:`fetch_contract`.

    Raises KeyError if not found.
    """
    from poorbricks.settings import settings

    doc = _client()[settings.contracts_db][settings.contracts_collection].find_one(
        {"_id": table_name}
    )
    if doc is None:
        raise KeyError(f"No contract found for table {table_name!r}.")
    return doc  # type: ignore[no-any-return]


@cache
def fetch_contract(table_name: str) -> dict[str, Any]:
    """Resolve a published contract from the poorbricks server over HTTP.

    Contracts live in the server's MongoDB; consumers never connect to Mongo
    themselves. The server base URL comes from ``settings.contracts_api_url``
    (the internal Tailscale endpoint by default), so CI only needs network
    access to the server — no Mongo URI.

    Cached per process: contract schemas are immutable within a run, so the
    same upstream is fetched at most once across a whole test session.

    Raises KeyError if the server returns 404.
    """
    import requests

    from poorbricks.settings import settings

    base = settings.contracts_api_url.rstrip("/")
    resp = requests.get(
        f"{base}/v1/contracts/{table_name}",
        timeout=_CONTRACT_HTTP_TIMEOUT_SECONDS,
    )
    if resp.status_code == 404:
        raise KeyError(f"No contract found for table {table_name!r}.")
    resp.raise_for_status()
    return resp.json()  # type: ignore[no-any-return]


def list_contracts() -> list[dict[str, Any]]:
    """Return a lightweight summary of every contract in the store.

    Used by the Streamlit explorer to populate its sidebar without
    pulling fixture rows for every pipeline.
    """
    from poorbricks.settings import settings

    cursor = _client()[settings.contracts_db][settings.contracts_collection].find(
        {},
        {
            "table_name": 1,
            "level": 1,
            "storage": 1,
            "comment": 1,
            "pushed_at": 1,
        },
    )
    return list(cursor)


def list_contract_details() -> list[dict[str, Any]]:
    """Return contract summaries plus upstream inputs and baseline row count.

    Heavier than :func:`list_contracts` — it also pulls each contract's
    ``inputs`` declarations and ``profile.row_count`` — so the Streamlit
    status dashboard and lineage DAG can be built from a single query
    without fetching example rows or fixtures.
    """
    from poorbricks.settings import settings

    cursor = _client()[settings.contracts_db][settings.contracts_collection].find(
        {},
        {
            "table_name": 1,
            "level": 1,
            "storage": 1,
            "comment": 1,
            "pushed_at": 1,
            "inputs": 1,
            "profile.row_count": 1,
        },
    )
    return list(cursor)


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
    storage: str = "delta",
    comment: str = "",
    module: str = "",
    fields: list[dict[str, Any]] | None = None,
    validation_rules: list[dict[str, Any]] | None = None,
    expectations: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    fixtures: list[dict[str, Any]] | None = None,
) -> None:
    """Upsert a contract document into the contracts collection.

    Stores the full pipeline configuration so the Streamlit explorer can
    render fields, expectations, inputs, fixtures, and sample data without
    importing pipeline code. The profile is used as a baseline for future
    drift detection.
    """
    from poorbricks.settings import settings

    document = {
        "_id": table_name,
        "table_name": table_name,
        "schema_json": schema.jsonValue(),
        "example_rows": example_rows,
        "pipeline_key": pipeline_key,
        "level": level,
        "storage": storage,
        "comment": comment,
        "module": module,
        "fields": fields or [],
        "validation_rules": validation_rules or [],
        "expectations": expectations or {},
        "inputs": inputs or [],
        "fixtures": fixtures or [],
        "profile": profile,
        "pushed_at": datetime.utcnow().isoformat(),
    }
    _client()[settings.contracts_db][settings.contracts_collection].replace_one(
        {"_id": table_name},
        _bson_safe(document),
        upsert=True,
    )


def _bson_safe(value: Any) -> Any:
    """Recursively make a value safe to store in MongoDB/BSON.

    BSON has no ``date`` type — only ``datetime``. A bare ``datetime.date``
    (e.g. a monthly-grain column's value in ``example_rows``) is promoted to
    a midnight ``datetime`` so contract upserts never raise ``InvalidDocument``.
    """
    from datetime import date, datetime

    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, dict):
        return {k: _bson_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_bson_safe(item) for item in value]
    return value


__all__ = [
    "fetch_contract",
    "fetch_contract_from_mongo",
    "list_contract_details",
    "list_contracts",
    "profile_dataframe",
    "push_contract",
]
