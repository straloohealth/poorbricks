"""Lazy, partitioned MongoDB reader.

A collection is split into ``settings.read_partitions`` contiguous ranges over
``_id`` (via ``$bucketAuto``, which builds balanced buckets server-side). Each
Spark partition opens its own cursor and reads just its range inside an
executor — documents are never collected into the driver, so memory stays
bounded no matter how large the collection is.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from typing import Any

from bson import Decimal128, ObjectId
from bson.int64 import Int64
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def _camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _sanitize_value(value: Any) -> Any:
    """Recursively convert BSON types to Python-native equivalents for Spark.

    Handles ObjectId -> str, Int64 -> int, Decimal128 -> float, and recurses
    into nested dicts and lists at any depth.
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, dict):
        return {k: _sanitize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    return value


def _snake_case_keys(value: Any) -> Any:
    """Recursively camelCase -> snake_case every dict key at any depth.

    ``createDataFrame`` binds a dict to a ``StructType`` by field name, so a
    nested Mongo sub-document (e.g. ``extraFields[].fieldName``) must have
    snake_case keys to populate a snake_case nested struct — otherwise it
    lands as all-null. Applied only inside the Mongo reader, never to the
    schema-inference path which intentionally preserves native key casing.
    """
    if isinstance(value, dict):
        return {_camel_to_snake(k): _snake_case_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_snake_case_keys(item) for item in value]
    return value


def _resolve_id_field(schema: StructType) -> str | None:
    """Find the schema field that should receive MongoDB's ``_id`` value.

    MongoDB documents key on ``_id``; bronze schemas rename it to a
    string-typed ``*_id`` field (e.g. ``mongo_id``). Returns None when the
    schema keeps ``_id`` verbatim.
    """
    field_names = {field.name for field in schema.fields}
    if "_id" in field_names:
        return None
    for field in schema.fields:
        type_str = str(field.dataType)
        if field.name.endswith("_id") and (
            "StringType" in type_str or "string" in type_str
        ):
            return field.name
    return None


def _prepare_doc(
    doc: dict[str, Any], id_field: str | None, field_names: list[str]
) -> tuple[Any, ...]:
    """Map one raw MongoDB document to a schema-ordered tuple.

    Applies the ``_id`` -> ``id_field`` rename, camelCase -> snake_case key
    renames (top-level when the snake form is a schema field, and recursively
    inside every nested sub-document), and BSON-type sanitisation. Returns
    values ordered to match ``field_names``.
    """
    field_set = set(field_names)
    mapped: dict[str, Any] = {}
    for key, value in doc.items():
        if key == "_id" and id_field is not None:
            mapped[id_field] = value
            continue
        snake = _camel_to_snake(key)
        if snake != key and snake in field_set:
            mapped[snake] = value
        else:
            mapped[key] = value
    return tuple(
        _sanitize_value(_snake_case_keys(mapped.get(name))) for name in field_names
    )


def _resolve_partition_bounds(
    mongo_uri: str, db_name: str, collection_name: str, num_partitions: int
) -> list[tuple[Any, Any, bool]]:
    """Split a collection into <= ``num_partitions`` contiguous ``_id`` ranges.

    Uses ``$bucketAuto`` so MongoDB computes balanced, roughly equal-count
    buckets server-side; only the tiny boundary documents reach the driver.
    Returns ``(lo, hi, is_last)`` tuples — the last range includes ``hi``.
    """
    import pymongo as pm

    client: pm.MongoClient = pm.MongoClient(mongo_uri)
    try:
        collection = client[db_name][collection_name]
        if collection.estimated_document_count() == 0:
            return []
        buckets = list(
            collection.aggregate(
                [{"$bucketAuto": {"groupBy": "$_id", "buckets": num_partitions}}],
                allowDiskUse=True,
            )
        )
        bounds: list[tuple[Any, Any, bool]] = []
        for index, bucket in enumerate(buckets):
            edges = bucket["_id"]
            bounds.append((edges["min"], edges["max"], index == len(buckets) - 1))
        return bounds
    finally:
        client.close()


def _make_range_reader(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    id_field: str | None,
    field_names: list[str],
) -> Callable[[tuple[Any, Any, bool]], Iterator[tuple[Any, ...]]]:
    """Build the ``flatMap`` function that reads one ``_id`` range on an executor."""

    def _read_range(
        bound: tuple[Any, Any, bool],
    ) -> Iterator[tuple[Any, ...]]:
        import pymongo as pm

        lo, hi, is_last = bound
        upper_op = "$lte" if is_last else "$lt"
        query = {"_id": {"$gte": lo, upper_op: hi}}
        client: pm.MongoClient = pm.MongoClient(mongo_uri)
        try:
            for doc in client[db_name][collection_name].find(query):
                yield _prepare_doc(doc, id_field, field_names)
        finally:
            client.close()

    return _read_range


def get_all(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    schema: StructType,
) -> DataFrame:
    """Read a MongoDB collection as a lazy, partitioned Spark DataFrame.

    The collection is split into ``settings.read_partitions`` ranges; each
    Spark partition reads its own range from an executor when an action runs.
    Documents are never materialised in the driver.

    Args:
        mongo_uri: MongoDB connection URI.
        db_name: Database name.
        collection_name: Collection name.
        schema: PySpark StructType for the result DataFrame.

    Returns:
        PySpark DataFrame with the collection's documents.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found.")

    from poorbricks.settings import settings

    field_names = [field.name for field in schema.fields]
    id_field = _resolve_id_field(schema)
    bounds = _resolve_partition_bounds(
        mongo_uri, db_name, collection_name, settings.read_partitions
    )
    if not bounds:
        return spark.createDataFrame([], schema)

    read_range = _make_range_reader(
        mongo_uri, db_name, collection_name, id_field, field_names
    )
    rows_rdd = spark.sparkContext.parallelize(bounds, len(bounds)).flatMap(read_range)
    return spark.createDataFrame(rows_rdd, schema)
