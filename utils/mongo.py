import re
from typing import Any

import pymongo as pm
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def _find_all(
    mongo_uri: str, db_name: str, collection_name: str
) -> list[dict[str, Any]]:
    mongo_client: pm.MongoClient[dict[str, Any]] = pm.MongoClient(mongo_uri)
    collection = mongo_client[db_name][collection_name]
    items = list(collection.find({}))
    return items


def _resolve_dot_key(doc: dict[str, Any], key: str) -> Any:
    """Resolve a dot-notation key (e.g. 'source.documentId') from a nested dict."""
    parts = key.split(".", 1)
    value = doc[parts[0]]
    if len(parts) > 1 and isinstance(value, dict):
        return _resolve_dot_key(value, parts[1])
    return value


def _sanitize_value(v: Any) -> Any:
    """Recursively convert BSON types to Python-native equivalents for PySpark compatibility."""
    from bson import Decimal128, ObjectId
    from bson.int64 import Int64

    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, Int64):
        return int(v)
    if isinstance(v, Decimal128):
        return float(v.to_decimal())
    if isinstance(v, dict):
        return {dk: _sanitize_value(dv) for dk, dv in v.items()}
    if isinstance(v, list):
        return [_sanitize_value(item) for item in v]
    return v


def _camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _rename_camel_keys(
    rows: list[dict[str, Any]], schema_field_names: set[str]
) -> list[dict[str, Any]]:
    """Rename camelCase document keys to snake_case when the snake_case form is in the schema."""
    if not rows:
        return rows
    sample_keys: set[str] = set()
    for doc in rows:
        sample_keys.update(doc.keys())
    rename_map = {
        k: _camel_to_snake(k)
        for k in sample_keys
        if _camel_to_snake(k) != k and _camel_to_snake(k) in schema_field_names
    }
    if not rename_map:
        return rows
    return [{rename_map.get(k, k): v for k, v in doc.items()} for doc in rows]


def _sanitize_bson_types(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert BSON types to Python-native equivalents for PySpark compatibility.

    Handles ObjectId → str, Int64 → int, Decimal128 → float, and recurses into
    nested dicts and lists at any depth.
    """
    return [{k: _sanitize_value(v) for k, v in doc.items()} for doc in rows]


def get_all(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    schema: StructType,
) -> DataFrame:
    """Fetch all documents from a MongoDB collection and return as DataFrame.

    Args:
        mongo_uri: MongoDB connection URI (e.g. mongodb://localhost:27017)
        db_name: Database name
        collection_name: Collection name
        schema: PySpark StructType for the result DataFrame

    Returns:
        PySpark DataFrame with documents from the collection
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found.")
    rows = _find_all(mongo_uri, db_name, collection_name)

    # Map MongoDB's "_id" field to the appropriate schema field before schema enforcement.
    # MongoDB documents have "_id", but the schema expects a different field name
    # (e.g., "mongo_id" for users, "navigator_id" for navigators).
    schema_field_names = {f.name for f in schema.fields}
    if "_id" not in schema_field_names:
        # Find a candidate field that should receive the MongoDB _id value.
        # Look for fields ending with "_id" that are string-typed.
        id_field = None
        for field in schema.fields:
            field_type_str = str(field.dataType)
            if field.name.endswith("_id") and (
                "StringType" in field_type_str or "string" in field_type_str
            ):
                id_field = field.name
                break

        if id_field and rows:
            mapped_rows = []
            for doc in rows:
                mapped_doc = dict(doc)  # shallow copy
                if "_id" in mapped_doc:
                    # Convert ObjectId to string
                    object_id = mapped_doc.pop("_id")
                    mapped_doc[id_field] = str(object_id)
                mapped_rows.append(mapped_doc)
            rows = mapped_rows

    rows = _rename_camel_keys(rows, schema_field_names)
    rows = _sanitize_bson_types(rows)
    return spark.createDataFrame(rows, schema=schema)
