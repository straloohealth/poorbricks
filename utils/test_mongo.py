"""Tests for the lazy, partitioned MongoDB reader.

Integration tests require ``docker-compose up`` (local MongoDB).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from bson import Decimal128, ObjectId
from bson.int64 import Int64
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from utils.mongo import (
    _camel_to_snake,
    _prepare_doc,
    _resolve_id_field,
    _sanitize_value,
    get_all,
)

LOCAL_MONGO_URI = "mongodb://localhost:27017"
TEST_DB = "test_ooc_db"

_SCHEMA = StructType(
    [
        StructField("mongo_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("score", LongType(), True),
    ]
)


class TestCamelToSnake:
    def test_converts_camel_case(self) -> None:
        assert _camel_to_snake("fullName") == "full_name"

    def test_leaves_snake_case_untouched(self) -> None:
        assert _camel_to_snake("full_name") == "full_name"


class TestSanitizeValue:
    """BSON types are converted to Spark-friendly Python natives."""

    def test_object_id_to_str(self) -> None:
        oid = ObjectId()
        assert _sanitize_value(oid) == str(oid)

    def test_int64_to_int(self) -> None:
        result = _sanitize_value(Int64(7))
        assert result == 7
        assert isinstance(result, int)

    def test_decimal128_to_float(self) -> None:
        assert _sanitize_value(Decimal128("1.5")) == 1.5

    def test_recurses_into_nested_structures(self) -> None:
        oid = ObjectId()
        out = _sanitize_value({"a": [oid], "b": {"c": Int64(3)}})
        assert out == {"a": [str(oid)], "b": {"c": 3}}


class TestResolveIdField:
    def test_picks_string_id_field(self) -> None:
        assert _resolve_id_field(_SCHEMA) == "mongo_id"

    def test_returns_none_when_schema_keeps_id(self) -> None:
        schema = StructType([StructField("_id", StringType(), False)])
        assert _resolve_id_field(schema) is None


class TestPrepareDoc:
    """One raw document mapped to a schema-ordered tuple."""

    def test_maps_id_renames_camel_and_orders(self) -> None:
        oid = ObjectId()
        doc = {"_id": oid, "fullName": "Ada", "score": Int64(9)}
        result = _prepare_doc(doc, "mongo_id", ["mongo_id", "full_name", "score"])
        assert result == (str(oid), "Ada", 9)

    def test_missing_field_becomes_none(self) -> None:
        oid = ObjectId()
        doc = {"_id": oid, "fullName": "Ada"}
        result = _prepare_doc(doc, "mongo_id", ["mongo_id", "full_name", "score"])
        assert result == (str(oid), "Ada", None)

    def test_nested_subdocument_keys_are_snake_cased(self) -> None:
        """A camelCase array-of-struct (e.g. extraFields) is renamed per element
        so it binds to a snake_case nested StructType."""
        oid = ObjectId()
        doc = {
            "_id": oid,
            "extraFields": [
                {"fieldName": "Cargo", "fieldValue": "Dev"},
                {"fieldName": "Unidade"},
            ],
        }
        result = _prepare_doc(doc, "mongo_id", ["mongo_id", "extra_fields"])
        assert result == (
            str(oid),
            [
                {"field_name": "Cargo", "field_value": "Dev"},
                {"field_name": "Unidade"},
            ],
        )


@pytest.fixture
def mongo_db() -> Iterator[Any]:
    import pymongo

    client: pymongo.MongoClient = pymongo.MongoClient(LOCAL_MONGO_URI)
    client.drop_database(TEST_DB)
    try:
        yield client[TEST_DB]
    finally:
        client.drop_database(TEST_DB)
        client.close()


@pytest.mark.integration
@pytest.mark.spark
@pytest.mark.xdist_group("ooc_mongo")
class TestGetAll:
    """The partitioned reader returns every document exactly once."""

    def test_round_trip(self, spark: SparkSession, mongo_db: Any) -> None:
        docs = [
            {"_id": ObjectId(), "fullName": f"user-{i}", "score": i} for i in range(50)
        ]
        mongo_db["people"].insert_many(docs)

        df = get_all(LOCAL_MONGO_URI, TEST_DB, "people", _SCHEMA)

        assert df.schema == _SCHEMA
        assert df.count() == 50
        ids = [r["mongo_id"] for r in df.select("mongo_id").collect()]
        assert sorted(ids) == sorted(str(d["_id"]) for d in docs)

    def test_partitions_cover_all_rows_without_gaps(
        self, spark: SparkSession, mongo_db: Any
    ) -> None:
        docs = [{"_id": ObjectId(), "fullName": "x", "score": i} for i in range(200)]
        mongo_db["people"].insert_many(docs)

        df = get_all(LOCAL_MONGO_URI, TEST_DB, "people", _SCHEMA)

        # the read fans out across partitions, and the contiguous _id ranges
        # cover every document exactly once — no gaps, no overlaps.
        assert df.rdd.getNumPartitions() > 1
        assert df.count() == 200
        assert df.select("mongo_id").distinct().count() == 200

    def test_empty_collection_returns_empty_dataframe(
        self, spark: SparkSession, mongo_db: Any
    ) -> None:
        df = get_all(LOCAL_MONGO_URI, TEST_DB, "missing", _SCHEMA)
        assert df.schema == _SCHEMA
        assert df.count() == 0
