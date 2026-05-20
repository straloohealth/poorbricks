"""Tests for the partition-streamed PostgreSQL writer and SQL profiler.

Integration tests require ``docker-compose up`` (local PostgreSQL).
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg2
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from utils.postgres import (
    PostgresLoader,
    _encode_copy_row,
    _encode_copy_value,
    _RowCopyReader,
)

TEST_SCHEMA = "test_ooc"

_DF_SCHEMA = StructType(
    [
        StructField("widget_id", StringType(), False),
        StructField("label", StringType(), True),
        StructField("score", LongType(), True),
    ]
)


class TestEncodeCopyValue:
    """Pure encoding of one COPY TEXT cell."""

    def test_none_becomes_null_token(self) -> None:
        assert _encode_copy_value(None) == "\\N"

    def test_escapes_tsv_framing_characters(self) -> None:
        assert _encode_copy_value("a\tb\nc\\d\re") == "a\\tb\\nc\\\\d\\re"

    def test_plain_value_passthrough(self) -> None:
        assert _encode_copy_value(42) == "42"


class TestRowCopyReader:
    """The file-like adapter streams Rows without buffering the partition."""

    def test_streams_rows_as_tsv(self) -> None:
        rows = [Row(widget_id="x", score=1), Row(widget_id="y", score=2)]
        reader = _RowCopyReader(iter(rows), ["widget_id", "score"])
        assert reader.read() == "x\t1\ny\t2\n"

    def test_sized_reads_reassemble_the_stream(self) -> None:
        rows = [Row(widget_id="x", score=1), Row(widget_id="y", score=2)]
        reader = _RowCopyReader(iter(rows), ["widget_id", "score"])
        chunks: list[str] = []
        while True:
            chunk = reader.read(3)
            if not chunk:
                break
            assert len(chunk) <= 3
            chunks.append(chunk)
        assert "".join(chunks) == "x\t1\ny\t2\n"

    def test_empty_iterator_yields_nothing(self) -> None:
        reader = _RowCopyReader(iter([]), ["widget_id"])
        assert reader.read(8) == ""

    def test_encode_copy_row_orders_by_columns(self) -> None:
        row = Row(score=7, widget_id="w1", label=None)
        assert _encode_copy_row(row, ["widget_id", "label", "score"]) == "w1\t\\N\t7\n"


def _query(loader: PostgresLoader, sql: str) -> list[tuple]:
    conn = psycopg2.connect(
        host=loader.host,
        port=loader.port,
        dbname=loader.db,
        user=loader.user,
        password=loader.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


def _table_exists(loader: PostgresLoader, schema: str, table: str) -> bool:
    rows = _query(
        loader,
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'",
    )
    return len(rows) == 1


@pytest.fixture
def loader() -> Iterator[PostgresLoader]:
    pg = PostgresLoader()
    yield pg
    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.db, user=pg.user, password=pg.password
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE')
        conn.commit()
    finally:
        conn.close()


@pytest.mark.integration
@pytest.mark.spark
@pytest.mark.xdist_group("ooc_postgres")
class TestPostgresWriter:
    """The streaming writer round-trips data and swaps atomically."""

    def test_write_round_trip(
        self, spark: SparkSession, loader: PostgresLoader
    ) -> None:
        rows = [
            {"widget_id": "w1", "label": "alpha", "score": 10},
            {"widget_id": "w2", "label": None, "score": 20},
            {"widget_id": "w3", "label": "gamma", "score": None},
        ]
        df = spark.createDataFrame(rows, _DF_SCHEMA)

        written = loader.write(df, TEST_SCHEMA, "widgets")

        assert written == 3
        result = _query(
            loader,
            f'SELECT widget_id, label, score FROM "{TEST_SCHEMA}".widgets '
            "ORDER BY widget_id",
        )
        assert result == [
            ("w1", "alpha", 10),
            ("w2", None, 20),
            ("w3", "gamma", None),
        ]

    def test_write_leaves_no_staging_table(
        self, spark: SparkSession, loader: PostgresLoader
    ) -> None:
        df = spark.createDataFrame(
            [{"widget_id": "w1", "label": "a", "score": 1}], _DF_SCHEMA
        )
        loader.write(df, TEST_SCHEMA, "widgets")
        assert _table_exists(loader, TEST_SCHEMA, "widgets")
        assert not _table_exists(loader, TEST_SCHEMA, "widgets__staging")

    def test_second_write_replaces_the_table(
        self, spark: SparkSession, loader: PostgresLoader
    ) -> None:
        first = spark.createDataFrame(
            [{"widget_id": "old", "label": "x", "score": 1}], _DF_SCHEMA
        )
        loader.write(first, TEST_SCHEMA, "widgets")

        second = spark.createDataFrame(
            [{"widget_id": "new", "label": "y", "score": 2}], _DF_SCHEMA
        )
        written = loader.write(second, TEST_SCHEMA, "widgets")

        assert written == 1
        ids = [
            r[0]
            for r in _query(loader, f'SELECT widget_id FROM "{TEST_SCHEMA}".widgets')
        ]
        assert ids == ["new"]

    def test_write_empty_dataframe(
        self, spark: SparkSession, loader: PostgresLoader
    ) -> None:
        df = spark.createDataFrame([], _DF_SCHEMA)
        written = loader.write(df, TEST_SCHEMA, "widgets")
        assert written == 0
        assert _table_exists(loader, TEST_SCHEMA, "widgets")


@pytest.mark.integration
@pytest.mark.spark
@pytest.mark.xdist_group("ooc_postgres")
class TestProfileAndBounds:
    """SQL-side profiling and column bounds."""

    def test_profile_table(self, spark: SparkSession, loader: PostgresLoader) -> None:
        rows = [
            {"widget_id": "w1", "label": "alpha", "score": 1},
            {"widget_id": "w2", "label": None, "score": 2},
            {"widget_id": "w3", "label": None, "score": 3},
            {"widget_id": "w4", "label": "alpha", "score": 4},
        ]
        df = spark.createDataFrame(rows, _DF_SCHEMA)
        loader.write(df, TEST_SCHEMA, "widgets")

        profile = loader.profile_table(TEST_SCHEMA, "widgets", _DF_SCHEMA)

        assert profile["row_count"] == 4
        assert profile["null_rates"]["widget_id"] == 0.0
        assert profile["null_rates"]["label"] == 0.5
        assert profile["enum_samples"]["label"] == ["alpha"]

    def test_column_bounds(self, spark: SparkSession, loader: PostgresLoader) -> None:
        rows = [
            {"widget_id": "w1", "label": "a", "score": 5},
            {"widget_id": "w2", "label": "b", "score": 99},
            {"widget_id": "w3", "label": "c", "score": 12},
        ]
        df = spark.createDataFrame(rows, _DF_SCHEMA)
        loader.write(df, TEST_SCHEMA, "widgets")

        low, high = loader.column_bounds(TEST_SCHEMA, "widgets", "score")

        assert (low, high) == (5, 99)
