"""PostgreSQL writer using psycopg2 for efficient bulk insert via COPY."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from itertools import chain
from typing import Any

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool


@dataclass(frozen=True)
class TableSnapshot:
    schema: str
    name: str
    row_count: int
    size_bytes: int
    columns: list[ColumnInfo]
    sample_rows: list[dict[str, Any]]


def _encode_copy_value(value: Any) -> str:
    """Encode one cell for PostgreSQL ``COPY ... FORMAT text``."""
    if value is None:
        return "\\N"
    text = str(value)
    if text == "NaT":  # defensive: pandas null datetime
        return "\\N"
    return (
        text.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _encode_copy_row(row: Any, columns: list[str]) -> str:
    """Encode one Spark Row as a tab-separated COPY line."""
    return "\t".join(_encode_copy_value(row[c]) for c in columns) + "\n"


class _RowCopyReader:
    """File-like adapter: lazily encodes Spark Rows as COPY TEXT for psycopg2.

    ``psycopg2.copy_expert`` calls ``read(size)``; we pull only as many rows
    from the iterator as each read needs, so memory stays O(one row) no matter
    how large the partition is.
    """

    def __init__(self, rows: Iterator[Any], columns: list[str]) -> None:
        self._rows = rows
        self._columns = columns
        self._buffer = ""

    def read(self, size: int = -1) -> str:
        if size is None or size < 0:
            parts = [self._buffer]
            self._buffer = ""
            parts.extend(_encode_copy_row(r, self._columns) for r in self._rows)
            return "".join(parts)
        while len(self._buffer) < size:
            try:
                row = next(self._rows)
            except StopIteration:
                break
            self._buffer += _encode_copy_row(row, self._columns)
        chunk = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return chunk


def _partition_copy_writer(
    conn_params: dict[str, Any], copy_sql: str, columns: list[str]
) -> Callable[[Iterator[Any]], None]:
    """Build a ``foreachPartition`` function that COPY-streams a partition.

    Runs on Spark executors: each non-empty partition opens its own
    connection and streams straight into ``COPY ... FROM STDIN`` — nothing is
    collected on the driver, so peak memory is bounded by one partition.
    """

    def _write_partition(rows: Iterator[Any]) -> None:
        import psycopg2

        iterator = iter(rows)
        try:
            first = next(iterator)
        except StopIteration:
            return  # empty partition — skip the connection entirely
        conn = psycopg2.connect(**conn_params)
        try:
            with conn.cursor() as cur:
                reader = _RowCopyReader(chain([first], iterator), columns)
                cur.copy_expert(copy_sql, reader)
            conn.commit()
        finally:
            conn.close()

    return _write_partition


def _jsonify_complex_columns(df: DataFrame) -> DataFrame:
    """Serialise struct/array/map columns to JSON text for Postgres storage.

    Postgres has no column type matching Spark's nested types, so a complex
    column is stored as JSON text; ``runner._parse_complex_columns`` restores
    the declared type when the table is read back.
    """
    from pyspark.sql import functions as f

    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType | MapType | StructType):
            df = df.withColumn(field.name, f.to_json(f.col(field.name)))
    return df


class PostgresLoader:
    """Load DataFrames into PostgreSQL via COPY FROM STDIN."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        from poorbricks.settings import settings

        self.host = host or settings.postgres_host
        self.port = port or settings.postgres_port
        self.db = db or settings.postgres_db
        self.user = user or settings.postgres_user
        self.password = password or settings.postgres_password

    def _connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password,
        )

    def ensure_schema(self, schema_name: str) -> None:
        """CREATE SCHEMA IF NOT EXISTS."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            conn.commit()
        finally:
            conn.close()

    def _spark_type_to_postgres(self, spark_type: Any) -> str:
        """Map PySpark types to PostgreSQL DDL types."""
        if isinstance(spark_type, StringType):
            return "TEXT"
        elif isinstance(spark_type, LongType):
            return "BIGINT"
        elif isinstance(spark_type, IntegerType):
            return "INTEGER"
        elif isinstance(spark_type, DoubleType):
            return "DOUBLE PRECISION"
        elif isinstance(spark_type, FloatType):
            return "REAL"
        elif isinstance(spark_type, BooleanType):
            return "BOOLEAN"
        elif isinstance(spark_type, TimestampType):
            return "TIMESTAMPTZ"
        elif isinstance(spark_type, DateType):
            return "DATE"
        elif isinstance(spark_type, ArrayType):
            return "TEXT"
        else:
            return "TEXT"

    def _create_table_ddl(
        self, schema_name: str, table_name: str, df_schema: StructType
    ) -> str:
        """Generate CREATE TABLE DDL from DataFrame schema."""
        columns = []
        for field in df_schema.fields:
            col_type = self._spark_type_to_postgres(field.dataType)
            col_def = f'"{field.name}" {col_type}'
            if not field.nullable:
                col_def += " NOT NULL"
            columns.append(col_def)
        return f'CREATE TABLE "{schema_name}"."{table_name}" ({", ".join(columns)})'

    def write(self, df: DataFrame, schema_name: str, table_name: str) -> int:
        """Overwrite a table from a DataFrame, streaming partition by partition.

        Each Spark partition is COPY-streamed straight into a staging table
        from its executor task — the DataFrame is never collected to the
        driver, so peak memory is bounded by one partition regardless of
        dataset size. The staging table is then swapped into place atomically,
        so consumers never observe a partially loaded table.

        Returns the row count written.
        """
        # Struct/array/map columns have no Postgres equivalent — store them as
        # JSON text; runner._parse_complex_columns restores them on read.
        df = _jsonify_complex_columns(df)
        columns = df.columns
        staging_name = f"{table_name}__staging"
        conn_params: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "dbname": self.db,
            "user": self.user,
            "password": self.password,
        }

        # 1. Driver: ensure the schema and a fresh, empty staging table.
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                cur.execute(
                    f'DROP TABLE IF EXISTS "{schema_name}"."{staging_name}" CASCADE'
                )
                cur.execute(
                    self._create_table_ddl(schema_name, staging_name, df.schema)
                )
            conn.commit()
        finally:
            conn.close()

        # 2. Executors: stream every partition into the staging table.
        col_list = ",".join(f'"{c}"' for c in columns)
        copy_sql = (
            f'COPY "{schema_name}"."{staging_name}" ({col_list}) '
            f"FROM STDIN WITH (FORMAT text, NULL '\\N')"
        )
        df.foreachPartition(_partition_copy_writer(conn_params, copy_sql, columns))

        # 3. Driver: atomically swap the staging table into the final name.
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE'
                )
                cur.execute(
                    f'ALTER TABLE "{schema_name}"."{staging_name}" '
                    f'RENAME TO "{table_name}"'
                )
                cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                result = cur.fetchone()
                row_count = int(result[0]) if result is not None else 0
            conn.commit()
        finally:
            conn.close()

        return row_count

    def profile_table(
        self, schema_name: str, table_name: str, df_schema: StructType
    ) -> dict[str, Any]:
        """Profile a table with a single SQL pass: row count + per-column null
        rates, plus a bounded distinct scan per string/boolean column for enum
        samples. Replaces a Spark profile that re-scanned the dataset once per
        column — pushing the work into PostgreSQL keeps it O(one pass).
        """
        columns = [field.name for field in df_schema.fields]
        qualified = f'"{schema_name}"."{table_name}"'
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                count_exprs = ", ".join(
                    ["COUNT(*) AS __row_count"]
                    + [f'COUNT("{c}") AS "{c}"' for c in columns]
                )
                cur.execute(f"SELECT {count_exprs} FROM {qualified}")
                row = cur.fetchone() or ()
                names = [d[0] for d in cur.description or []]
                counts = dict(zip(names, row))
                row_count = int(counts.get("__row_count", 0) or 0)

                null_rates: dict[str, float] = {}
                for c in columns:
                    non_null = int(counts.get(c, 0) or 0)
                    null_rates[c] = (
                        round((row_count - non_null) / row_count, 4)
                        if row_count > 0
                        else 0.0
                    )

                enum_samples: dict[str, list[Any]] = {}
                for field in df_schema.fields:
                    if str(field.dataType) not in ("StringType()", "BooleanType()"):
                        continue
                    cur.execute(
                        f'SELECT DISTINCT "{field.name}" FROM {qualified} '
                        f'WHERE "{field.name}" IS NOT NULL LIMIT 51'
                    )
                    values = [r[0] for r in cur.fetchall()]
                    if len(values) <= 50:
                        enum_samples[field.name] = sorted(values)

            return {
                "row_count": row_count,
                "null_rates": null_rates,
                "enum_samples": enum_samples,
            }
        finally:
            conn.close()

    def column_bounds(
        self, schema_name: str, table_name: str, column: str
    ) -> tuple[Any, Any]:
        """Return ``(min, max)`` of a column — used to range-partition JDBC reads."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT MIN("{column}"), MAX("{column}") '
                    f'FROM "{schema_name}"."{table_name}"'
                )
                row = cur.fetchone()
                if row is None:
                    return None, None
                return row[0], row[1]
        finally:
            conn.close()


class PostgresInspector:
    """Read-only PostgreSQL inspection for the Streamlit status page."""

    _SYSTEM_SCHEMAS = (
        "pg_catalog",
        "information_schema",
        "pg_toast",
    )

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        from poorbricks.settings import settings

        self.host = host or settings.postgres_host
        self.port = port or settings.postgres_port
        self.db = db or settings.postgres_db
        self.user = user or settings.postgres_user
        self.password = password or settings.postgres_password

    def _connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password,
        )

    def _columns_by_table(
        self, cur: psycopg2.extensions.cursor
    ) -> dict[tuple[str, str], list[ColumnInfo]]:
        """Fetch every user table's columns in a single query."""
        cur.execute(
            """
            SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema NOT IN %s
              AND table_schema NOT LIKE 'pg_%%'
            ORDER BY table_schema, table_name, ordinal_position
            """,
            (self._SYSTEM_SCHEMAS,),
        )
        by_table: dict[tuple[str, str], list[ColumnInfo]] = {}
        for schema, table, name, data_type, is_nullable in cur.fetchall():
            by_table.setdefault((schema, table), []).append(
                ColumnInfo(
                    name=name,
                    data_type=data_type,
                    nullable=is_nullable == "YES",
                )
            )
        return by_table

    def inspect(self, sample_size: int = 10) -> list[TableSnapshot]:
        """Return a full snapshot of every user table over a single connection.

        Per table: exact row count, on-disk size, column schema, and up to
        ``sample_size`` random rows. Accurate but O(n) over rows — suitable
        for the analytics-scale tables this framework produces.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                      AND table_schema NOT IN %s
                      AND table_schema NOT LIKE 'pg_%%'
                    ORDER BY table_schema, table_name
                    """,
                    (self._SYSTEM_SCHEMAS,),
                )
                tables = cur.fetchall()
                columns_by_table = self._columns_by_table(cur)

                snapshots: list[TableSnapshot] = []
                for schema, name in tables:
                    qualified = f'"{schema}"."{name}"'
                    cur.execute(f"SELECT COUNT(*) FROM {qualified}")
                    row_count = int(cur.fetchone()[0])  # type: ignore[index]
                    cur.execute(
                        "SELECT pg_total_relation_size(%s::regclass)",
                        (f"{schema}.{name}",),
                    )
                    size_bytes = int(cur.fetchone()[0])  # type: ignore[index]

                    sample_rows: list[dict[str, Any]] = []
                    if row_count > 0 and sample_size > 0:
                        cur.execute(
                            f"SELECT * FROM {qualified} ORDER BY random() LIMIT %s",
                            (sample_size,),
                        )
                        col_names = [d[0] for d in cur.description or []]
                        sample_rows = [
                            dict(zip(col_names, row)) for row in cur.fetchall()
                        ]

                    snapshots.append(
                        TableSnapshot(
                            schema=schema,
                            name=name,
                            row_count=row_count,
                            size_bytes=size_bytes,
                            columns=columns_by_table.get((schema, name), []),
                            sample_rows=sample_rows,
                        )
                    )
                return snapshots
        finally:
            conn.close()

    def server_info(self) -> dict[str, str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version(), current_database(), current_user")
                version, database, user = cur.fetchone()  # type: ignore[misc]
                return {
                    "host": str(self.host),
                    "port": str(self.port),
                    "database": database,
                    "user": user,
                    "version": version,
                }
        finally:
            conn.close()


__all__ = [
    "PostgresLoader",
    "PostgresInspector",
    "TableSnapshot",
    "ColumnInfo",
]
