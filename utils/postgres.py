"""PostgreSQL writer using psycopg2 for efficient bulk insert via COPY."""

from __future__ import annotations

import io
from dataclasses import dataclass
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
        """Overwrite table from DataFrame using COPY FROM STDIN.

        Returns row count written.
        """
        row_count: int = df.count()

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                # Create schema
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')

                # Drop and recreate table
                cur.execute(
                    f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE'
                )
                ddl = self._create_table_ddl(schema_name, table_name, df.schema)
                cur.execute(ddl)

                # Convert to pandas and write via COPY
                pdf = df.toPandas()
                buffer = io.StringIO()

                # Write TSV-format data (tab-separated, \N for null)
                for _, row in pdf.iterrows():
                    values = []
                    for col in df.columns:
                        val = row[col]
                        if val is None:
                            values.append("\\N")
                        else:
                            # Convert NaT (pandas null datetime) to \N for PostgreSQL
                            str_val = str(val)
                            if str_val == "NaT":
                                values.append("\\N")
                            else:
                                # Escape backslashes + characters that would
                                # collide with COPY TEXT format's TSV framing.
                                str_val = (
                                    str_val.replace("\\", "\\\\")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                    .replace("\t", "\\t")
                                )
                                values.append(str_val)
                    buffer.write("\t".join(values) + "\n")

                buffer.seek(0)
                # Use copy_expert for schema-qualified table names
                col_list = ",".join(f'"{c}"' for c in df.columns)
                copy_sql = f"COPY {schema_name}.{table_name} ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
                cur.copy_expert(copy_sql, buffer)

            conn.commit()
        finally:
            conn.close()

        return row_count


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
