"""PostgreSQL writer using psycopg2 for efficient bulk insert via COPY."""

from __future__ import annotations

import io
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


class PostgresLoader:
    """Load DataFrames into PostgreSQL via COPY FROM STDIN."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        db: str = "analytics",
        user: str = "analytics",
        password: str = "analytics",
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password

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
        row_count = df.count()

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
                                # Escape backslashes and newlines
                                str_val = str_val.replace("\\", "\\\\").replace(
                                    "\n", "\\n"
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


__all__ = ["PostgresLoader"]
