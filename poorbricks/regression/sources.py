"""Result-set sources for :mod:`poorbricks.regression.diff`.

A ``Source`` is anything that can be loaded into a ``pandas.DataFrame``.
The diff harness compares two arbitrary sources — Mongo against Postgres,
Postgres against Parquet, an in-memory frame against either, etc. —
without caring which side is "legacy" or "new".
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class Source(ABC):
    """Anything that produces a ``pd.DataFrame`` when ``load()`` is called."""

    @abstractmethod
    def load(self):  # -> pd.DataFrame
        """Materialise the source as a pandas DataFrame."""


@dataclass(frozen=True)
class MongoSource(Source):
    """A MongoDB collection, optionally filtered and/or projected.

    ``query`` and ``projection`` follow the pymongo signature. ``_id`` is
    excluded by default so it doesn't dominate the diff with unique values.
    """

    uri: str
    db: str
    collection: str
    query: dict[str, Any] | None = None
    projection: dict[str, int] | None = None
    limit: int | None = None

    def load(self):
        import pandas as pd
        from pymongo import MongoClient

        client = MongoClient(self.uri)
        cursor = client[self.db][self.collection].find(
            self.query or {},
            self.projection if self.projection is not None else {"_id": 0},
        )
        if self.limit is not None:
            cursor = cursor.limit(self.limit)
        return pd.DataFrame(list(cursor))


@dataclass(frozen=True)
class PostgresSource(Source):
    """A parameterised SQL query against a Postgres DSN."""

    dsn: str
    sql: str
    params: tuple[Any, ...] | dict[str, Any] | None = None

    def load(self):
        import pandas as pd
        import psycopg2

        conn = psycopg2.connect(self.dsn)
        try:
            return pd.read_sql(self.sql, conn, params=self.params)
        finally:
            conn.close()


@dataclass(frozen=True)
class ParquetSource(Source):
    """A parquet file on local disk — used to load a prior snapshot."""

    path: Path | str

    def load(self):
        import pandas as pd

        return pd.read_parquet(self.path)


@dataclass
class DataFrameSource(Source):
    """Wrap an in-memory ``pd.DataFrame`` so the diff harness can consume it."""

    df: Any  # pd.DataFrame; loose-typed so importers don't need pandas at class time

    def load(self):
        return self.df.copy()
