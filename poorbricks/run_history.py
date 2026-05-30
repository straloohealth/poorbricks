"""Framework-owned run history: a record of every pipeline execution.

``run_and_persist`` (the single compute → write → contract path) wraps each run
and records a :class:`RunRecord` into ``poorbricks_meta.run_history`` — the
system of record for "what ran, when, with what result". Everything downstream
reads from here: the row-count anomaly check (recent successful counts), the
stale-data monitor (last successful run per pipeline), regression-vs-prior
(the previous successful run), and the web-debug UI (``GET /v1/runs``).

The store deliberately reuses the PostgreSQL connection settings the writer
already resolves (``poorbricks.settings``), so a deployment that can write
tables can also record history with no extra configuration.

Recording is best-effort: callers wrap ``record`` so a meta-store outage never
fails a production pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras

META_SCHEMA = "poorbricks_meta"
_RUN_HISTORY_TABLE = "run_history"


def run_context() -> tuple[str, str | None]:
    """Resolve ``(environment, sha)`` for the current run from the environment.

    Workers receive ``POORBRICKS_ENV`` / ``POORBRICKS_SHA`` as injected env
    vars (see the DAG generator). Falls back to ``GIT_SHA`` for local/CI runs
    and ``"unknown"`` when no environment tag is present.
    """
    environment = os.getenv("POORBRICKS_ENV", "unknown")
    sha = os.getenv("POORBRICKS_SHA") or os.getenv("GIT_SHA")
    return environment, sha


@dataclass
class RunRecord:
    """One row of ``poorbricks_meta.run_history``."""

    pipeline_key: str
    table_name: str
    environment: str
    mode: str
    status: str  # "ok" | "failed"
    started_at: datetime
    finished_at: datetime
    duration_s: float
    sha: str | None = None
    row_count: int | None = None
    schema_hash: str | None = None
    error: str | None = None
    drift_summary: dict[str, Any] | None = None
    timings: dict[str, float] = field(default_factory=dict)
    anomaly: dict[str, Any] | None = None
    id: int | None = None


class RunHistoryStore:
    """Append-only DAO over ``poorbricks_meta.run_history``.

    Connection parameters mirror :class:`utils.postgres.PostgresLoader` (read
    from ``poorbricks.settings`` unless overridden), so the store targets the
    same database the pipeline writes to — including a dev database when the
    worker's ``POSTGRES_*`` env vars point there.
    """

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
        self._schema_ready = False

    def _connect(self) -> psycopg2.extensions.connection:
        # Fail fast rather than hang a worker if the meta-store is briefly
        # unreachable — recording is best-effort and callers swallow errors.
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.db,
            user=self.user,
            password=self.password,
            connect_timeout=10,
        )

    def ensure_schema(self) -> None:
        """Create the meta schema + table + indexes if absent (idempotent)."""
        if self._schema_ready:
            return
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{META_SCHEMA}"')
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{META_SCHEMA}".{_RUN_HISTORY_TABLE} (
                        id            BIGSERIAL PRIMARY KEY,
                        pipeline_key  TEXT        NOT NULL,
                        table_name    TEXT        NOT NULL,
                        environment   TEXT        NOT NULL,
                        sha           TEXT,
                        mode          TEXT        NOT NULL,
                        status        TEXT        NOT NULL,
                        started_at    TIMESTAMPTZ NOT NULL,
                        finished_at   TIMESTAMPTZ NOT NULL,
                        duration_s    DOUBLE PRECISION NOT NULL,
                        row_count     BIGINT,
                        schema_hash   TEXT,
                        error         TEXT,
                        drift_summary JSONB,
                        timings       JSONB,
                        anomaly       JSONB
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS run_history_pipeline_started_idx
                    ON "{META_SCHEMA}".{_RUN_HISTORY_TABLE} (pipeline_key, started_at DESC)
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS run_history_status_idx
                    ON "{META_SCHEMA}".{_RUN_HISTORY_TABLE} (status, started_at DESC)
                    """
                )
            conn.commit()
            self._schema_ready = True
        finally:
            conn.close()

    def record(self, rec: RunRecord) -> int:
        """Insert one run record, returning its generated ``id``."""
        self.ensure_schema()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO "{META_SCHEMA}".{_RUN_HISTORY_TABLE} (
                        pipeline_key, table_name, environment, sha, mode, status,
                        started_at, finished_at, duration_s, row_count, schema_hash,
                        error, drift_summary, timings, anomaly
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                    """,
                    (
                        rec.pipeline_key,
                        rec.table_name,
                        rec.environment,
                        rec.sha,
                        rec.mode,
                        rec.status,
                        rec.started_at,
                        rec.finished_at,
                        rec.duration_s,
                        rec.row_count,
                        rec.schema_hash,
                        rec.error,
                        psycopg2.extras.Json(rec.drift_summary)
                        if rec.drift_summary is not None
                        else None,
                        psycopg2.extras.Json(rec.timings) if rec.timings else None,
                        psycopg2.extras.Json(rec.anomaly)
                        if rec.anomaly is not None
                        else None,
                    ),
                )
                row = cur.fetchone()
                new_id = int(row[0]) if row is not None else 0
            conn.commit()
            rec.id = new_id
            return new_id
        finally:
            conn.close()

    def _rows_to_records(self, rows: list[tuple[Any, ...]]) -> list[RunRecord]:
        records: list[RunRecord] = []
        for r in rows:
            records.append(
                RunRecord(
                    id=r[0],
                    pipeline_key=r[1],
                    table_name=r[2],
                    environment=r[3],
                    sha=r[4],
                    mode=r[5],
                    status=r[6],
                    started_at=r[7],
                    finished_at=r[8],
                    duration_s=r[9],
                    row_count=r[10],
                    schema_hash=r[11],
                    error=r[12],
                    drift_summary=r[13],
                    timings=r[14] or {},
                    anomaly=r[15],
                )
            )
        return records

    _SELECT_COLUMNS = (
        "id, pipeline_key, table_name, environment, sha, mode, status, "
        "started_at, finished_at, duration_s, row_count, schema_hash, error, "
        "drift_summary, timings, anomaly"
    )

    def recent_successful(
        self, pipeline_key: str, limit: int = 20, environment: str | None = None
    ) -> list[RunRecord]:
        """Most recent successful runs for a pipeline, newest first.

        Pass ``environment`` to scope the baseline to one environment so a dev
        run's tiny row counts never contaminate the prod anomaly baseline.
        """
        self.ensure_schema()
        where = "pipeline_key = %s AND status = 'ok'"
        params: list[Any] = [pipeline_key]
        if environment is not None:
            where += " AND environment = %s"
            params.append(environment)
        params.append(limit)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {self._SELECT_COLUMNS}
                    FROM "{META_SCHEMA}".{_RUN_HISTORY_TABLE}
                    WHERE {where}
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                return self._rows_to_records(cur.fetchall())
        finally:
            conn.close()

    def last_successful(
        self, pipeline_key: str, environment: str | None = None
    ) -> RunRecord | None:
        recent = self.recent_successful(pipeline_key, limit=1, environment=environment)
        return recent[0] if recent else None

    def last_run_per_pipeline(
        self, environment: str | None = None
    ) -> dict[str, RunRecord]:
        """Most recent run (any status) per pipeline — feeds the staleness monitor.

        Pass ``environment`` to scope to one environment (the monitor checks
        prod freshness, so dev runs of the same pipeline don't mask staleness).
        """
        self.ensure_schema()
        where = "" if environment is None else "WHERE environment = %s"
        params: tuple[Any, ...] = () if environment is None else (environment,)
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (pipeline_key) {self._SELECT_COLUMNS}
                    FROM "{META_SCHEMA}".{_RUN_HISTORY_TABLE}
                    {where}
                    ORDER BY pipeline_key, started_at DESC
                    """,
                    params,
                )
                return {
                    rec.pipeline_key: rec
                    for rec in self._rows_to_records(cur.fetchall())
                }
        finally:
            conn.close()

    def recent(self, limit: int = 100) -> list[RunRecord]:
        """Most recent runs across all pipelines — feeds ``GET /v1/runs``."""
        self.ensure_schema()
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {self._SELECT_COLUMNS}
                    FROM "{META_SCHEMA}".{_RUN_HISTORY_TABLE}
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return self._rows_to_records(cur.fetchall())
        finally:
            conn.close()


__all__ = ["META_SCHEMA", "RunHistoryStore", "RunRecord", "run_context"]
