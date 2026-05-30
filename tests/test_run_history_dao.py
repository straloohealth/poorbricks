"""Integration tests for the run-history DAO (req 8/9/11) against local Postgres."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from poorbricks.run_history import RunHistoryStore, RunRecord

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("run_history_dao")]

_KEY = "postgres:_rh_test_table"


def _rec(env: str, status: str, rows: int, ago_min: int) -> RunRecord:
    ts = datetime.now(UTC) - timedelta(minutes=ago_min)
    return RunRecord(
        pipeline_key=_KEY,
        table_name="_rh_test_table",
        environment=env,
        mode="production",
        status=status,
        started_at=ts,
        finished_at=ts,
        duration_s=1.0,
        row_count=rows,
    )


@pytest.fixture
def store() -> Iterator[RunHistoryStore]:
    st = RunHistoryStore()
    st.ensure_schema()
    conn = st._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM poorbricks_meta.run_history WHERE pipeline_key = %s",
                (_KEY,),
            )
        conn.commit()
    finally:
        conn.close()
    yield st
    conn = st._connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM poorbricks_meta.run_history WHERE pipeline_key = %s",
                (_KEY,),
            )
        conn.commit()
    finally:
        conn.close()


def test_record_and_recent_successful_is_env_scoped(store: RunHistoryStore) -> None:
    store.record(_rec("prod", "ok", 1000, ago_min=30))
    store.record(_rec("prod", "ok", 1010, ago_min=20))
    store.record(_rec("prod", "failed", 0, ago_min=10))
    store.record(_rec("dev", "ok", 3, ago_min=5))

    prod = store.recent_successful(_KEY, environment="prod")
    assert [r.row_count for r in prod] == [1010, 1000]  # newest first, ok only
    dev = store.recent_successful(_KEY, environment="dev")
    assert [r.row_count for r in dev] == [3]
    # Unscoped sees both environments' successes.
    assert len(store.recent_successful(_KEY)) == 3


def test_last_run_per_pipeline_scopes_to_environment(store: RunHistoryStore) -> None:
    store.record(_rec("prod", "ok", 1000, ago_min=60))
    store.record(_rec("dev", "ok", 3, ago_min=1))  # more recent, but dev

    prod_last = store.last_run_per_pipeline(environment="prod")
    assert _KEY in prod_last
    assert prod_last[_KEY].environment == "prod"
    assert prod_last[_KEY].row_count == 1000
