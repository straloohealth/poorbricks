"""Tests for poorbricks.airflow.dag_parser.

The round-trip tests lock the parser to the generator: if the generated DAG
format drifts in a way the parser cannot read back, CI fails here.
"""

from __future__ import annotations

import pytest

from poorbricks.airflow.dag_generator import generate_dag_file
from poorbricks.airflow.dag_parser import DagParseError, parse_generated_dag
from poorbricks.airflow.workflow import TaskConfig, WorkflowConfig


def test_round_trip_recovers_workflow() -> None:
    """generate_dag_file → parse_generated_dag recovers tasks + every param."""
    workflow = WorkflowConfig(
        name="gold_patients",
        schedule="0 2 * * *",
        tasks=(
            TaskConfig(id="patients", pipeline="postgres:patients"),
            TaskConfig(
                id="gold_summary",
                pipeline="postgres:gold_summary",
                depends_on=("patients",),
                command="check",
            ),
        ),
    )
    source = generate_dag_file(
        workflow,
        prefix="deadpool",
        image="img:abc123",
        namespace="airflow",
        runtime_secret="rt-secret",
        postgres_creds_secret="pg-secret",
        start_year=2025,
    )

    parsed = parse_generated_dag(source)

    assert parsed.schedule == "0 2 * * *"
    assert parsed.tasks == workflow.tasks
    assert parsed.image == "img:abc123"
    assert parsed.namespace == "airflow"
    assert parsed.runtime_secret == "rt-secret"
    assert parsed.postgres_creds_secret == "pg-secret"
    assert parsed.start_year == 2025


def test_round_trip_manual_schedule() -> None:
    """A manual-trigger DAG round-trips with schedule None."""
    workflow = WorkflowConfig(
        name="wf",
        schedule=None,
        tasks=(TaskConfig(id="a", pipeline="postgres:a"),),
    )
    source = generate_dag_file(workflow, prefix="r", image="img")
    assert parse_generated_dag(source).schedule is None


def test_regenerate_is_idempotent() -> None:
    """Re-rendering a parsed DAG yields byte-identical source — so running
    POST /v1/regenerate twice never churns an already-current DAG."""
    workflow = WorkflowConfig(
        name="nightly",
        schedule="30 1 * * *",
        tasks=(
            TaskConfig(id="extract", pipeline="postgres:extract"),
            TaskConfig(
                id="report",
                pipeline="postgres:report",
                depends_on=("extract",),
            ),
        ),
    )
    first = generate_dag_file(workflow, prefix="repo", image="img:v1", start_year=2024)

    parsed = parse_generated_dag(first)
    second = generate_dag_file(
        WorkflowConfig(name="nightly", schedule=parsed.schedule, tasks=parsed.tasks),
        prefix="repo",
        image=parsed.image,
        namespace=parsed.namespace,
        runtime_secret=parsed.runtime_secret,
        postgres_creds_secret=parsed.postgres_creds_secret,
        start_year=parsed.start_year,
    )

    assert first == second


def test_invalid_source_raises() -> None:
    with pytest.raises(DagParseError, match="not valid Python"):
        parse_generated_dag("this is @@ not python")


def test_no_tasks_raises() -> None:
    with pytest.raises(DagParseError, match="no _build_task"):
        parse_generated_dag(
            "SCHEDULE = None\n"
            "IMAGE = 'img'\n"
            "NAMESPACE = 'airflow'\n"
            "RUNTIME_SECRET = 'rt'\n"
            "POSTGRES_CREDS_SECRET = 'pg'\n"
        )


def test_missing_constant_raises() -> None:
    source = (
        "from datetime import datetime\n"
        "NAMESPACE = 'airflow'\n"
        "RUNTIME_SECRET = 'rt'\n"
        "POSTGRES_CREDS_SECRET = 'pg'\n"
        "SCHEDULE = None\n"
        "START_DATE = datetime(2025, 1, 1)\n"
        "task_a = _build_task('a', 'postgres:a', 'run')\n"
    )
    # IMAGE is absent.
    with pytest.raises(DagParseError, match="IMAGE"):
        parse_generated_dag(source)
