"""Tests for poorbricks.airflow.dag_generator."""

from __future__ import annotations

import ast

import pytest

from poorbricks.airflow.dag_generator import generate_dag_file
from poorbricks.airflow.workflow import TaskConfig, WorkflowConfig


def _wf(tasks: tuple[TaskConfig, ...]) -> WorkflowConfig:
    return WorkflowConfig(
        name="gold_patients",
        schedule="0 2 * * *",
        tasks=tasks,
    )


def test_generated_dag_is_valid_python() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="postgres:patients"),
            TaskConfig(
                id="gold_summary",
                pipeline="postgres:gold_summary",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="table-repo",
        image="docker.io/danielspeixoto/databricks:abc123",
    )
    ast.parse(source)


def test_generated_dag_references_keys() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="postgres:patients"),
            TaskConfig(
                id="gold_summary",
                pipeline="postgres:gold_summary",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="table_repo",
        image="img:abc",
    )
    assert "table_repo__gold_patients" in source
    assert "'postgres:patients'" in source
    assert "'postgres:gold_summary'" in source
    assert "task_patients >> task_gold_summary" in source
    assert "KubernetesPodOperator" in source
    assert "poorbricks" in source
    assert "production" in source


def test_check_command_renders_check_arguments() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="gold.patients"),
            TaskConfig(
                id="verify",
                pipeline="gold.patients",
                command="check",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="table_repo",
        image="img:abc",
    )
    ast.parse(source)
    assert "'check'" in source
    assert "'run'" in source
    assert '"run": ["run", "--mode", "production"]' in source
    assert '"check": ["check"]' in source
    assert "task_patients >> task_verify" in source


def test_pvc_volume_and_subpath_present() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="my-prefix",
        image="img",
    )
    ast.parse(source)
    assert "V1PersistentVolumeClaimVolumeSource" in source
    assert "airflow-dags" in source
    assert "'__code__/my-prefix'" in source
    assert "/workspace" in source
    assert "TABLES_ROOT" in source
    assert "poorbricks.io/dags" in source
    # The old git init container must be gone.
    assert "git clone" not in source
    assert "alpine/git" not in source


def test_custom_node_selector_propagates() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
        node_selector={"role": "etl"},
    )
    assert "'role': 'etl'" in source


def test_invalid_prefix_rejected() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    with pytest.raises(ValueError, match="prefix"):
        generate_dag_file(
            wf,
            prefix="bad prefix!",
            image="img",
        )


def test_dependencies_omitted_when_none() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
    )
    ast.parse(source)
    assert "no inter-task dependencies" in source
