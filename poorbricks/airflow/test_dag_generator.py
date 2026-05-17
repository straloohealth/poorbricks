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
        table_repo_url="https://github.com/org/table-repo.git",
        table_repo_sha="deadbeef",
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
        table_repo_url="https://github.com/org/repo.git",
        table_repo_sha="cafebabe",
    )
    assert "table_repo__gold_patients" in source
    assert "'postgres:patients'" in source
    assert "'postgres:gold_summary'" in source
    assert "task_patients >> task_gold_summary" in source
    assert "cafebabe" in source
    assert "KubernetesPodOperator" in source
    assert "poorbricks" in source
    assert "production" in source


def test_repo_clone_secret_threads_through(tmp_path: object) -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
        table_repo_url="git@example",
        table_repo_sha="sha",
        repo_clone_secret="repo-clone-r",
    )
    assert "repo-clone-r" in source
    assert "/root/.ssh" in source
    ast.parse(source)


def test_no_clone_secret_emits_no_ssh_mount() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
        table_repo_url="git@example",
        table_repo_sha="sha",
    )
    assert "/root/.ssh" not in source
    assert "repo-clone-key" not in source


def test_invalid_prefix_rejected() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    with pytest.raises(ValueError, match="prefix"):
        generate_dag_file(
            wf,
            prefix="bad prefix!",
            image="img",
            table_repo_url="x",
            table_repo_sha="y",
        )


def test_dependencies_omitted_when_none() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
        table_repo_url="git@example",
        table_repo_sha="sha",
    )
    ast.parse(source)
    assert "no inter-task dependencies" in source
