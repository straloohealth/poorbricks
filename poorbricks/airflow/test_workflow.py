"""Tests for poorbricks.airflow.workflow."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from poorbricks.airflow.workflow import (
    WorkflowParseError,
    load_workflow,
    load_workflows,
)


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(dedent(body))
    return path


def test_load_workflow_minimal(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: gold_patients
        schedule: "0 2 * * *"
        tasks:
          - id: patients
            pipeline: postgres:patients
        """,
    )
    wf = load_workflow(path)
    assert wf.name == "gold_patients"
    assert wf.schedule == "0 2 * * *"
    assert wf.image is None
    assert len(wf.tasks) == 1
    assert wf.tasks[0].id == "patients"
    assert wf.tasks[0].pipeline == "postgres:patients"
    assert wf.tasks[0].depends_on == ()


def test_load_workflow_with_dependencies(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: multi
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
          - id: b
            pipeline: postgres:b
            depends_on: [a]
        """,
    )
    wf = load_workflow(path)
    assert wf.tasks[1].depends_on == ("a",)


def test_invalid_cron_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: bad
        schedule: "not a cron"
        tasks:
          - id: a
            pipeline: postgres:a
        """,
    )
    with pytest.raises(WorkflowParseError, match="invalid cron"):
        load_workflow(path)


def test_duplicate_task_id_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: dup
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
          - id: a
            pipeline: postgres:b
        """,
    )
    with pytest.raises(WorkflowParseError, match="duplicate task id"):
        load_workflow(path)


def test_unknown_dependency_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: bad_dep
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
            depends_on: [ghost]
        """,
    )
    with pytest.raises(WorkflowParseError, match="unknown task 'ghost'"):
        load_workflow(path)


def test_empty_tasks_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: empty
        schedule: "0 * * * *"
        tasks: []
        """,
    )
    with pytest.raises(WorkflowParseError, match="non-empty list"):
        load_workflow(path)


def test_missing_name_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
        """,
    )
    with pytest.raises(WorkflowParseError, match="name is required"):
        load_workflow(path)


def test_task_command_defaults_to_run(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: solo
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
        """,
    )
    wf = load_workflow(path)
    assert wf.tasks[0].command == "run"


def test_task_command_check_accepted(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: with_check
        schedule: "*/5 * * * *"
        tasks:
          - id: a
            pipeline: gold.patients
          - id: verify
            pipeline: gold.patients
            command: check
            depends_on: [a]
        """,
    )
    wf = load_workflow(path)
    assert wf.tasks[1].command == "check"


def test_task_command_invalid_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: bad_cmd
        schedule: "0 * * * *"
        tasks:
          - id: a
            pipeline: postgres:a
            command: dance
        """,
    )
    with pytest.raises(WorkflowParseError, match="command must be one of"):
        load_workflow(path)


def test_load_workflows_directory(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "a.yaml",
        """
        name: a
        schedule: "0 0 * * *"
        tasks:
          - id: t
            pipeline: postgres:t
        """,
    )
    _write(
        tmp_path,
        "b.yml",
        """
        name: b
        schedule: "0 1 * * *"
        tasks:
          - id: t
            pipeline: postgres:t
        """,
    )
    workflows = load_workflows(tmp_path)
    assert sorted(w.name for w in workflows) == ["a", "b"]
