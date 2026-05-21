"""Tests for poorbricks.airflow.workflow."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from typing import Annotated

import pytest
from pyspark.sql import DataFrame

from poorbricks import Inputs
from poorbricks.airflow.workflow import (
    TaskConfig,
    WorkflowConfig,
    WorkflowParseError,
    derive_task_dependencies,
    load_workflow,
    load_workflows,
)
from poorbricks.depgraph import CycleError
from poorbricks.inputs import MongoSource, TableSource
from validation import ValidatedStruct


class _BronzeModel(ValidatedStruct):
    id: str


class _BronzeInputs(Inputs):
    raw: Annotated[
        DataFrame,
        MongoSource(db="d", collection="c", schema=_BronzeModel.to_struct()),
    ]


class _SilverInputs(Inputs):
    up: Annotated[DataFrame, TableSource("bronze_tbl", _BronzeModel)]


class _CycleXInputs(Inputs):
    up: Annotated[DataFrame, TableSource("y_tbl", _BronzeModel)]


class _CycleYInputs(Inputs):
    up: Annotated[DataFrame, TableSource("x_tbl", _BronzeModel)]


def _pipeline_ns(table_name: str, storage: str, inputs_cls: type) -> object:
    return SimpleNamespace(
        table_name=table_name,
        target_storage=storage,
        module=f"tables.{table_name}.pipeline",
        inputs_cls=inputs_cls,
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


def test_depends_on_in_yaml_rejected(tmp_path: Path) -> None:
    """depends_on is derived from pipeline inputs — declaring it is an error."""
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
    with pytest.raises(WorkflowParseError, match="depends_on is not accepted"):
        load_workflow(path)


def test_load_workflow_manual_schedule(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "wf.yaml",
        """
        name: manual_dag
        schedule: manual
        tasks:
          - id: t
            pipeline: postgres:t
        """,
    )
    wf = load_workflow(path)
    assert wf.schedule is None


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


def test_derive_task_dependencies_from_inputs() -> None:
    """A task reading another task's table gains a derived depends_on edge."""
    pipelines = {
        "delta:bronze_tbl": _pipeline_ns("bronze_tbl", "delta", _BronzeInputs),
        "postgres:silver_tbl": _pipeline_ns("silver_tbl", "postgres", _SilverInputs),
    }
    wf = WorkflowConfig(
        name="wf",
        schedule="0 * * * *",
        tasks=(
            TaskConfig(id="silver", pipeline="postgres:silver_tbl"),
            TaskConfig(id="bronze", pipeline="delta:bronze_tbl"),
        ),
    )
    derived = {t.id: t for t in derive_task_dependencies(wf, pipelines).tasks}
    assert derived["silver"].depends_on == ("bronze",)
    assert derived["bronze"].depends_on == ()


def test_derive_task_dependencies_detects_cycle() -> None:
    """Mutually-dependent pipelines raise CycleError."""
    pipelines = {
        "delta:x_tbl": _pipeline_ns("x_tbl", "delta", _CycleXInputs),
        "delta:y_tbl": _pipeline_ns("y_tbl", "delta", _CycleYInputs),
    }
    wf = WorkflowConfig(
        name="wf",
        schedule="0 * * * *",
        tasks=(
            TaskConfig(id="x", pipeline="delta:x_tbl"),
            TaskConfig(id="y", pipeline="delta:y_tbl"),
        ),
    )
    with pytest.raises(CycleError):
        derive_task_dependencies(wf, pipelines)


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
