"""Workflow YAML schema and parser.

A workflow YAML names a cron schedule and a DAG of pipeline tasks. The
parsed form is consumed by ``dag_generator.generate_dag_file`` to emit
Airflow Python source.

Schema (locked — established by ``test_table_repo/workflows/test_workflow.yaml``):

.. code-block:: yaml

    name: gold_patients
    schedule: "0 2 * * *"   # cron, or "manual" for trigger-only DAGs
    # image: optional — defaults to constants.DEFAULT_WORKER_IMAGE

    tasks:
      - id: patients
        pipeline: postgres:patients
      - id: gold_summary
        pipeline: postgres:gold_summary

Task execution order is **not** declared here — it is derived from each
pipeline's ``TableSource`` / ``ContractSource`` inputs by
``derive_task_dependencies`` so the DAG can never drift from the real data
flow. A ``depends_on`` key in the YAML is rejected.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter


class WorkflowParseError(ValueError):
    """Raised when a workflow YAML cannot be parsed or validated."""

    def __init__(self, path: Path, message: str) -> None:
        super().__init__(f"{path}: {message}")
        self.path = path
        self.message = message


_ALLOWED_COMMANDS = ("run", "check")
_ALLOWED_WORKFLOW_KEYS = {"name", "schedule", "image", "tasks"}
_ALLOWED_TASK_KEYS = {"id", "pipeline", "command"}


@dataclass(frozen=True)
class TaskConfig:
    id: str
    pipeline: str
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    command: str = "run"


@dataclass(frozen=True)
class WorkflowConfig:
    name: str
    schedule: str | None  # None → manual-trigger-only DAG (schedule: manual in YAML)
    tasks: tuple[TaskConfig, ...]
    image: str | None = None


def load_workflow(path: Path) -> WorkflowConfig:
    """Parse a single workflow YAML file."""
    raw = _read_yaml(path)
    return _build_workflow(path, raw)


def load_workflows(directory: Path) -> list[WorkflowConfig]:
    """Parse every ``*.yaml`` / ``*.yml`` file in ``directory``."""
    if not directory.exists():
        raise FileNotFoundError(f"workflows directory does not exist: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"not a directory: {directory}")
    files = sorted(list(directory.glob("*.yaml")) + list(directory.glob("*.yml")))
    return [load_workflow(f) for f in files]


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text()
    except OSError as exc:
        raise WorkflowParseError(path, f"cannot read file: {exc}") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise WorkflowParseError(path, f"invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise WorkflowParseError(path, "top-level must be a mapping")
    return data


def _build_workflow(path: Path, raw: dict[str, Any]) -> WorkflowConfig:
    unused_keys = set(raw.keys()) - _ALLOWED_WORKFLOW_KEYS
    if unused_keys:
        raise WorkflowParseError(
            path, f"unknown keys in workflow: {sorted(unused_keys)}"
        )
    name = _required_str(path, raw, "name")
    schedule_raw = _required_str(path, raw, "schedule")
    if schedule_raw.strip().lower() == "manual":
        schedule: str | None = None
    elif croniter.is_valid(schedule_raw):
        schedule = schedule_raw
    else:
        raise WorkflowParseError(path, f"invalid cron expression: {schedule_raw!r}")
    image_raw = raw.get("image")
    image: str | None
    if image_raw is None:
        image = None
    elif isinstance(image_raw, str) and image_raw.strip():
        image = image_raw.strip()
    else:
        raise WorkflowParseError(path, "image must be a non-empty string when set")

    tasks_raw = raw.get("tasks")
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise WorkflowParseError(path, "tasks must be a non-empty list")

    tasks: list[TaskConfig] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(tasks_raw):
        if not isinstance(item, dict):
            raise WorkflowParseError(path, f"task[{index}] must be a mapping")
        if "depends_on" in item:
            raise WorkflowParseError(
                path,
                f"task[{index}].depends_on is not accepted — task order is "
                "derived automatically from pipeline inputs",
            )
        unused_task_keys = set(item.keys()) - _ALLOWED_TASK_KEYS
        if unused_task_keys:
            raise WorkflowParseError(
                path,
                f"task[{index}] has unknown keys: {sorted(unused_task_keys)}",
            )
        task_id = _required_str(path, item, "id", context=f"task[{index}]")
        pipeline = _required_str(path, item, "pipeline", context=f"task[{index}]")
        command_raw = item.get("command", "run")
        if not isinstance(command_raw, str) or command_raw not in _ALLOWED_COMMANDS:
            raise WorkflowParseError(
                path,
                f"task[{index}].command must be one of {list(_ALLOWED_COMMANDS)}, "
                f"got {command_raw!r}",
            )
        if task_id in seen_ids:
            raise WorkflowParseError(path, f"duplicate task id: {task_id!r}")
        seen_ids.add(task_id)
        tasks.append(TaskConfig(id=task_id, pipeline=pipeline, command=command_raw))

    return WorkflowConfig(
        name=name,
        schedule=schedule,
        tasks=tuple(tasks),
        image=image,
    )


def _required_str(
    path: Path, data: dict[str, Any], key: str, *, context: str = ""
) -> str:
    value = data.get(key)
    where = f"{context}." if context else ""
    if not isinstance(value, str) or not value.strip():
        raise WorkflowParseError(
            path, f"{where}{key} is required and must be a non-empty string"
        )
    return value.strip()


def _resolve_pipeline(pipeline_key: str, pipelines: dict[str, Any]) -> Any:
    """Resolve a task's ``pipeline`` field to its registered ``PipelineMeta``.

    Accepts both the ``<storage>:<table_name>`` registry-key form and the
    dotted module-path form (``silver.dim_patient``) — mirrors the runner.
    """
    if ":" in pipeline_key:
        storage, table_name = pipeline_key.split(":", 1)
        for meta in pipelines.values():
            if meta.table_name == table_name and meta.target_storage == storage:
                return meta
    else:
        target_module = f"tables.{pipeline_key}.pipeline"
        for meta in pipelines.values():
            if meta.module == target_module:
                return meta
    raise KeyError(pipeline_key)


def derive_task_dependencies(
    workflow: WorkflowConfig, pipelines: dict[str, Any]
) -> WorkflowConfig:
    """Return ``workflow`` with each task's ``depends_on`` derived from data flow.

    A task that reads a table produced by another task in the same workflow
    gains a ``depends_on`` edge on it, so the generated DAG's execution order
    always matches the real ``TableSource`` / ``ContractSource`` dependencies.
    Raises :class:`~poorbricks.depgraph.CycleError` if the edges form a cycle
    and ``KeyError`` if a task names an unregistered pipeline.
    """
    from poorbricks.depgraph import topological_order
    from poorbricks.inputs import ContractSource, TableSource

    task_meta = {
        task.id: _resolve_pipeline(task.pipeline, pipelines) for task in workflow.tasks
    }
    producer_of: dict[str, str] = {
        meta.table_name: task_id for task_id, meta in task_meta.items()
    }
    derived: list[TaskConfig] = []
    for task in workflow.tasks:
        deps: set[str] = set()
        for spec in task_meta[task.id].inputs_cls.sources().values():
            if isinstance(spec, ContractSource | TableSource):
                producer = producer_of.get(spec.table_name)
                if producer is not None and producer != task.id:
                    deps.add(producer)
        derived.append(replace(task, depends_on=tuple(sorted(deps))))
    topological_order({t.id: set(t.depends_on) for t in derived})
    return replace(workflow, tasks=tuple(derived))


__all__ = [
    "TaskConfig",
    "WorkflowConfig",
    "WorkflowParseError",
    "derive_task_dependencies",
    "load_workflow",
    "load_workflows",
]
