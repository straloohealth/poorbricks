"""Airflow integration for poorbricks.

Provides:

* ``workflow`` — YAML schema for a workflow definition (``WorkflowConfig``,
  ``TaskConfig``) and parser.
* ``dag_generator`` — turns a ``WorkflowConfig`` into Python source for an
  Airflow DAG using ``KubernetesPodOperator``.
* ``dag_store`` — abstraction over where DAG files land (local FS or GCS),
  with prefix-scoped prune so removing a workflow YAML removes its DAG.
"""

from __future__ import annotations

from .dag_generator import generate_dag_file
from .dag_store import DagStore, LocalDagStore
from .workflow import (
    TaskConfig,
    WorkflowConfig,
    WorkflowParseError,
    load_workflow,
    load_workflows,
)

__all__ = [
    "DagStore",
    "LocalDagStore",
    "TaskConfig",
    "WorkflowConfig",
    "WorkflowParseError",
    "generate_dag_file",
    "load_workflow",
    "load_workflows",
]
