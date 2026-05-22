"""Recover a workflow definition from an already-generated Airflow DAG.

``parse_generated_dag`` reads back the *format-stable* parts of a DAG file
emitted by :mod:`poorbricks.airflow.dag_generator` — the top-level string
constants, the ``_build_task(...)`` calls, and the ``a >> b`` dependency
edges — using :mod:`ast`, without importing Airflow or executing the module.

This is what powers ``POST /v1/regenerate``: a stored DAG already carries its
full task graph, so a workflow can be reconstructed from it and re-rendered
with the current generator — no source workflow YAML needed. Only fields the
generator has always emitted are read, so the parser works on both the
pre-Spot (PVC-mounted) and post-Spot (init-container) DAG layouts.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import TypeGuard

from .workflow import TaskConfig


class DagParseError(ValueError):
    """Raised when a generated DAG file cannot be parsed back to a workflow."""


@dataclass(frozen=True)
class ParsedDag:
    """The format-stable fields recovered from a generated DAG file."""

    schedule: str | None
    tasks: tuple[TaskConfig, ...]
    image: str
    namespace: str
    runtime_secret: str
    postgres_creds_secret: str
    start_year: int


def parse_generated_dag(source: str) -> ParsedDag:
    """Parse a generated Airflow DAG file back into a :class:`ParsedDag`.

    Args:
        source: the Python source of a DAG file produced by
            ``dag_generator.generate_dag_file``.

    Raises:
        DagParseError: the source is not valid Python, is missing a
            format-stable constant, or contains no ``_build_task`` calls.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise DagParseError(f"source is not valid Python: {exc}") from exc

    constants: dict[str, ast.expr] = {}
    var_to_task: dict[str, str] = {}  # task var name -> task id
    task_order: list[str] = []  # task ids in source order
    task_specs: dict[str, tuple[str, str]] = {}  # task id -> (pipeline, command)
    edges: list[tuple[str, str]] = []  # (upstream var, downstream var)

    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if _is_build_task_call(node.value):
                task_id, pipeline, command = _parse_task(node.value)
                if target.id in var_to_task:
                    raise DagParseError(f"duplicate task var: {target.id}")
                var_to_task[target.id] = task_id
                task_order.append(task_id)
                task_specs[task_id] = (pipeline, command)
            else:
                constants[target.id] = node.value
        elif isinstance(node, ast.Expr):
            edge = _extract_edge(node.value)
            if edge is not None:
                edges.append(edge)

    if not task_order:
        raise DagParseError("no _build_task(...) calls found in DAG source")

    depends_on: dict[str, list[str]] = {task_id: [] for task_id in task_order}
    for up_var, down_var in edges:
        if up_var not in var_to_task or down_var not in var_to_task:
            raise DagParseError(
                f"dependency edge references unknown task var: {up_var} >> {down_var}"
            )
        depends_on[var_to_task[down_var]].append(var_to_task[up_var])

    tasks = tuple(
        TaskConfig(
            id=task_id,
            pipeline=task_specs[task_id][0],
            depends_on=tuple(depends_on[task_id]),
            command=task_specs[task_id][1],
        )
        for task_id in task_order
    )

    return ParsedDag(
        schedule=_const_schedule(constants),
        tasks=tasks,
        image=_const_str(constants, "IMAGE"),
        namespace=_const_str(constants, "NAMESPACE"),
        runtime_secret=_const_str(constants, "RUNTIME_SECRET"),
        postgres_creds_secret=_const_str(constants, "POSTGRES_CREDS_SECRET"),
        start_year=_start_year(constants),
    )


def _is_build_task_call(value: ast.expr) -> TypeGuard[ast.Call]:
    """True when ``value`` is a ``_build_task(...)`` call expression."""
    return (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == "_build_task"
    )


def _parse_task(call: ast.Call) -> tuple[str, str, str]:
    """Return ``(task_id, pipeline, command)`` from a ``_build_task`` call."""
    if len(call.args) != 3 or call.keywords:
        raise DagParseError("_build_task must be called with 3 positional args")
    values: list[str] = []
    for arg in call.args:
        value = _literal(arg, "_build_task argument")
        if not isinstance(value, str):
            raise DagParseError(f"_build_task argument must be a string: {value!r}")
        values.append(value)
    return values[0], values[1], values[2]


def _extract_edge(expr: ast.expr) -> tuple[str, str] | None:
    """Return the ``(upstream, downstream)`` var names of an ``a >> b`` expr."""
    if (
        isinstance(expr, ast.BinOp)
        and isinstance(expr.op, ast.RShift)
        and isinstance(expr.left, ast.Name)
        and isinstance(expr.right, ast.Name)
    ):
        return expr.left.id, expr.right.id
    return None


def _literal(node: ast.expr, label: str) -> object:
    """Evaluate ``node`` as a Python literal, or raise ``DagParseError``."""
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError, TypeError) as exc:
        raise DagParseError(f"{label} is not a literal: {exc}") from exc


def _const_str(constants: dict[str, ast.expr], name: str) -> str:
    """Return a required top-level string constant."""
    if name not in constants:
        raise DagParseError(f"missing top-level constant {name}")
    value = _literal(constants[name], name)
    if not isinstance(value, str):
        raise DagParseError(f"{name} must be a string, got {value!r}")
    return value


def _const_schedule(constants: dict[str, ast.expr]) -> str | None:
    """Return the ``SCHEDULE`` constant (a cron string, or None for manual)."""
    if "SCHEDULE" not in constants:
        raise DagParseError("missing top-level constant SCHEDULE")
    value = _literal(constants["SCHEDULE"], "SCHEDULE")
    if value is not None and not isinstance(value, str):
        raise DagParseError(f"SCHEDULE must be a string or None, got {value!r}")
    return value


def _start_year(constants: dict[str, ast.expr]) -> int:
    """Return the year from ``START_DATE = datetime(YYYY, 1, 1)``."""
    if "START_DATE" not in constants:
        raise DagParseError("missing top-level constant START_DATE")
    node = constants["START_DATE"]
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "datetime"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, int)
    ):
        return node.args[0].value
    raise DagParseError("START_DATE must be of the form datetime(YYYY, 1, 1)")


__all__ = ["DagParseError", "ParsedDag", "parse_generated_dag"]
