"""Portable architecture checker for poorbricks table repos.

Verifies pipeline file structure and framework compliance using AST parsing
only — no imports, no Spark, no network. Runs fast enough for pre-commit and
CI. Designed to work both inside the framework repo and in any downstream repo
that installs poorbricks-framework as a wheel.

Public API:
    check_architecture(tables_root=None) -> list[ArchError]
        Returns an empty list if all checks pass; otherwise a list of errors.
        Each ArchError names the pipeline directory and describes the violation.

    tables_root resolution order:
        1. Explicit ``tables_root`` argument
        2. ``TABLES_ROOT`` environment variable
        3. ``CWD/tables/``
"""

from __future__ import annotations

import ast
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SNAKE_RE = re.compile(r"^[a-z][a-z0-9_]*$")

_REQUIRED_FILES = [
    "__init__.py",
    "config.py",
    "pipeline.py",
    "transform.py",
    "fixtures.py",
    "test_pipeline.py",
]

_SILVER_GOLD_LEVELS = {"silver", "gold"}


@dataclass
class ArchError:
    """A single architecture violation found in a pipeline directory."""

    pipeline_dir: str
    message: str

    def format(self) -> str:
        return f"{self.pipeline_dir}: {self.message}"


def check_architecture(tables_root: Path | None = None) -> list[ArchError]:
    """Check all pipeline directories under ``tables_root`` for compliance.

    Checks performed (AST-based, no imports):
    1. All six required files are present.
    2. ``pipeline.py`` imports from ``poorbricks`` (not legacy @dlt.table).
    3. ``fixtures.py`` declares at least one ``@scenario(...)`` call.
    4. ``config.py`` declares an ``Expectations`` subclass.
    5. Silver/gold ``config.py`` declares a non-empty ``UNIQUE_KEYS``.
    6. ``pipeline.py`` does not call ``dbutils.secrets`` in the function body.
    """
    root = _resolve_tables_root(tables_root)
    if not root.exists():
        return [
            ArchError(pipeline_dir=str(root), message="tables directory does not exist")
        ]

    errors: list[ArchError] = []
    for pipeline_py in sorted(root.rglob("pipeline.py")):
        if "__pycache__" in pipeline_py.parts:
            continue
        pipeline_dir = pipeline_py.parent
        rel = pipeline_dir.as_posix()
        errors.extend(_check_pipeline_dir(pipeline_dir, rel))

    # Stubs are an architecture violation: a schema column projected as a
    # placeholder constant silently breaks downstream consumers. The AST
    # detector already exists — surface its findings as ArchErrors so
    # ``verify --mode arch`` (CI) enforces "stubs are never used".
    from .verification.no_stubs import find_stubs

    for finding in find_stubs(root):
        errors.append(
            ArchError(
                pipeline_dir=finding.file.parent.as_posix(),
                message=finding.format(),
            )
        )
    return errors


def _resolve_tables_root(tables_root: Path | None) -> Path:
    if tables_root is not None:
        return tables_root
    env = os.environ.get("TABLES_ROOT")
    if env:
        return Path(env)
    from .settings import settings

    return settings.tables_root


def check_pipeline_dir(pipeline_dir: Path) -> list[ArchError]:
    """Run all architecture checks on a single pipeline directory."""
    return _check_pipeline_dir(pipeline_dir, pipeline_dir.as_posix())


def _check_pipeline_dir(pipeline_dir: Path, rel: str) -> list[ArchError]:
    errors: list[ArchError] = []

    # 1. Required files
    for required in _REQUIRED_FILES:
        if not (pipeline_dir / required).exists():
            errors.append(ArchError(pipeline_dir=rel, message=f"missing {required}"))

    # 2. Uses poorbricks framework decorator
    pipeline_path = pipeline_dir / "pipeline.py"
    if pipeline_path.exists():
        text = pipeline_path.read_text(encoding="utf-8")
        if "from poorbricks" not in text:
            errors.append(
                ArchError(
                    pipeline_dir=rel,
                    message="pipeline.py does not import from poorbricks — "
                    "use @pipeline(...) from poorbricks instead of @dlt.table",
                )
            )
        if "dbutils.secrets" in text:
            errors.append(
                ArchError(
                    pipeline_dir=rel,
                    message="pipeline.py calls dbutils.secrets in the function body — "
                    "use environment-based settings (MONGO_URI, etc.) instead",
                )
            )

    # 3. @scenario() in fixtures.py
    fixtures_path = pipeline_dir / "fixtures.py"
    if fixtures_path.exists():
        if "@scenario(" not in fixtures_path.read_text(encoding="utf-8"):
            errors.append(
                ArchError(
                    pipeline_dir=rel,
                    message="fixtures.py must declare at least one @scenario(...) function",
                )
            )

    # 4. Expectations subclass in config.py
    config_path = pipeline_dir / "config.py"
    if config_path.exists():
        tree = ast.parse(config_path.read_text(encoding="utf-8"))
        has_expectations = any(
            isinstance(node, ast.ClassDef) and _inherits_from(node, "Expectations")
            for node in ast.walk(tree)
        )
        if not has_expectations:
            errors.append(
                ArchError(
                    pipeline_dir=rel,
                    message="config.py is missing a subclass of Expectations",
                )
            )

        # 5. UNIQUE_KEYS for silver/gold
        level = _read_pipeline_level(pipeline_dir)
        if level in _SILVER_GOLD_LEVELS:
            unique_keys = _read_class_attr_value(
                config_path, "Expectations", "UNIQUE_KEYS"
            )
            if not unique_keys:
                errors.append(
                    ArchError(
                        pipeline_dir=rel,
                        message=f"level={level} pipeline must declare non-empty "
                        "Expectations.UNIQUE_KEYS in config.py",
                    )
                )

        # 6. snake_case field names on ValidatedStruct subclasses
        errors.extend(_check_snake_case_fields(config_path, rel))

    return errors


def _inherits_from(class_def: ast.ClassDef, base_name: str) -> bool:
    return any(
        (isinstance(b, ast.Name) and b.id == base_name)
        or (isinstance(b, ast.Attribute) and b.attr == base_name)
        for b in class_def.bases
    )


def _read_pipeline_level(pipeline_dir: Path) -> str | None:
    pipeline_path = pipeline_dir / "pipeline.py"
    if not pipeline_path.exists():
        return None
    tree = ast.parse(pipeline_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_pipeline = (isinstance(func, ast.Name) and func.id == "pipeline") or (
            isinstance(func, ast.Attribute) and func.attr == "pipeline"
        )
        if is_pipeline:
            for kw in node.keywords:
                if kw.arg == "level" and isinstance(kw.value, ast.Constant):
                    return str(kw.value.value)
    return None


def _check_snake_case_fields(config_path: Path, rel: str) -> list[ArchError]:
    """Return ArchError for any ValidatedStruct field name that is not snake_case."""
    errors: list[ArchError] = []
    tree = ast.parse(config_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if not _inherits_from(node, "ValidatedStruct"):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            field_name = stmt.target.id
            if not _SNAKE_RE.match(field_name):
                errors.append(
                    ArchError(
                        pipeline_dir=rel,
                        message=(
                            f"{node.name}.{field_name}: field name must be snake_case "
                            "(lowercase letters, digits, and underscores only)"
                        ),
                    )
                )
    return errors


def _read_class_attr_value(path: Path, base_name: str, attr_name: str) -> Any:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or not _inherits_from(node, base_name):
            continue
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name, value = stmt.target.id, stmt.value
            elif (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
            ):
                target_name, value = stmt.targets[0].id, stmt.value
            else:
                continue
            if target_name == attr_name and value is not None:
                try:
                    return ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    return None
    return None


__all__ = [
    "ArchError",
    "check_architecture",
    "check_pipeline_dir",
    "_check_snake_case_fields",
]
