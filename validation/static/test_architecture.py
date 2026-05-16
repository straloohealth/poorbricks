"""Architecture gate tests for the framework's own tables/ directory.

Structural and compliance checks (file presence, poorbricks import, @scenario,
Expectations, UNIQUE_KEYS, no dbutils.secrets) live in
``poorbricks.arch.check_architecture()`` and are exercised in CI via
``poorbricks-verify --mode arch`` — not duplicated here.

What this module enforces:
- Legacy-pattern scan: no pipeline may still use ``@dlt.table``.
- Documentation quality (catalog-specific): every ``ValidatedStruct`` must
  have a class docstring and every field must use ``Field(description=...)``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_ALLOWLIST_PATH = _REPO_ROOT / "validation" / "_framework_allowlist.txt"


def _load_allowlist() -> set[str]:
    if not _ALLOWLIST_PATH.exists():
        return set()
    return {
        line.strip()
        for line in _ALLOWLIST_PATH.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def _enforced_pipeline_dirs() -> list[Path]:
    allowlist = _load_allowlist()
    pipeline_dirs: list[Path] = []
    for pipeline_py in (_REPO_ROOT / "tables").rglob("pipeline.py"):
        if "__pycache__" in pipeline_py.parts:
            continue
        rel_dir = pipeline_py.parent.relative_to(_REPO_ROOT).as_posix()
        if rel_dir in allowlist:
            continue
        pipeline_dirs.append(pipeline_py.parent)
    return pipeline_dirs


def _inherits_from(class_def: ast.ClassDef, base_name: str) -> bool:
    return any(
        (isinstance(b, ast.Name) and b.id == base_name)
        or (isinstance(b, ast.Attribute) and b.attr == base_name)
        for b in class_def.bases
    )


def _is_field_with_description(value: ast.expr | None) -> bool:
    if value is None or not isinstance(value, ast.Call):
        return False
    func = value.func
    is_field_call = (isinstance(func, ast.Name) and func.id == "Field") or (
        isinstance(func, ast.Attribute) and func.attr == "Field"
    )
    if not is_field_call:
        return False
    return any(
        kw.arg == "description"
        and isinstance(kw.value, ast.Constant)
        and isinstance(kw.value.value, str)
        and kw.value.value.strip()
        for kw in value.keywords
    )


# ---------------------------------------------------------------------------
# Legacy pattern scan
# ---------------------------------------------------------------------------


class _LegacyPipelineVisitor(ast.NodeVisitor):
    """AST visitor that finds legacy @dlt.table decorated functions."""

    def __init__(self) -> None:
        self.dlt_functions: list[tuple[str, int]] = []  # (name, lineno)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        for decorator in node.decorator_list:
            has_dlt = False
            if isinstance(decorator, ast.Call) and isinstance(
                decorator.func, ast.Attribute
            ):
                has_dlt = (
                    isinstance(decorator.func.value, ast.Name)
                    and decorator.func.value.id == "dlt"
                    and decorator.func.attr == "table"
                )
            elif isinstance(decorator, ast.Attribute):
                has_dlt = (
                    isinstance(decorator.value, ast.Name)
                    and decorator.value.id == "dlt"
                    and decorator.attr == "table"
                )
            if has_dlt:
                self.dlt_functions.append((node.name, node.lineno))
        self.generic_visit(node)


class TestLegacyArchitecture:
    """Ensure no pipeline still uses the legacy @dlt.table decorator.

    All pipelines must have migrated to @pipeline(...) from poorbricks.
    """

    def test_no_pipeline_uses_dlt_table(self) -> None:
        tables_root = _REPO_ROOT / "tables"
        if not tables_root.exists():
            pytest.skip("tables directory not found")

        violations: list[str] = []
        for pipeline_py in tables_root.rglob("pipeline.py"):
            if "__pycache__" in pipeline_py.parts:
                continue
            tree = ast.parse(pipeline_py.read_text(encoding="utf-8"))
            visitor = _LegacyPipelineVisitor()
            visitor.visit(tree)
            for func_name, lineno in visitor.dlt_functions:
                rel = pipeline_py.relative_to(_REPO_ROOT).as_posix()
                violations.append(
                    f"{rel}:{lineno} — {func_name!r} uses @dlt.table. "
                    f"Migrate to @pipeline(...) from poorbricks."
                )
        if violations:
            pytest.fail("\n".join(violations))


# ---------------------------------------------------------------------------
# Documentation quality (catalog-specific, not portable)
# ---------------------------------------------------------------------------

_PHASE_0_PENDING: set[str] = {
    "appointments",
    "meta.catalog",
    "reports.roi.sub_reports.surgical_recommendations",
    "status.aon_monthly_status",
}


def _pipeline_dir_to_key(pipeline_dir: Path) -> str:
    parts = pipeline_dir.relative_to(_REPO_ROOT / "tables").parts
    return ".".join(parts)


def _enforced_for_docs() -> list[Path]:
    return [
        d
        for d in _enforced_pipeline_dirs()
        if _pipeline_dir_to_key(d) not in _PHASE_0_PENDING
    ]


class TestDocumentationQuality:
    """Catalog-specific requirements: class docstrings and Field(description=...)."""

    def test_migrated_pipelines_have_class_docstring(self) -> None:
        """ValidatedStruct in config.py must have a class docstring."""
        violations: list[str] = []
        for pipeline_dir in _enforced_pipeline_dirs():
            config_path = next(
                (
                    pipeline_dir / name
                    for name in ("schema.py", "config.py")
                    if (pipeline_dir / name).exists()
                ),
                None,
            )
            if config_path is None:
                continue
            tree = ast.parse(config_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                if not _inherits_from(node, "ValidatedStruct"):
                    continue
                if not (ast.get_docstring(node) or "").strip():
                    rel = config_path.relative_to(_REPO_ROOT).as_posix()
                    violations.append(
                        f"{rel}::{node.name} must have a class docstring "
                        f"describing the dataset (used by the catalog)."
                    )
        if violations:
            pytest.fail("\n".join(violations))

    def test_migrated_pipelines_have_field_descriptions(self) -> None:
        """Every Pydantic field must have Field(description=...) for catalog consumption."""
        violations: list[str] = []
        for pipeline_dir in _enforced_pipeline_dirs():
            config_path = next(
                (
                    pipeline_dir / name
                    for name in ("schema.py", "config.py")
                    if (pipeline_dir / name).exists()
                ),
                None,
            )
            if config_path is None:
                continue
            tree = ast.parse(config_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                if not _inherits_from(node, "ValidatedStruct"):
                    continue
                for stmt in node.body:
                    if not isinstance(stmt, ast.AnnAssign) or not isinstance(
                        stmt.target, ast.Name
                    ):
                        continue
                    if not _is_field_with_description(stmt.value):
                        rel = config_path.relative_to(_REPO_ROOT).as_posix()
                        violations.append(
                            f"{rel}::{node.name}.{stmt.target.id}: must use "
                            f'`Field(description="...")`.'
                        )
        if violations:
            pytest.fail("\n".join(violations))
