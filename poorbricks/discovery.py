"""Import every framework-registered pipeline to populate the registry.

This traverses ``<tables_root>/**/pipeline.py`` and imports each module that
imports from ``poorbricks``, which triggers ``@pipeline`` decorators to
register themselves.

Tables root resolution order:
1. Explicit ``tables_root`` argument to ``discover_all_pipelines``
2. ``TABLES_ROOT`` environment variable
3. ``CWD/tables/`` (default — works in any repo using ``poorbricks`` as a lib)

Safe to call multiple times (idempotent).
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def _resolve_roots(override: Path | None) -> tuple[Path, Path]:
    """Resolve (repo_root, pipelines_root) per the documented precedence."""
    if override is not None:
        tables_root = Path(override).resolve()
        return tables_root.parent, tables_root
    env = os.environ.get("TABLES_ROOT")
    if env:
        tables_root = Path(env).resolve()
        return tables_root.parent, tables_root
    cwd = Path.cwd().resolve()
    return cwd, cwd / "tables"


def discover_all_pipelines(tables_root: Path | None = None) -> None:
    """Import every pipeline.py that uses ``from poorbricks``.

    Populates the pipeline registry so ``all_pipelines()`` and
    ``list_pipelines()`` return results.
    """
    repo_root, pipelines_root = _resolve_roots(tables_root)

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    for pipeline_path in sorted(pipelines_root.rglob("pipeline.py")):
        if "__pycache__" in pipeline_path.parts:
            continue

        text = pipeline_path.read_text(encoding="utf-8")
        if "from poorbricks" not in text:
            continue

        rel = pipeline_path.relative_to(repo_root)
        module_path = ".".join(rel.with_suffix("").parts)

        try:
            importlib.import_module(module_path)
            fixtures_module = module_path.removesuffix(".pipeline") + ".fixtures"
            try:
                importlib.import_module(fixtures_module)
            except ImportError:
                pass
        except Exception as exc:
            print(
                f"[discover] WARN: failed to import {module_path}: {exc}",
                file=sys.stderr,
            )


# Back-compat constants — resolved at import time from the default precedence.
REPO_ROOT, PIPELINES_ROOT = _resolve_roots(None)


__all__ = ["PIPELINES_ROOT", "REPO_ROOT", "discover_all_pipelines"]
