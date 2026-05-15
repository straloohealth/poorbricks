"""Import every framework-registered pipeline to populate the registry.

This traverses tables/**/pipeline.py and imports each module that imports
from poorbricks, which triggers @pipeline decorators to register themselves.

Safe to call multiple times (idempotent).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_ROOT = REPO_ROOT / "tables"


def discover_all_pipelines() -> None:
    """Import every pipeline.py that uses `from poorbricks`.

    Populates the pipeline registry so all_pipelines() and list_pipelines()
    return results.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    for pipeline_path in sorted(PIPELINES_ROOT.rglob("pipeline.py")):
        if "__pycache__" in pipeline_path.parts:
            continue

        text = pipeline_path.read_text(encoding="utf-8")
        if "from poorbricks" not in text:
            continue

        rel = pipeline_path.relative_to(REPO_ROOT)
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


__all__ = ["PIPELINES_ROOT", "REPO_ROOT", "discover_all_pipelines"]
