"""Locate pipelines and TableSource references via filesystem scan.

Kept independent of the framework registry (which requires importing every
pipeline) so static tests stay fast.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_PIPELINES_ROOT = REPO_ROOT / "tables"

# Match: TableSource("poorbricks_dev.<level>.<table>", <Model>[, …])
# Captures (catalog, level, table, model). Catalog is bound for level-prefix tests.
_TABLE_SOURCE_RE = re.compile(
    r"""TableSource\(\s*["']"""
    r"""(?P<catalog>[a-z_][a-z0-9_]*)"""
    r"""\.(?P<level>bronze|silver|gold)"""
    r"""\.(?P<table>[a-z_][a-z0-9_]*)"""
    r"""["']\s*,\s*"""
    r"""(?P<model>[A-Za-z_][A-Za-z0-9_]*)"""
)


def discover_pipeline_keys() -> list[str]:
    """All dotted pipeline keys (``<domain>.<table>``) under ``source/pipelines/``.

    Mirrors how ``run_full_ci.py`` discovers them — a pipeline is any directory
    containing a ``pipeline.py``.
    """
    keys: list[str] = []
    for path in sorted(_PIPELINES_ROOT.rglob("pipeline.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.parent.relative_to(_PIPELINES_ROOT)
        keys.append(".".join(rel.parts))
    return keys


def discover_framework_pipelines() -> list[str]:
    """Pipeline keys whose ``pipeline.py`` imports ``source.framework``.

    Excludes legacy pipelines that still use ``@dlt.table`` directly.
    """
    keys: list[str] = []
    for path in sorted(_PIPELINES_ROOT.rglob("pipeline.py")):
        if "__pycache__" in path.parts:
            continue
        if "from poorbricks" in path.read_text(encoding="utf-8"):
            rel = path.parent.relative_to(_PIPELINES_ROOT)
            keys.append(".".join(rel.parts))
    return keys


def find_table_source_refs(
    path: Path,
) -> Iterator[tuple[str, str, str, str]]:
    """Yield ``(catalog, level, table, model_name)`` per ``TableSource`` literal.

    Reads the file once. The regex tolerates whitespace + multiline ``TableSource``
    calls; it does NOT execute any Python.
    """
    for m in _TABLE_SOURCE_RE.finditer(path.read_text(encoding="utf-8")):
        yield m["catalog"], m["level"], m["table"], m["model"]


def iter_pipeline_files() -> Iterator[Path]:
    """All ``pipeline.py`` files under ``tables/`` (skips ``__pycache__``)."""
    for path in sorted(_PIPELINES_ROOT.rglob("pipeline.py")):
        if "__pycache__" not in path.parts:
            yield path


__all__ = [
    "REPO_ROOT",
    "discover_framework_pipelines",
    "discover_pipeline_keys",
    "find_table_source_refs",
    "iter_pipeline_files",
]
