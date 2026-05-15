"""Shared helpers for validation tests.

Public surface — keep tiny so tests stay readable:

- :func:`discover_pipeline_keys` — every dotted pipeline key under ``tables/``
- :func:`discover_framework_pipelines` — subset that imports ``framework``
- :func:`find_table_source_refs` — yield ``(catalog, level, table, model_name)`` per ref
- :func:`filter_jvm_noise` — drop ``\\tat`` frames, SLF4J banners from subprocess output
- :func:`first_error_line` — postgres_export-style ``str(exc).splitlines()[0][:120]``
"""

from .discovery import (
    REPO_ROOT,
    discover_framework_pipelines,
    discover_pipeline_keys,
    find_table_source_refs,
    iter_pipeline_files,
)
from .subprocess_filter import filter_jvm_noise, first_error_line

__all__ = [
    "REPO_ROOT",
    "discover_framework_pipelines",
    "discover_pipeline_keys",
    "filter_jvm_noise",
    "find_table_source_refs",
    "first_error_line",
    "iter_pipeline_files",
]
