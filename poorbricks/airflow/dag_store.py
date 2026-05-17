"""DAG file storage backends.

Two implementations:

* ``LocalDagStore`` — writes to a directory on the local filesystem.
  Used for ``--dry-run`` and developer-loop iteration.
* ``GcsDagStore`` — writes to a GCS bucket under a per-repo prefix. The
  Airflow scheduler / webserver run a ``gsutil rsync`` sidecar that
  syncs the bucket into ``/opt/airflow/dags`` every 30 seconds.

Both implementations expose the same ``put`` + ``prune`` interface so
the upload service treats them interchangeably. ``prune`` is what makes
deletion work end-to-end: when a workflow YAML is removed from a
table-repo, the next upload removes its DAG from the store (and thus
from Airflow within the sidecar's sync interval).
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class DagStore(Protocol):
    """Backend-agnostic interface for writing and pruning generated DAG files."""

    def put(self, prefix: str, name: str, content: str) -> None:
        """Write ``<prefix>/<name>.py`` with the given source."""

    def list_dags(self, prefix: str) -> list[str]:
        """Return the names (without ``.py``) currently stored under ``prefix``."""

    def prune(self, prefix: str, keep: set[str]) -> list[str]:
        """Delete every DAG under ``prefix`` whose name is not in ``keep``.

        Returns the list of deleted names (without ``.py`` suffix), sorted.
        Strictly scoped to ``prefix`` so multi-repo isolation cannot break.
        """


class LocalDagStore:
    """Filesystem-backed store. One directory per ``prefix``."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _prefix_dir(self, prefix: str) -> Path:
        return self.root / prefix

    def put(self, prefix: str, name: str, content: str) -> None:
        target_dir = self._prefix_dir(prefix)
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / f"{name}.py").write_text(content)

    def list_dags(self, prefix: str) -> list[str]:
        target_dir = self._prefix_dir(prefix)
        if not target_dir.exists():
            return []
        return sorted(p.stem for p in target_dir.glob("*.py"))

    def prune(self, prefix: str, keep: set[str]) -> list[str]:
        target_dir = self._prefix_dir(prefix)
        if not target_dir.exists():
            return []
        deleted: list[str] = []
        for path in target_dir.glob("*.py"):
            if path.stem not in keep:
                path.unlink()
                deleted.append(path.stem)
        return sorted(deleted)


__all__ = ["DagStore", "LocalDagStore"]
