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


class GcsDagStore:
    """Google Cloud Storage backend.

    Layout::

        gs://<bucket>/<prefix>/<workflow-name>.py

    ``prune`` lists only the ``<prefix>/`` keyspace; impossible to touch
    another repo's DAGs even with a misconfigured caller.
    """

    def __init__(self, bucket_name: str, client: object | None = None) -> None:
        self.bucket_name = bucket_name
        if client is None:
            from google.cloud import storage

            client = storage.Client()
        self._client = client
        self._bucket = client.bucket(bucket_name)  # type: ignore[union-attr]

    def _blob_name(self, prefix: str, name: str) -> str:
        return f"{prefix}/{name}.py"

    def put(self, prefix: str, name: str, content: str) -> None:
        blob = self._bucket.blob(self._blob_name(prefix, name))
        blob.upload_from_string(content, content_type="text/x-python")

    def list_dags(self, prefix: str) -> list[str]:
        names: list[str] = []
        for blob in self._client.list_blobs(  # type: ignore[union-attr]
            self.bucket_name, prefix=f"{prefix}/"
        ):
            if blob.name.endswith(".py"):
                stem = blob.name[len(prefix) + 1 : -3]
                names.append(stem)
        return sorted(names)

    def prune(self, prefix: str, keep: set[str]) -> list[str]:
        deleted: list[str] = []
        for blob in self._client.list_blobs(  # type: ignore[union-attr]
            self.bucket_name, prefix=f"{prefix}/"
        ):
            if not blob.name.endswith(".py"):
                continue
            stem = blob.name[len(prefix) + 1 : -3]
            if stem not in keep:
                blob.delete()
                deleted.append(stem)
        return sorted(deleted)


__all__ = ["DagStore", "GcsDagStore", "LocalDagStore"]
